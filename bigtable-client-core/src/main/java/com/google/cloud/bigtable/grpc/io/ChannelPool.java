/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.io;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 */
public class ChannelPool extends Channel {

  protected static final Logger log = Logger.getLogger(ChannelPool.class.getName());

  /**
   * An implementation of {@link Channel} that knows how to return itself to the {@link ChannelPool}
   */
  public class PooledChannel extends Channel {
    private final Channel delegate;
    private boolean returned = false;

    private PooledChannel(Channel delegate, boolean returned) {
      this.delegate = delegate;
      this.returned = returned;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
      return createWrappedCall(methodDescriptor, callOptions, delegate);
    }

    @Override
    public String authority() {
      return delegate.authority();
    }

    public synchronized void returnToPool() {
      if (!returned) {
        ChannelPool.this.returnChannel(this);
        returned = true;
      }
    }
  }

  private Channel[] channels;
  private final AtomicInteger requestCount = new AtomicInteger();
  private final List<HeaderInterceptor> headerInterceptors;
  private final ReadWriteLock channelsLock = new ReentrantReadWriteLock();
  private final Lock readChannelsLock = channelsLock.readLock();
  private final Lock writeChannelsLock = channelsLock.writeLock();

  public ChannelPool(Channel[] channels, List<HeaderInterceptor> headerInterceptors) {
    Preconditions.checkArgument(channels != null && channels.length > 0);
    this.channels = channels;
    this.headerInterceptors = headerInterceptors;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    return createWrappedCall(methodDescriptor, callOptions, getNextChannel());
  }

  private <ReqT, RespT> ClientCall<ReqT, RespT> createWrappedCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return wrap(channel.newCall(methodDescriptor, callOptions));
  }

  private <ReqT, RespT> ClientCall<ReqT, RespT> wrap(ClientCall<ReqT, RespT> delegate) {
    return new CheckedForwardingClientCall<ReqT, RespT>(delegate) {
      @Override
      protected void checkedStart(ClientCall.Listener<RespT> responseListener, Metadata headers)
          throws Exception {
        for (HeaderInterceptor interceptor : headerInterceptors) {
          interceptor.updateHeaders(headers);
        }
        delegate().start(responseListener, headers);
      }
    };
  }

  private Channel getNextChannel() {
    int currentRequestNum = requestCount.getAndIncrement();
    readChannelsLock.lock();
    try {
      return channels[Math.abs(currentRequestNum % channels.length)];
    } finally {
      readChannelsLock.unlock();
    }
  }

  @Override
  public String authority() {
    return channels[0].authority();
  }

  /**
   * Gets a channel from the pool. Long running streaming RPCs can cause a contention issue if there
   * is another RPC started on the same channel. If the pool only has a single channel, keep the
   * channel in the pool so that other RPCs can at least attempt to use it.
   */
  public PooledChannel reserveChannel() {
    Channel reserved;
    boolean returned = false;
    writeChannelsLock.lock();
    try {
      if (channels.length == 1) {
        reserved = channels[0];
        returned = true;
      } else {
        reserved = channels[channels.length - 1];
        Channel[] newChannelArray = new Channel[channels.length - 1];
        System.arraycopy(channels, 0, newChannelArray, 0, channels.length - 1);
        channels = newChannelArray;
      }
    } finally {
      writeChannelsLock.unlock();
    }
    return new PooledChannel(reserved, returned);
  }

  private void returnChannel(PooledChannel channel){
    writeChannelsLock.lock();
    try {
      Channel[] newChannelArray = new Channel[channels.length + 1];
      System.arraycopy(channels, 0, newChannelArray, 0, channels.length);
      newChannelArray[channels.length] = channel.delegate;
      channels = newChannelArray;
    } finally {
      writeChannelsLock.unlock();
    }
  }

  @VisibleForTesting
  public int size() {
    readChannelsLock.lock();
    try {
      return channels.length;
    } finally {
      readChannelsLock.unlock();
    }
  }
}
