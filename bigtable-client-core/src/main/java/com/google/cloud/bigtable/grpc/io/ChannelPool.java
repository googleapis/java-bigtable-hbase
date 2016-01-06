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

import com.google.common.collect.ImmutableList;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 */
public class ChannelPool extends Channel {

  protected static final Logger log = Logger.getLogger(ChannelPool.class.getName());

  public interface ChannelFactory {
    Channel create() throws IOException;
  }

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
      return authority;
    }

    public synchronized void returnToPool() {
      if (!returned) {
        ChannelPool.this.returnChannel(this);
        returned = true;
      }
    }
  }

  private final AtomicReference<ImmutableList<Channel>> channels = new AtomicReference<>();
  private final AtomicInteger requestCount = new AtomicInteger();
  private final ImmutableList<HeaderInterceptor> headerInterceptors;
  private final AtomicInteger totalSize;
  private final ChannelFactory factory;
  private int reservedChannelCount = 0;
  private final String authority;

  public ChannelPool(List<HeaderInterceptor> headerInterceptors, ChannelFactory factory)
      throws IOException {
    Channel channel = factory.create();
    this.channels.set(ImmutableList.of(channel));
    authority = channel.authority();
    totalSize = new AtomicInteger(1);
    this.factory = factory;
    if (headerInterceptors == null) {
      this.headerInterceptors = ImmutableList.of();
    } else {
      this.headerInterceptors = ImmutableList.copyOf(headerInterceptors);
    }
  }

  /**
   * Makes sure that the number of channels is at least as big as the specified capacity.  This
   * method is only synchornized when the pool has to be expanded.
   *
   * @param capacity The minimum number of channels required for the RPCs of the ChannelPool's
   * clients.
   */
  public void ensureChannelCount(int capacity) throws IOException {
    if (totalSize.get() < capacity) {
      synchronized(this){
        if (totalSize.get() < capacity) {
          List<Channel> newChannelList = new ArrayList<>(channels.get());
          int unreservedCapacity = capacity - reservedChannelCount;
          while(newChannelList.size() < unreservedCapacity) {
            newChannelList.add(factory.create());
          }
          setChannels(newChannelList);
          totalSize.set(capacity);
        }
      }
    }
  }

  /**
   * Performs a simple round robin on the list of {@link Channel}s in the {@code channels} list.
   * This method should not be synchronized, if possible, to reduce bottlenecks.
   * 
   * @return A channel that can be used for a safe 
   */
  private Channel getNextChannel() {
    int currentRequestNum = requestCount.getAndIncrement();
    ImmutableList<Channel> channelsList = channels.get();
    int index = Math.abs(currentRequestNum % channelsList.size());
    return channelsList.get(index);
  }

  /**
   * Gets a channel from the pool. Long running streaming RPCs can cause a contention issue if there
   * is another RPC started on the same channel. If the pool only has a single channel, keep the
   * channel in the pool so that other RPCs can at least attempt to use it. This is required for
   * scans due to a gRPC bug. gRPC 0.10 will remove the necessity of reserving a channel.
   */
  public synchronized PooledChannel reserveChannel() {
    Channel reserved;
    boolean returned = false;

    ImmutableList<Channel> channelsList = channels.get();

    if (channelsList.size() == 1) {
      reserved = channelsList.get(0);
      returned = true;
    } else {
      List<Channel> newChannelList = new ArrayList<>(channelsList);
      reserved = newChannelList.remove(newChannelList.size() - 1);
      setChannels(newChannelList);
      reservedChannelCount++;
    }
    return new PooledChannel(reserved, returned);
  }

  /**
   * Return a reserved {@link Channel}, via {@link #reserveChannel()}, to the general pool.
   */
  private synchronized void returnChannel(PooledChannel channel){
    List<Channel> newChannelList = new ArrayList<>(channels.get());
    newChannelList.add(channel.delegate);
    setChannels(newChannelList);
    reservedChannelCount--;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String authority() {
    return authority;
  }

  /**
   * Create a {@link ClientCall} on a Channel from the pool chosen in a round-robin fashion to the
   * remote operation specified by the given {@link MethodDescriptor}. The returned {@link
   * ClientCall} does not trigger any remote behavior until {@link
   * ClientCall#start(ClientCall.Listener, Metadata)} is
   * invoked.
   *
   * @param methodDescriptor describes the name and parameter types of the operation to call.
   * @param callOptions runtime options to be applied to this call.
   * @return a {@link ClientCall} bound to the specified method.
   */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    return createWrappedCall(methodDescriptor, callOptions, getNextChannel());
  }

  private <ReqT, RespT> ClientCall<ReqT, RespT> createWrappedCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    ClientCall<ReqT, RespT> delegate = channel.newCall(methodDescriptor, callOptions);
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

  /**
   * Sets the values in newChannelList to the {@code channels} AtomicReference.  The values are
   * copied into an {@link ImmutableList}.
   *
   * @param newChannelList A {@link List} of {@link Channel}s to set to the {@code channels}
   */
  private void setChannels(List<Channel> newChannelList) {
    channels.set(ImmutableList.copyOf(newChannelList));
  }

  public int size() {
    return totalSize.get();
  }

  public int availbleSize() {
    return channels.get().size();
  }
}
