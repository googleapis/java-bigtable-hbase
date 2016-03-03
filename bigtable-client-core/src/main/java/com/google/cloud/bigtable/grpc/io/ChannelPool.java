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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.cloud.bigtable.config.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 */
public class ChannelPool extends ManagedChannel {

  protected static final Logger LOG = new Logger(ChannelPool.class);

  public interface ChannelFactory {
    ManagedChannel create() throws IOException;
  }

  private final ImmutableList<ManagedChannel> channels;
  private final AtomicInteger requestCount = new AtomicInteger();
  private final ImmutableList<HeaderInterceptor> headerInterceptors;
  private final String authority;

  private boolean shutdown = false;

  public ChannelPool(ChannelFactory factory, int channelCount)
      throws IOException {
    this(ImmutableList.<HeaderInterceptor>of(), factory, channelCount);
  }

  public ChannelPool(List<HeaderInterceptor> headerInterceptors, ChannelFactory factory, int channelCount)
      throws IOException {
    Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");
    Preconditions.checkNotNull(headerInterceptors, "Must pass non null headerInterceptors");
    this.headerInterceptors = ImmutableList.copyOf(headerInterceptors);

    ManagedChannel[] channelArray = new ManagedChannel[channelCount];
    for (int i = 0; i < channelCount; i++) {
      channelArray[i] = factory.create();
    }
    authority = channelArray[0].authority();
    channels = ImmutableList.copyOf(channelArray);
  }

  /**
   * Performs a simple round robin on the list of {@link ManagedChannel}s.
   * This method is thread safe.
   *
   * @return A channel.
   */
  private ManagedChannel getNextChannel() {
    int currentRequestNum = requestCount.getAndIncrement();
    int index = Math.abs(currentRequestNum % channels.size());
    return channels.get(index);
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
    Preconditions.checkState(!shutdown, "Cannot perform operations on a closed connection");
    return createWrappedCall(methodDescriptor, callOptions, getNextChannel());
  }

  private <ReqT, RespT> ClientCall<ReqT, RespT> createWrappedCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, ManagedChannel channel) {
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

  public int size() {
    return channels.size();
  }

  @Override
  public synchronized ManagedChannel shutdown() {
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
    this.shutdown = true;
    return this;
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    for (ManagedChannel managedChannel : channels) {
      if (!managedChannel.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ManagedChannel shutdownNow() {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow();
    }
    return this;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    for (ManagedChannel channel : channels) {
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
    }

    return isTerminated();
  }
}
