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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Counter;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.Timer.Context;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ChannelPool extends ManagedChannel {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(ChannelPool.class);

  private static final AtomicInteger ChannelIdGenerator = new AtomicInteger();

  /**
   * Best effort counter of active channels. There may be some cases where channel termination
   * counting may not accurately be decremented.
   */
  protected static Counter ACTIVE_CHANNEL_COUNTER;

  /**
   * A factory for creating ManagedChannels to be used in a {@link ChannelPool}.
   *
   * @author sduskis
   *
   */
  public interface ChannelFactory {
    ManagedChannel create() throws IOException;
  }


  /**
   * There's a classloading vs. user configuration timing issue. A user should have the opportunity
   * to configure metrics gathering even if the class loader loads the ChannelPool. If a user turns
   * on metrics after this class is loaded, these metrics should not have the NULL
   * implementation.
   * @return
   */
  protected synchronized static Counter getActiveChannelCounter() {
    if (ACTIVE_CHANNEL_COUNTER == null) {
      ACTIVE_CHANNEL_COUNTER = BigtableClientMetrics.createCounter("ChannelPool.active.count");
    }
    return ACTIVE_CHANNEL_COUNTER;
  }

  /**
   * Contains a {@link ManagedChannel} and metrics for the channel
   * @author sduskis
   *
   */
  private static class ChannelAndMetrics {
    final ManagedChannel channel;
    // a uniquely named timer for this channel's latency
    final Timer timer;

    private final AtomicBoolean active = new AtomicBoolean(true);

    public ChannelAndMetrics(ManagedChannel channel) {
      this.channel = channel;
      this.timer = BigtableClientMetrics.createTimer(
        "ChannelPool.channel." + ChannelIdGenerator.incrementAndGet() + ".latency");
      getActiveChannelCounter().inc();
    }

    public synchronized void markInactive(){
      boolean previouslyActive = active.getAndSet(false);
      if (previouslyActive) {
        getActiveChannelCounter().dec();
      }
    }
  }

  private final AtomicReference<ImmutableList<ChannelAndMetrics>> channels = new AtomicReference<>();
  private final AtomicInteger requestCount = new AtomicInteger();
  private final ImmutableList<HeaderInterceptor> headerInterceptors;
  private final ChannelFactory factory;
  private final String authority;

  private boolean shutdown = false;

  /**
   * <p>Constructor for ChannelPool.</p>
   *
   * @param headerInterceptors a {@link java.util.List} object.
   * @param factory a {@link com.google.cloud.bigtable.grpc.io.ChannelPool.ChannelFactory} object.
   * @throws java.io.IOException if any.
   */
  public ChannelPool(List<HeaderInterceptor> headerInterceptors, ChannelFactory factory)
      throws IOException {
    this.factory = factory;
    ChannelAndMetrics channelAndMetrics = new ChannelAndMetrics(factory.create());
    this.channels.set(ImmutableList.of(channelAndMetrics));
    authority = channelAndMetrics.channel.authority();
    if (headerInterceptors == null) {
      this.headerInterceptors = ImmutableList.of();
    } else {
      this.headerInterceptors = ImmutableList.copyOf(headerInterceptors);
    }
  }

  /**
   * Makes sure that the number of channels is at least as big as the specified capacity.  This
   * method is only synchronized when the pool has to be expanded.
   *
   * @param capacity The minimum number of channels required for the RPCs of the ChannelPool's
   * clients.
   * @throws java.io.IOException if any.
   */
  public void ensureChannelCount(int capacity) throws IOException {
    if (this.shutdown) {
      throw new IOException("The channel is closed.");
    }
    if (channels.get().size() < capacity) {
      synchronized (this) {
        if (channels.get().size() < capacity) {
          List<ChannelAndMetrics> newChannelList = new ArrayList<>(channels.get());
          while(newChannelList.size() < capacity) {
            newChannelList.add(new ChannelAndMetrics(factory.create()));
          }
          setChannels(newChannelList);
        }
      }
    }
  }

  /**
   * Performs a simple round robin on the list of {@link ChannelAndMetrics}s in the {@code channels}
   * list. This method should not be synchronized, if possible, to reduce bottlenecks.
   *
   * @return A {@link ChannelAndMetrics} that can be used for a single RPC call.
   */
  private ChannelAndMetrics getNextChannel() {
    int currentRequestNum = requestCount.getAndIncrement();
    ImmutableList<ChannelAndMetrics> channelsList = channels.get();
    int index = Math.abs(currentRequestNum % channelsList.size());
    return channelsList.get(index);
  }

  /** {@inheritDoc} */
  @Override
  public String authority() {
    return authority;
  }

  /**
   * {@inheritDoc}
   *
   * Create a {@link ClientCall} on a Channel from the pool chosen in a round-robin fashion to the
   * remote operation specified by the given {@link MethodDescriptor}. The returned {@link
   * ClientCall} does not trigger any remote behavior until {@link
   * ClientCall#start(ClientCall.Listener, Metadata)} is
   * invoked.
   */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    Preconditions.checkState(!shutdown, "Cannot perform operations on a closed connection");
    ChannelAndMetrics channelWrapper = getNextChannel();
    final Context timerContext = channelWrapper.timer.time();
    ClientCall<ReqT, RespT> delegate = channelWrapper.channel.newCall(methodDescriptor, callOptions);
    return new CheckedForwardingClientCall<ReqT, RespT>(delegate) {
      @Override
      protected void checkedStart(ClientCall.Listener<RespT> responseListener, Metadata headers)
          throws Exception {
        for (HeaderInterceptor interceptor : headerInterceptors) {
          interceptor.updateHeaders(headers);
        }
        ClientCall.Listener<RespT> wrappedListener = wrap(responseListener, timerContext);
        delegate().start(wrappedListener, headers);
      }
    };
  }

  protected <RespT> ClientCall.Listener<RespT> wrap(final ClientCall.Listener<RespT> delegate,
      final Context timeContext) {
    return new ClientCall.Listener<RespT>() {

      @Override
      public void onHeaders(Metadata headers) {
        delegate.onHeaders(headers);
      }

      @Override
      public void onMessage(RespT message) {
        delegate.onMessage(message);
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        try {
          delegate.onClose(status, trailers);
        } finally {
          timeContext.close();
        }
      }

      @Override
      public void onReady() {
        delegate.onReady();
      }
    };
  }

  /**
   * Sets the values in newChannelList to the {@code channels} AtomicReference.  The values are
   * copied into an {@link ImmutableList}.
   *
   * @param newChannelList A {@link List} of {@link ManagedChannel}s to set to the {@code channels}
   */
  private void setChannels(List<ChannelAndMetrics> newChannelList) {
    channels.set(ImmutableList.copyOf(newChannelList));
  }

  /**
   * <p>size.</p>
   *
   * @return a int.
   */
  public int size() {
    return channels.get().size();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ManagedChannel shutdown() {
    for (ChannelAndMetrics channelWrapper : channels.get()) {
      channelWrapper.channel.shutdown();
      channelWrapper.markInactive();
    }
    this.shutdown = true;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTerminated() {
    for (ChannelAndMetrics channelWrapper: channels.get()) {
      if (!channelWrapper.channel.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public ManagedChannel shutdownNow() {
    for (ChannelAndMetrics channelWrapper : channels.get()) {
      ManagedChannel channel = channelWrapper.channel;
      if (!channel.isTerminated()) {
        channel.shutdownNow();
        channelWrapper.markInactive();
      }
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    for (ChannelAndMetrics channelWrapper : channels.get()) {
      ManagedChannel channel = channelWrapper.channel;
      if (channel.isTerminated()) {
        continue;
      }
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
      channelWrapper.markInactive();
    }

    return isTerminated();
  }
}
