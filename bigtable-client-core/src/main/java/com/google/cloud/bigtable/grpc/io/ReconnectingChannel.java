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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.internal.ManagedChannelImpl;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A ClosableChannel that refreshes itself based on a user supplied timeout.
 */
public class ReconnectingChannel extends Channel implements Closeable {

  protected static final Logger log = Logger.getLogger(ReconnectingChannel.class.getName());
  public static final long CHANNEL_TERMINATE_WAIT_MS = 500;
  public static final long ACTIVE_REQUEST_START_WAIT_MS = 10;

  /** This executor is used to refresh {@link Channel}s and shutdown &amp; await termination of
   * old ones. The work done on these threads should be minimal
   * as long as we don't perform a {@link ManagedChannelImpl#shutdownNow()}. As
   * a result, allow there to be an unbounded number of shutdowns.
   * It is not expected to happen often, but there are cases where
   * shutdown will never complete and we don't want to take up a thread
   * that could be used to indicate that a {@link ClientCall} is completed. In such cases, the call
   * would finish when {@link ReconnectingChannel#close()} is called.
   */
  protected final ScheduledExecutorService refreshExecutor;

  /**
   * Creates a new {@link Channel} and a way to close it, probably through {@link
   * ManagedChannelImpl#shutdown()}.
   */
  public interface Factory {
    /**
     * Creates a new {@link Channel}.
     */
    Channel createChannel() throws IOException;

    /**
     * {@link Channel} does not expose a shutdown mechanism.  Default behavior will use
     * {@link ManagedChannelImpl#shutdown()} to close the {@link Channel}.
     */
    Closeable createClosable(Channel channel);
  }

  /**
   * Wraps a {@link Channel} and makes sure that when a {@link Channel#newCall(MethodDescriptor,
   * CallOptions)} creates a {@link ClientCall}, the delegate doesn't
   * close the {@link Channel} until {@link ClientCall#start(io.grpc.ClientCall.Listener, Metadata)}
   * is called, when it's safe to call {@link ManagedChannelImpl#shutdown()}.
   */
  private static class ChannelWrapper {
    final Factory factory;
    final Channel channel;
    final AtomicInteger activeRequests = new AtomicInteger();
    final AtomicBoolean isClosing = new AtomicBoolean(false);

    public ChannelWrapper(Factory factory) throws IOException {
      this.factory = factory;
      this.channel = factory.createChannel();
    }

    void addActiveRequest(){
      activeRequests.incrementAndGet();
    }

    void completeActiveRequest() {
      int count = activeRequests.decrementAndGet();
      if (count == 0 && isClosing.get()) {
        synchronized (this) {
          notify();
        }
      }
    }

    synchronized void close() throws IOException {
      isClosing.set(true);
      try {
        while (activeRequests.get() > 0) {
          wait(ACTIVE_REQUEST_START_WAIT_MS);
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        return;
      }
      factory.createClosable(channel).close();
    }

    <RequestT, ResponseT> ClientCall<RequestT, ResponseT> start(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions,
        io.grpc.ClientCall.Listener<ResponseT> responseListener, Metadata headers) {
      ClientCall<RequestT, ResponseT> callDelegate = null;
      try {
        callDelegate = channel.newCall(methodDescriptor, callOptions);
        callDelegate.start(responseListener, headers);
      } finally {
        completeActiveRequest();
      }
      return callDelegate;
    }
  }

  /**
   * This class makes sure that the underlying {@link ChannelWrapper} is available and the freshest
   * delegate is used.  It prevents an older {@link Channel} delegate from closing between {@link
   * Channel#newCall(MethodDescriptor, CallOptions)} and {@link
   * ClientCall#start(io.grpc.ClientCall.Listener, Metadata)}.
   */
  private class DelayingCall<RequestT, ResponseT> extends ClientCall<RequestT, ResponseT> {

    final MethodDescriptor<RequestT, ResponseT> methodDescriptor;
    final CallOptions callOptions;
    ClientCall<RequestT, ResponseT> callDelegate = null;

    public DelayingCall(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
      this.methodDescriptor = methodDescriptor;
      this.callOptions = callOptions;
    }

    @Override
    public void start(ClientCall.Listener<ResponseT> responseListener, Metadata headers) {
      Preconditions.checkState(callDelegate == null, "Call cannot be restarted.");
      callDelegate =
          getDelegateForRequest().start(methodDescriptor, callOptions, responseListener, headers);
    }

    @Override
    public void request(int numMessages) {
      Preconditions.checkState(callDelegate != null, "Not started");
      callDelegate.request(numMessages);
    }

    @Override
    public void cancel() {
      if (callDelegate != null) {
        callDelegate.cancel();
      }
    }

    @Override
    public void halfClose() {
      Preconditions.checkState(callDelegate != null, "Not started");
      callDelegate.halfClose();
    }

    @Override
    public void sendMessage(RequestT message) {
      Preconditions.checkState(callDelegate != null, "Not started");
      callDelegate.sendMessage(message);
    }
  }

  private final long maxRefreshMs;
  private final Factory factory;
  private final String authority;

  private ChannelWrapper delegate;

  /**
   * @param maxRefreshMs  The maximum amount of time before the channel should be discarded in ms.
   * @param connectionFactory Creates a Channel and a way to close it.
   * @throws IOException on channel creation. The SSLContext creation would be the likely culprit.
   */
  public ReconnectingChannel(long maxRefreshMs, Factory connectionFactory) throws IOException {
    this(maxRefreshMs, connectionFactory,
        Executors.newScheduledThreadPool(
            2,
            new ThreadFactoryBuilder()
                .setNameFormat("reconnection-async-close-%s")
                .setDaemon(true)
                .build()));
  }

  /**
   * @param maxRefreshMs  The maximum amount of time before the channel should be discarded in ms.
   * @param connectionFactory Creates a Channel and a way to close it.
   * @param executorService Manages the creation of new Channels and closing ofthe old ones.
   * @throws IOException on channel creation. The SSLContext creation would be the likely culprit.
   */
  @VisibleForTesting
  ReconnectingChannel(
      long maxRefreshMs,
      Factory connectionFactory,
      ScheduledExecutorService executorService) throws IOException {
    Preconditions.checkArgument(maxRefreshMs >= 0L, "maxRefreshMs cannot be less than 0.");
    this.maxRefreshMs = maxRefreshMs;
    this.factory = connectionFactory;
    this.delegate = new ChannelWrapper(factory);
    this.authority = delegate.channel.authority();
    this.refreshExecutor = executorService;
    if (maxRefreshMs > 0) {
      double randomizedPercentage = 1D - (.05D * Math.random());
      long delay = (long) (this.maxRefreshMs * randomizedPercentage);
      Runnable refreshRunanable = new Runnable() {
        @Override
        public void run() {
          ChannelWrapper oldDelegate = delegate;
          try {
            setDelegate(new ChannelWrapper(factory));
          } catch (IOException e) {
            throw new IllegalStateException("Channel cannot create a new delegate", e);
          }
          if(oldDelegate != null) {
            try {
              oldDelegate.close();
            } catch (IOException e) {
              log.log(Level.INFO, "Could not close a recycled delegate", e);
            }
          }
        }
      };
      refreshExecutor.scheduleAtFixedRate(refreshRunanable, delay, delay, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Creates a new call on a fresh Channel.
   */
  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    return new DelayingCall<>(methodDescriptor, callOptions);
  }

  /**
   * Closes the currently fresh channel, and any channels that may have been discarded, but are not
   * yet closed.
   */
  @Override
  public void close() throws IOException {
    ChannelWrapper toClose = getDelegateForClose();
    if (toClose != null) {
      toClose.close();
    }
    refreshExecutor.shutdown();
    while (!refreshExecutor.isTerminated()) {
      try {
        refreshExecutor.awaitTermination(CHANNEL_TERMINATE_WAIT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.interrupted();
        break;
      }
    }
  }

  private synchronized ChannelWrapper getDelegateForRequest() {
    Preconditions.checkState(delegate != null, "Channel is closed");
    delegate.addActiveRequest();
    return delegate;
  }

  private synchronized ChannelWrapper getDelegateForClose(){
    ChannelWrapper toClose = delegate;
    this.delegate = null;
    return toClose;
  }

  private synchronized void setDelegate(ChannelWrapper newDelegate) {
    this.delegate = newDelegate;
  }

  @Override
  public String authority() {
    return authority;
  }
}
