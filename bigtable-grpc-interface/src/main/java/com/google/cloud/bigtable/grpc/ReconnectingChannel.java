/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.common.base.Preconditions;

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.Metadata.Headers;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ClosableChannel that refreshes itself based on a user supplied timeout.
 */
public class ReconnectingChannel implements CloseableChannel {

  protected static final long MINIMUM_REFRESH_TIME = 5000;

  protected static final Logger log = Logger.getLogger(ChannelPool.class.getName());
  protected static final long CLOSE_WAIT_TIME = 5000;

  /**
   * Creates a fresh CloseableChannel.
   */
  public interface Factory {
    CloseableChannel create();
  }

  /**
   * TODO: This class was introduced because of a temporary issue in gRPC. It should be handling
   * .shutdown() calls gracefully, but isn't due to a bug. Once they fix the bug, remove
   * TrackingCall and CountingChannel. Also, uncomment ReconnectingChannelTest.
   */
  private static class TrackingCall<RequestT, ResponseT> 
      extends ClientInterceptors.ForwardingCall<RequestT, ResponseT> {

    private final Integer id;
    private final Set<Integer> outstandingRequests;

    protected TrackingCall(Call<RequestT, ResponseT> delegate, Integer id,
        Set<Integer> outstandingRequests) {
      super(delegate);
      this.id = id;
      this.outstandingRequests = outstandingRequests;
    }

    @Override
    public void start(Call.Listener<ResponseT> responseListener, Headers headers) {
      super.start(wrap(responseListener), headers);
    }

    @Override
    public void cancel() {
      super.cancel();
      outstandingRequests.remove(id);
    }

    private Call.Listener<ResponseT> wrap(Call.Listener<ResponseT> responseListener) {
      outstandingRequests.add(id);
      return new ClientInterceptors.ForwardingListener<ResponseT>(responseListener) {
        @Override
        public void onClose(Status status, Metadata.Trailers trailers) {
          super.onClose(status, trailers);
          outstandingRequests.remove(id);
          if (outstandingRequests.isEmpty()) {
            synchronized (outstandingRequests) {
              outstandingRequests.notify();
            }
          }
        }
      };
    }
  }

  private static class CountingChannel implements ClientInterceptor {
    private final CloseableChannel closableChannel;
    private final Channel channel;
    private final Set<Integer> outstandingRequests =
        Collections.synchronizedSet(new HashSet<Integer>());
    private final AtomicInteger counter = new AtomicInteger();

    public CountingChannel(CloseableChannel closableChannel) {
      this.closableChannel = closableChannel;
      this.channel = ClientInterceptors.intercept(closableChannel, this);
    }

    @Override
    public <RequestT, ResponseT> Call<RequestT, ResponseT> interceptCall(
        MethodDescriptor<RequestT, ResponseT> descriptor, Channel channel) {
      return new TrackingCall<>(channel.newCall(descriptor), counter.incrementAndGet(),
          outstandingRequests);
    }

    public void close() throws IOException, InterruptedException {
      synchronized (outstandingRequests) {
        while (!outstandingRequests.isEmpty()) {
          outstandingRequests.wait(CLOSE_WAIT_TIME);
        }
      }

      closableChannel.close();
    }
  }

  // We can't do a newCall on a closed delegate. This will ensure that refreshes don't
  // allow a closed delegate to perform a newCall. Once closed is called, all existing
  // calls will complete before the delegate shuts down.
  private final ReentrantReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final AtomicInteger closingAsynchronously = new AtomicInteger(0);

  private final long maxRefreshTime;
  private final Factory factory;
  private final Executor executor;

  // nextRefresh and delegate need to be protected by delegateLock.
  private long nextRefresh;
  private CountingChannel delegate;
  private Runnable updateRunnable = null;

  public ReconnectingChannel(long maxRefreshTime, Executor executor, final Factory factory) {
    Preconditions.checkArgument(maxRefreshTime > MINIMUM_REFRESH_TIME,
        "maxRefreshTime has to be at least " + MINIMUM_REFRESH_TIME + " ms.");
    this.maxRefreshTime = maxRefreshTime;
    this.executor = executor;
    this.nextRefresh = calculateNewRefreshTime();
    this.factory = factory;

    this.delegate = createDelegate();
  }

  private CountingChannel createDelegate() {
    return new CountingChannel(factory.create());
  }

  @Override
  public <RequestT, ResponseT> Call<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor) {
    ReadLock readLock = delegateLock.readLock();
    readLock.lock();
    try {
      if (delegate == null) {
        throw new IllegalStateException("Channel is closed");
      }
      checkRefresh();
      return delegate.channel.newCall(methodDescriptor);
    } finally {
      readLock.unlock();
    }
  }

  private void checkRefresh() {
    if (delegate == null || !requiresRefresh() || updateRunnable != null) {
      return;
    }

    updateRunnable = new Runnable() {
      @Override
      public void run() {
        CountingChannel toClose = null;
        WriteLock writeLock = null;
        try {
          CountingChannel newChannel = new CountingChannel(factory.create());
          writeLock = delegateLock.writeLock();
          writeLock.lock();
          // Double check that a previous call didn't refresh the connection since this thread
          // acquired the write lock.
          if (delegate != null && requiresRefresh()) {
            toClose = delegate;
            delegate = newChannel;
            nextRefresh = calculateNewRefreshTime();
          } else {
            toClose = newChannel;
          }
        } finally {
          updateRunnable = null;
          if (toClose != null) {
            closingAsynchronously.incrementAndGet();
          }
          if (writeLock != null) {
            writeLock.unlock();
          }
          try {
            toClose.close();
          } catch (IOException e) {
            log.log(Level.WARNING, "Could not close a recycled delegate", e);
          } catch (InterruptedException e) {
            Thread.interrupted();
          } finally {
            synchronized (closingAsynchronously) {
              if (closingAsynchronously.decrementAndGet() == 0) {
                closingAsynchronously.notify();
              }
            }
          }
        }
      }
    };
    executor.execute(updateRunnable);
  }

  @Override
  public void close() throws IOException {
    CountingChannel toClose = null;

    WriteLock writeLock = delegateLock.writeLock();
    writeLock.lock();
    try {
      toClose = delegate;
      delegate = null;
    } finally {
      writeLock.unlock();
    }
    try {
      if (toClose != null) {
        toClose.close();
      }
      synchronized (closingAsynchronously) {
        while (closingAsynchronously.get() > 0) {
          closingAsynchronously.wait(CLOSE_WAIT_TIME);
        }
      }
    } catch (InterruptedException ignored) {
      // TODO(angusdavis): rework this to allow the thread interrupted state to propagate.
    }
  }

  private boolean requiresRefresh() {
    return System.currentTimeMillis() > nextRefresh;
  }

  private long calculateNewRefreshTime() {
    // Set the timeout. Use a random variability to reduce jetteriness when this Channel is part of
    // a pool.
    double randomizedPercentage = 1.0D - (.05D * Math.random());
    long randomizedEnd = (long) (this.maxRefreshTime * randomizedPercentage);
    return (randomizedEnd + System.currentTimeMillis());
  }
}
