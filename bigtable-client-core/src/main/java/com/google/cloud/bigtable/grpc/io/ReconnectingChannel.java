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

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.Metadata.Headers;
import io.grpc.MethodDescriptor;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
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
  public static final long CHANNEL_TERMINATE_WAIT_MS = 5000;

  // This executor is used to shutdown & await termination of
  // grpc connections. The work done on these threads should be minimal
  // as long as we don't perform a shutdownNow() call (or similar). As
  // a result, allow there to be an unbounded number of these.
  // It is not expected to happen often, but there are cases where
  // shutdown will never complete and we don't want to take up a thread
  // that could be used to indicate that a Call is completed (and would
  // then finish client shutdown).
  protected final ExecutorService closeExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("reconnection-async-close-%s")
              .setDaemon(true)
              .build());

  /**
   * Creates a fresh CloseableChannel.
   */
  public interface Factory {
    Channel createChannel() throws IOException;
    Closeable createClosable(Channel channel);
  }

  private class DelayingCall<RequestT, ResponseT> extends Call<RequestT, ResponseT> {

    final MethodDescriptor<RequestT, ResponseT> methodDescriptor;
    Call<RequestT, ResponseT> callDelegate = null;
    
    public DelayingCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor) {
      this.methodDescriptor = methodDescriptor;
    }

    @Override
    public void start(Call.Listener<ResponseT> responseListener, Headers headers) {
      Preconditions.checkState(callDelegate == null, "Already started");
      ReadLock readLock = delegateLock.readLock();
      readLock.lock();
      try {
        if (delegate == null) {
          throw new IllegalStateException("Channel is closed");
        }
        checkRefresh(readLock);
        callDelegate = delegate.newCall(methodDescriptor);
        callDelegate.start(responseListener, headers);
      } catch (IOException e) {
        throw new IllegalStateException("Channel cannot create a new call", e);
      } finally {
        readLock.unlock();
      }
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
    public void sendPayload(RequestT payload) {
      Preconditions.checkState(callDelegate != null, "Not started");
      callDelegate.sendPayload(payload);
    }

  }

  // We can't do a newCall on a closed delegate.  This will ensure that refreshes don't 
  // allow a closed delegate to perform a newCall.  Once closed is called, all existing
  // calls will complete before the delegate shuts down.
  private final ReentrantReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final AtomicInteger closingAsynchronously = new AtomicInteger(0);

  private final long maxRefreshTime;
  private final Factory factory;

  // nextRefresh and delegate need to be protected by delegateLock.
  private long nextRefresh;
  private Channel delegate;

  public ReconnectingChannel(
      long maxRefreshTime,
      Factory connectionFactory) throws IOException {
    Preconditions.checkArgument(maxRefreshTime >= 0L, "maxRefreshTime cannot be less than 0.");
    this.maxRefreshTime = maxRefreshTime;
    this.delegate = connectionFactory.createChannel();
    this.nextRefresh = calculateNewRefreshTime();
    this.factory = connectionFactory;
  }

  @Override
  public <RequestT, ResponseT> Call<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor) {
    return new DelayingCall<>(methodDescriptor);
  }

  private void checkRefresh(ReadLock readLock) throws IOException {
    if (!requiresRefresh()) {
      return;
    }
    
    // A writeLock will assure that only a single thread updates to delgate.  See close() for
    // the other use of writeLocks in this class.
    //
    // A write lock can't be granted while there's an outstanding readLock, even by this thread.
    // A read lock can be granted while holding a write lock.  This read lock was obtained by 
    // newCall() and will be unlocked there.

    WriteLock writeLock = delegateLock.writeLock();
    readLock.unlock();
    writeLock.lock();
    readLock.lock();

    try {
      // Double check that a previous call didn't refresh the connection since this thread 
      // acquired the write lock. 
      if (requiresRefresh()) {
        // Startup should be non-blocking and async.
        Channel oldDelegate = delegate;
        delegate = factory.createChannel();
        nextRefresh = calculateNewRefreshTime();
        asyncClose(oldDelegate);
      }
    } finally {
      writeLock.unlock();
    }
  }

  
  @Override
  public void close() throws IOException {
    Channel toClose = null;

    WriteLock writeLock = delegateLock.writeLock();
    writeLock.lock();
    try {
      toClose = delegate;
      delegate = null;
    } finally {
      writeLock.unlock();
    }
    final Channel channel = toClose;
    if (channel != null) {
      factory.createClosable(channel).close();
    }
    synchronized (closingAsynchronously) {
      while (closingAsynchronously.get() > 0) {
        try {
          closingAsynchronously.wait(CHANNEL_TERMINATE_WAIT_MS);
        } catch (InterruptedException e){
          throw new IOException("Could not close all channels.", e);
        }
      }
    }
    closeExecutor.shutdownNow();
  }

  private void asyncClose(final Channel channel) {
    closingAsynchronously.incrementAndGet();
    closeExecutor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          factory.createClosable(channel).close();
        } catch (IOException e) {
          log.log(Level.WARNING, "Could not close a recycled delegate", e);
        } finally {
          closingAsynchronously.decrementAndGet();
          synchronized (closingAsynchronously) {
            closingAsynchronously.notify();
          }
        }
      }
    });
  }

  @VisibleForTesting
  boolean requiresRefresh() {
    return delegate != null && maxRefreshTime > 0 && System.currentTimeMillis() > nextRefresh;
  }

  private long calculateNewRefreshTime() {
    // Set the timeout. Use a random variability to reduce jetteriness when this Channel is part of
    // a pool.
    double randomizedPercentage = 1D - (.05D * Math.random());
    long randomizedEnd = (long) (this.maxRefreshTime * randomizedPercentage);
    return (randomizedEnd + System.currentTimeMillis());
  }
}
