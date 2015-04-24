package com.google.cloud.bigtable.grpc;

import io.grpc.Call;
import io.grpc.MethodDescriptor;

import java.io.IOException;
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

  protected static final Logger log = Logger.getLogger(ChannelPool.class.getName());
  protected static final long CLOSE_WAIT_TIME = 5000;

  /**
   * Creates a fresh CloseableChannel.
   */
  public interface Factory {
    CloseableChannel create();
  }

  // We can't do a newCall on a closed delegate.  This will ensure that refreshes don't 
  // allow a closed delegate to perform a newCall.  Once closed is called, all existing
  // calls will complete before the delegate shuts down.
  private final ReentrantReadWriteLock delegateLock = new ReentrantReadWriteLock();
  private final AtomicInteger closingAsynchronously = new AtomicInteger(0);

  private final long maxRefreshTime;
  private final Factory factory;
  private final Executor executor;

  // nextRefresh and delegate need to be protected by delegateLock.
  private long nextRefresh;
  private CloseableChannel delegate;

  public ReconnectingChannel(long maxRefreshTime, Executor executor, final Factory factory) {
    this.maxRefreshTime = maxRefreshTime;
    this.executor = executor;
    this.delegate = factory.create();
    this.nextRefresh = calculateNewRefreshTime();
    this.factory = factory;
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
      checkRefresh(readLock);
      return delegate.newCall(methodDescriptor);
    } finally {
      readLock.unlock();
    }
  }

  private void checkRefresh(ReadLock readLock) {
    if (delegate == null || !requiresRefresh()) {
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
      if (delegate != null && requiresRefresh()) {
        // Startup should be non-blocking and async.
        asyncClose(delegate);
        delegate = factory.create();
        nextRefresh = calculateNewRefreshTime();
      }
    } finally {
      writeLock.unlock();
    }
  }

  
  @Override
  public void close() throws IOException {
    CloseableChannel toClose = null;

    WriteLock writeLock = delegateLock.writeLock();
    writeLock.lock();
    try {
      toClose = delegate;
      delegate = null;
    } finally {
      writeLock.unlock();
    }
    if (toClose != null) {
      toClose.close();
    }
    synchronized (closingAsynchronously) {
      while (closingAsynchronously.get() > 0) {
        try {
          closingAsynchronously.wait(CLOSE_WAIT_TIME);
        } catch (InterruptedException ignored){
          // TODO(angusdavis): rework this to allow the thread
          // interrupted state to propagate.
        }
      }
    }
  }

  private void asyncClose(final CloseableChannel channel) {
    closingAsynchronously.incrementAndGet();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          channel.close();
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

  private boolean requiresRefresh() {
    return System.currentTimeMillis() > nextRefresh;
  }


  private long calculateNewRefreshTime() {
    // Set the timeout. Use a random variability to reduce jetteriness when this Channel is part of
    // a pool.
    double randomizedPercentage = 1 - (.05 * Math.random());
    long randomizedEnd =
        this.maxRefreshTime < 0 ? -1L : (long) (this.maxRefreshTime * randomizedPercentage);
    return randomizedEnd <= 0 ? randomizedEnd : (randomizedEnd + System.currentTimeMillis());
  }
}
