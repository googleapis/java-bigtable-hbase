/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.utils.faillog;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffer for failed mutations to be logged.
 *
 * <p>Objects of this class act a bit like a `BlockingQueue`, but instead of having a capacity
 * expressed in the number of elements, their capacity is expressed in bytes. They also make it a
 * bit easier for their reader to learn that there are no more log entries coming - `drain()`
 * returns a `null` after the user decides to `close()` the `LogBuffer`.
 */
public class LogBuffer implements Closeable {
  /**
   * Maximum size in bytes which will be stored in the buffer.
   *
   * <p>The limit will be disobeyed when the user inserts a log entry larger than this number.
   */
  private final int maxSize;
  /**
   * When `true` and the buffer clogs, attempts to append more data, will yield dropping it instead
   * of blocking.
   */
  private final boolean dropOnOverflow;

  private final Lock lock = new ReentrantLock();
  /** Condition used by threads blocked on draining the buffer. */
  private final Condition notEmpty = lock.newCondition();
  /** Condition used by threads blocked on appending to the buffer. */
  private final Condition notFull = lock.newCondition();

  private boolean shutdown = false;

  private ArrayDeque<byte[]> buffers = new ArrayDeque<>();
  /** Amount of data accumulated in `buffers`. */
  private int usedSize = 0;
  /**
   * If the buffer was closed due to an exception, this field holds the exception which caused it
   */
  private Throwable shutdownCause = null;

  /**
   * Construct a `LogBuffer`.
   *
   * @param recordBufferSize how much data the buffer will keep at most; this limit may be broken if
   *     a buffer larger than this number is `append()`-ed; in such a case, the buffer will not hold
   *     anything else
   * @param dropOnOverflow when `true` and the buffer clogs, attempts to `append()` more data, will
   *     yield dropping it instead of blocking the caller
   */
  public LogBuffer(int recordBufferSize, boolean dropOnOverflow) {
    this.maxSize = recordBufferSize;
    this.dropOnOverflow = dropOnOverflow;
  }

  private boolean admitLocked(byte[] data) {
    if (buffers.isEmpty() || usedSize + data.length <= maxSize) {
      usedSize += data.length;
      buffers.add(data);
      notEmpty.signal();
      return true;
    }
    return false;
  }

  private void waitForSpaceAndAdmitLocked(byte[] data) throws InterruptedException {
    while (!admitLocked(data) && !shutdown) {
      notFull.await();
    }
  }

  private void throwOnMisuseLocked(String description) {
    if (shutdownCause != null) {
      throw new IllegalStateException(description, shutdownCause);
    }
    throw new IllegalStateException(description);
  }

  /**
   * Append more data to the buffer.
   *
   * <p>Unless `dropOnOverflow` was set in the c-tor, this method may block in case the buffer
   * cannot fit the provided data. It will be unblocked when another thread `drain()`s the buffer or
   * `close()`-es the buffer.
   *
   * @param data the data to be appended
   * @return whether the data was appended; `false` may be returned only if `dropOnOverflow` was set
   *     to `true` in the c-tor and the buffer cannot fit the provided `data`
   * @throws InterruptedException in case the thread is interrupted and `dropOnOverflow` was set to
   *     `false`
   */
  public boolean append(byte[] data) throws InterruptedException {
    lock.lock();
    try {
      if (shutdown) {
        throwOnMisuseLocked("Can't append to a closed LogBuffer");
      }
      if (dropOnOverflow) {
        if (!admitLocked(data)) {
          // No space in the buffers, drop this buffer
          return false;
        }
      } else {
        waitForSpaceAndAdmitLocked(data);
        if (shutdown) {
          throwOnMisuseLocked("LogBuffer closed while waiting for log admission");
        }
      }

    } finally {
      lock.unlock();
    }
    return true;
  }

  /**
   * Drain the buffer from the data it gathered.
   *
   * <p>This method will return all data which was successfully `append()`-ed but not yet
   * `drain()`-ed. If the buffer is empty it will block until data is `append()`-ed or the buffer is
   * `close()`-ed.
   *
   * @return all data aggregated since previous `drain()` call or `null` if the buffer is
   *     `close()`-ed
   * @throws InterruptedException if the thread is interrupted
   */
  public Queue<byte[]> drain() throws InterruptedException {
    lock.lock();
    try {
      while (buffers.isEmpty() && !shutdown) {
        notEmpty.await();
      }
      if (buffers.isEmpty()) {
        Preconditions.checkState(shutdown);
        // We've been instructed to shut down and have already been drained from any buffers.
        return null;
      }
      Queue<byte[]> res = buffers;
      buffers = new ArrayDeque<>();
      usedSize = 0;
      notFull.signal();
      return res;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close the buffer for more `append()`-s and release any threads waiting for data or space in the
   * buffer.
   *
   * <p>When a buffer is `close()`-ed, anything which was successfully `append()`-ed to it will
   * still be returned via the `drain()` call. `drain()` calls on a closed, empty buffer will yield
   * a `null`. Any threads blocked on `append()` or attempting to `append()` after `close()` is
   * called will get a `IllegalStateException`.
   */
  @Override
  public void close() {
    closeWithCause(null);
  }

  /**
   * Close the buffer for more `append()`-s and release any threads waiting for data or space in the
   * buffer.
   *
   * <p>When a buffer is `close()`-ed, anything which was successfully `append()`-ed to it will
   * still be returned via the `drain()` call. `drain()` calls on a closed, empty buffer will yield
   * a `null`. Any threads blocked on `append()` or attempting to `append()` after `close()` is
   * called will get a `IllegalStateException`.
   *
   * @param cause the cause for the shutdown; if anyone attempts to append to the buffer, the
   *     exception thrown will have this parameter set as its cause
   */
  public void closeWithCause(Throwable cause) {
    lock.lock();
    try {
      if (shutdown) {
        // It is legal to close the buffer many times, but we should memoize only the first reason
        // for closure.
        return;
      }
      shutdownCause = cause;
      shutdown = true;
      notEmpty.signalAll();
      notFull.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
