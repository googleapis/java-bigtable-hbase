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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog;

import static org.junit.Assert.*;

import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.*;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LogBufferTest {
  private <V> Future<V> runInThread(final Callable<V> callable) {
    final SettableFuture<V> res = SettableFuture.create();
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  res.set(callable.call());
                } catch (Throwable t) {
                  res.setException(t);
                }
              }
            });
    thread.setDaemon(true); // make sure it doesn't block shutdown if it blocks
    thread.start();
    return res;
  }

  /**
   * Verify if the future is not completing without any external actions.
   *
   * <p>We achieve it by simply waiting 100ms. False positives are possible, but false negatives are
   * not, so tests won't become flaky.
   *
   * @param future the future in question
   * @param <V> future's return type
   */
  private <V> void assertFutureNotCompleting(final Future<V> future) {
    assertThrows(
        TimeoutException.class,
        new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            future.get(100, TimeUnit.MILLISECONDS);
          }
        });
  }

  @Test
  public void admissionExactLimitIsObeyed() throws InterruptedException {
    final LogBuffer buffer = new LogBuffer(11, true);
    final byte[] buf1 = new byte[] {0, 1, 2};
    final byte[] buf2 = new byte[] {3, 4, 5, 6, 7, 8};
    final byte[] buf3 = new byte[] {9, 10};
    final byte[] buf4 = new byte[] {11};

    assertTrue(buffer.append(buf1));
    assertTrue(buffer.append(buf2));
    assertTrue(buffer.append(buf3));
    // No more space for buf4
    assertFalse(buffer.append(buf4));

    Queue<byte[]> res = buffer.drain();
    assertArrayEquals(buf1, res.poll());
    assertArrayEquals(buf2, res.poll());
    assertArrayEquals(buf3, res.poll());
    assertNull(res.poll());
  }

  @Test
  public void admissionNotExactLimitIsObeyed() throws InterruptedException {
    final LogBuffer buffer = new LogBuffer(11, true);
    final byte[] buf1 = new byte[] {0, 1, 2};
    final byte[] buf2 = new byte[] {3, 4, 5, 6, 7, 8};
    final byte[] buf3 = new byte[] {9, 10, 11};

    assertTrue(buffer.append(buf1));
    assertTrue(buffer.append(buf2));
    // no more space for buf3
    assertFalse(buffer.append(buf3));

    Queue<byte[]> res = buffer.drain();
    assertArrayEquals(buf1, res.poll());
    assertArrayEquals(buf2, res.poll());
    assertNull(res.poll());
  }

  @Test
  public void oneEntryLargerThanLimitCanBeAdmitted() throws InterruptedException {
    final LogBuffer buffer = new LogBuffer(3, true);
    final byte[] buf1 = new byte[] {0};
    final byte[] buf2 = new byte[] {1, 2, 3, 4};
    final byte[] buf3 = new byte[] {5, 6, 7, 8, 9, 10, 11};
    assertTrue(buffer.append(buf1));

    // `buf2` doesn't get admitted because there is no space.
    assertFalse(buffer.append(buf2));
    Queue<byte[]> res = buffer.drain();
    assertArrayEquals(buf1, res.poll());
    assertNull(res.poll());

    // `buf3` gets admitted even though it's larger than the total space limit, because it's the
    // only entry.
    assertTrue(buffer.append(buf3));
    res = buffer.drain();
    assertArrayEquals(buf3, res.poll());
    assertNull(res.poll());
  }

  @Test
  public void overflowBlocks() throws InterruptedException, ExecutionException {
    final LogBuffer buffer = new LogBuffer(11, false);
    final byte[] buf1 = new byte[] {0, 1, 2};
    final byte[] buf2 = new byte[] {3, 4, 5, 6, 7, 8};
    final byte[] buf3 = new byte[] {9, 10, 11};

    assertTrue(buffer.append(buf1));
    assertTrue(buffer.append(buf2));
    // There is no space for `buf3`, so `buffer.append(buf3)` should block, so we run it in a
    // separate thread.
    final Future<Void> threadCompletion =
        runInThread(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                buffer.append(buf3);
                return null;
              }
            });

    // A reasonably confident check if to verify that the thread is blocked. No false negatives are
    // possible.
    assertFutureNotCompleting(threadCompletion);

    Queue<byte[]> res = buffer.drain();
    assertArrayEquals(buf1, res.poll());
    assertArrayEquals(buf2, res.poll());
    assertNull(res.poll());

    // After we've drained, `buf3` should have been finally admitted and thread exited.
    threadCompletion.get();
    res = buffer.drain();
    assertArrayEquals(buf3, res.poll());
    assertNull(res.poll());
  }

  @Test
  public void emptyBufferBlocks() throws InterruptedException, ExecutionException {
    final LogBuffer buffer = new LogBuffer(11, true);
    final byte[] buf1 = new byte[] {0, 1, 2};

    final Future<Void> threadCompletion =
        runInThread(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                Queue<byte[]> res = buffer.drain();
                assertArrayEquals(buf1, res.poll());
                assertNull(res.poll());
                return null;
              }
            });

    // A reasonably confident check to verify that the thread is blocked. No false negatives are
    // possible.
    assertFutureNotCompleting(threadCompletion);

    assertTrue(buffer.append(buf1));
    // Now the thread should have unblocked and finished.
    threadCompletion.get();
  }

  @Test
  public void appendingToClosedBufferThrows() {
    final LogBuffer buffer = new LogBuffer(11, true);
    buffer.close();
    final byte[] buf1 = new byte[] {0, 1, 2};
    assertThrows(
        IllegalStateException.class,
        new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            buffer.append(buf1);
          }
        });
  }

  @Test
  public void drainingClosedBufferReturnsContentsAndThenNull() throws InterruptedException {
    final LogBuffer buffer = new LogBuffer(11, true);
    final byte[] buf1 = new byte[] {0, 1, 2};
    buffer.append(buf1);
    buffer.close();

    // The buffer should still return the data it admitted before closing.
    Queue<byte[]> res = buffer.drain();
    assertArrayEquals(buf1, res.poll());
    assertNull(res.poll());

    // The buffer should no longer block on `drain()`. Instead, it should return `null`.
    res = buffer.drain();
    assertNull(res);
  }

  @Test
  public void closingUnblocksDrain() throws InterruptedException {
    final LogBuffer buffer = new LogBuffer(11, true);

    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  Queue<byte[]> res = buffer.drain();
                  assertNull(res);
                } catch (InterruptedException e) {
                  fail("Test thread interrupted.");
                }
              }
            });
    thread.start();
    thread.join(100);
    // A reasonably confident check if to verify that the thread is blocked. No false negatives are
    // possible.
    assertTrue(thread.isAlive());

    buffer.close();
    // Now the thread should have unblocked and finished.
    thread.join();
  }

  @Test
  public void closingUnblocksAppend() throws InterruptedException, ExecutionException {
    final LogBuffer buffer = new LogBuffer(3, false);
    final byte[] buf1 = new byte[] {0, 1, 2};
    final byte[] buf2 = new byte[] {3};

    assertTrue(buffer.append(buf1));
    // There is no space for `buf2`, so `buffer.append(buf2)` should block, so we run it in a
    // separate thread.
    final Future<Void> threadCompletion =
        runInThread(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                assertThrows(
                    IllegalStateException.class,
                    new ThrowingRunnable() {
                      @Override
                      public void run() throws Throwable {
                        buffer.append(buf2);
                      }
                    });

                return null;
              }
            });

    // A reasonably confident check if to verify that the thread is blocked. No false negatives are
    // possible.
    assertFutureNotCompleting(threadCompletion);

    buffer.close();
    // After we've closed, `buf2` should have been finally rejected and thread exited with an
    // exception.
    threadCompletion.get();

    Queue<byte[]> res = buffer.drain();
    assertArrayEquals(buf1, res.poll());
    assertNull(res.poll());

    // There's nothing left in the buffer.
    assertNull(buffer.drain());
  }

  @Test
  public void closureCauseIsReported() throws InterruptedException {
    final LogBuffer buffer = new LogBuffer(11, true);
    buffer.closeWithCause(new IOException("foo"));
    buffer.closeWithCause(new IOException("bar"));
    buffer.close();
    final byte[] buf1 = new byte[] {0, 1, 2};
    try {
      buffer.append("foo".getBytes(StandardCharsets.UTF_8));
      fail("IllegalStateException was expected.");
    } catch (IllegalStateException e) {
      Throwable cause = e.getCause();
      assertNotNull(cause);
      assertEquals("foo", cause.getMessage());
    }
  }
}
