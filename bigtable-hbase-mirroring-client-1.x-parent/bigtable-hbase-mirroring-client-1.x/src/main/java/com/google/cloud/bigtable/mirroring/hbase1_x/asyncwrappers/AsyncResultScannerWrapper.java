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
package com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringResultScanner;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 * {@link MirroringResultScanner} schedules asynchronous next()s after synchronous operations to
 * verify consistency. HBase doesn't provide any Asynchronous API for Scanners thus we wrap
 * ResultScanner into AsyncResultScannerWrapper to enable async operations on scanners.
 *
 * <p>Note that next() method returns a Supplier<> as its result is used only in callbacks
 */
@InternalApi("For internal usage only")
public class AsyncResultScannerWrapper implements ListenableCloseable {
  private final Table table;
  private ResultScanner scanner;
  private ListeningExecutorService executorService;
  private boolean closed;

  /**
   * We use this queue to ensure that asynchronous next()s are called in the same order and with the
   * same parameters as next()s on primary result scanner.
   */
  private final ConcurrentLinkedQueue<ScannerRequestContext> nextContextQueue;

  /**
   * We are counting references to this object to be able to call {@link ResultScanner#close()} on
   * underlying scanner in a predictable way. The reference count is increased before submitting
   * each asynchronous task, and decreased after it finishes. Moreover, this object holds an
   * implicit self-reference, which in released in {@link #asyncClose()}.
   *
   * <p>In this way we are able to call ResultScanner#close() only if all scheduled tasks have
   * finished and #asyncClose() was called.
   */
  private ListenableReferenceCounter pendingOperationsReferenceCounter;

  public AsyncResultScannerWrapper(
      Table table, ResultScanner scanner, ListeningExecutorService executorService) {
    super();
    this.table = table;
    this.scanner = scanner;
    this.pendingOperationsReferenceCounter = new ListenableReferenceCounter();
    this.executorService = executorService;
    this.closed = false;
    this.nextContextQueue = new ConcurrentLinkedQueue<>();
  }

  public Supplier<ListenableFuture<AsyncScannerVerificationPayload>> next(
      final ScannerRequestContext context) {
    return new Supplier<ListenableFuture<AsyncScannerVerificationPayload>>() {
      @Override
      public ListenableFuture<AsyncScannerVerificationPayload> get() {
        nextContextQueue.add(context);
        ListenableFuture<AsyncScannerVerificationPayload> future = scheduleNext();
        pendingOperationsReferenceCounter.holdReferenceUntilCompletion(future);
        return future;
      }
    };
  }

  private ListenableFuture<AsyncScannerVerificationPayload> scheduleNext() {
    return this.executorService.submit(
        new Callable<AsyncScannerVerificationPayload>() {
          @Override
          public AsyncScannerVerificationPayload call() throws AsyncScannerExceptionWithContext {
            synchronized (AsyncResultScannerWrapper.this.table) {
              ScannerRequestContext requestContext =
                  AsyncResultScannerWrapper.this.nextContextQueue.remove();

              try {
                if (requestContext.singleNext) {
                  Result result = AsyncResultScannerWrapper.this.scanner.next();
                  return new AsyncScannerVerificationPayload(requestContext, result);
                } else {
                  Result[] result =
                      AsyncResultScannerWrapper.this.scanner.next(requestContext.numRequests);
                  return new AsyncScannerVerificationPayload(requestContext, result);
                }
              } catch (IOException e) {
                throw new AsyncScannerExceptionWithContext(e, requestContext);
              }
            }
          }
        });
  }

  public ListenableFuture<Boolean> renewLease() {
    return submitTask(
        new Callable<Boolean>() {
          @Override
          public Boolean call() {
            boolean result;
            synchronized (table) {
              result = scanner.renewLease();
            }
            return result;
          }
        });
  }

  public synchronized ListenableFuture<Void> asyncClose() {
    if (this.closed) {
      return this.pendingOperationsReferenceCounter.getOnLastReferenceClosed();
    }
    this.closed = true;

    this.pendingOperationsReferenceCounter.decrementReferenceCount();
    this.pendingOperationsReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(
            new Runnable() {
              @Override
              public void run() {
                synchronized (table) {
                  scanner.close();
                }
              }
            },
            MoreExecutors.directExecutor());
    return this.pendingOperationsReferenceCounter.getOnLastReferenceClosed();
  }

  public <T> ListenableFuture<T> submitTask(Callable<T> task) {
    ListenableFuture<T> future = this.executorService.submit(task);
    this.pendingOperationsReferenceCounter.holdReferenceUntilCompletion(future);
    return future;
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.pendingOperationsReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }

  /**
   * Context of single execution of {@link ResultScanner#next()} and {@link
   * ResultScanner#next(int)}.
   */
  public static class ScannerRequestContext {

    /** Scan object that created this result scanner. */
    public final Scan scan;
    /** Results of corresponding scan operation on primary ResultScanner. */
    public final Result[] result;
    /** Number of Results that were retrieved from this scanner before current request. */
    public final int startingIndex;
    /** Number of Results requested in current next call. */
    public final int numRequests;
    /**
     * Marks whether this next was issued using {@link ResultScanner#next()} (true) or {@link
     * ResultScanner#next(int)} (false). Used to forward the same method call to underlying
     * secondary scanner and to pass the result to appropriate verifier method.
     */
    public final boolean singleNext;

    private ScannerRequestContext(
        Scan scan, Result[] result, int startingIndex, int numRequests, boolean singleNext) {
      this.scan = scan;
      this.result = result;
      this.startingIndex = startingIndex;
      this.numRequests = numRequests;
      this.singleNext = singleNext;
    }

    public ScannerRequestContext(Scan scan, Result[] result, int startingIndex, int numRequests) {
      this(scan, result, startingIndex, numRequests, false);
    }

    public ScannerRequestContext(Scan scan, Result result, int startingIndex) {
      this(scan, new Result[] {result}, startingIndex, 1, true);
    }
  }

  public static class AsyncScannerVerificationPayload {
    public final ScannerRequestContext context;
    public final Result[] secondary;

    public AsyncScannerVerificationPayload(ScannerRequestContext context, Result[] secondary) {
      this.context = context;
      this.secondary = secondary;
    }

    public AsyncScannerVerificationPayload(ScannerRequestContext context, Result secondary) {
      this.context = context;
      this.secondary = new Result[] {secondary};
    }
  }

  public static class AsyncScannerExceptionWithContext extends Exception {
    public final ScannerRequestContext context;

    public AsyncScannerExceptionWithContext(Throwable cause, ScannerRequestContext context) {
      super(cause);
      this.context = context;
    }
  }
}
