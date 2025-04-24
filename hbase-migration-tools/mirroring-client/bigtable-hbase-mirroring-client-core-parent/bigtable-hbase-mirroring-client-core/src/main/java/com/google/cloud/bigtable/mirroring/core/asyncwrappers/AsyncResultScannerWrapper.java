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
package com.google.cloud.bigtable.mirroring.core.asyncwrappers;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.MirroringResultScanner;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * {@link MirroringResultScanner} schedules asynchronous next()s after synchronous operations to
 * verify consistency. HBase doesn't provide any Asynchronous API for Scanners thus we wrap
 * ResultScanner into AsyncResultScannerWrapper to enable async operations on scanners.
 *
 * <p>Note that next() method returns a Supplier<> as its result is used only in callbacks
 */
@InternalApi("For internal usage only")
public class AsyncResultScannerWrapper {
  /**
   * We use this queue to ensure that asynchronous next()s are called in the same order and with the
   * same parameters as next()s on primary result scanner.
   */
  private final ConcurrentLinkedQueue<ScannerRequestContext> nextContextQueue;

  /**
   * We use this queue to ensure that asynchronous verification of scan results is called in the
   * same order as next()s on primary result scanner.
   */
  public final ConcurrentLinkedQueue<AsyncScannerVerificationPayload> nextResultQueue;

  private final ResultScanner scanner;
  private final ListeningExecutorService executorService;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AsyncResultScannerWrapper(
      ResultScanner scanner,
      ListeningExecutorService executorService) {
    this.scanner = scanner;
    this.executorService = executorService;
    this.nextContextQueue = new ConcurrentLinkedQueue<>();
    this.nextResultQueue = new ConcurrentLinkedQueue<>();
  }

  public Supplier<ListenableFuture<Void>> next(final ScannerRequestContext context) {
    return new Supplier<ListenableFuture<Void>>() {
      @Override
      public ListenableFuture<Void> get() {
        nextContextQueue.add(context);
        return scheduleNext();
      }
    };
  }

  private ListenableFuture<Void> scheduleNext() {
    return this.executorService.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws AsyncScannerExceptionWithContext {
            synchronized (AsyncResultScannerWrapper.this) {
              final ScannerRequestContext requestContext =
                  AsyncResultScannerWrapper.this.nextContextQueue.remove();
              AsyncResultScannerWrapper.this.nextResultQueue.add(performNext(requestContext));
              return null;
            }
          }
        });
  }

  private AsyncScannerVerificationPayload performNext(final ScannerRequestContext requestContext)
      throws AsyncScannerExceptionWithContext {
    try {
      if (requestContext.singleNext) {
        return performNextSingle(requestContext);
      } else {
        return performNextMultiple(requestContext);
      }
    } catch (IOException e) {
      throw new AsyncScannerExceptionWithContext(e, requestContext);
    }
  }

  private AsyncScannerVerificationPayload performNextSingle(ScannerRequestContext requestContext)
      throws IOException {
    Result result = AsyncResultScannerWrapper.this.scanner.next();
    return new AsyncScannerVerificationPayload(requestContext, result);
  }

  private AsyncScannerVerificationPayload performNextMultiple(
      final ScannerRequestContext requestContext) throws IOException {
    Result[] result = AsyncResultScannerWrapper.this.scanner.next(requestContext.numRequests);
    return new AsyncScannerVerificationPayload(requestContext, result);
  }

  /**
   * This operation is thread-safe, but not asynchronous, because there is no need to invoke it
   * asynchronously.
   */
  public boolean renewLease() {
    synchronized (this) {
      return scanner.renewLease();
    }
  }

  public void close() {
    if (this.closed.getAndSet(true)) {
      return;
    }

    synchronized (this) {
      scanner.close();
    }
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
    /** Number of Results requested in current next call. */
    public final int numRequests;
    /**
     * Marks whether this next was issued using {@link ResultScanner#next()} (true) or {@link
     * ResultScanner#next(int)} (false). Used to forward the same method call to underlying
     * secondary scanner and to pass the result to appropriate verifier method.
     */
    public final boolean singleNext;

    private ScannerRequestContext(
        Scan scan, Result[] result, int numRequests, boolean singleNext) {
      this.scan = scan;
      this.result = result;
      this.numRequests = numRequests;
      this.singleNext = singleNext;
    }

    public ScannerRequestContext(Scan scan, Result[] result, int numRequests) {
      this(scan, result, numRequests, false);
    }

    public ScannerRequestContext(Scan scan, Result result) {
      this(scan, new Result[] {result}, 1, true);
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
