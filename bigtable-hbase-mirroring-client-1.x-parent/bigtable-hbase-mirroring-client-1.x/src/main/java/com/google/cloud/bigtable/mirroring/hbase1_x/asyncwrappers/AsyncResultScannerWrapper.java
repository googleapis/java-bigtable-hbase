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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
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
  private final MirroringTracer mirroringTracer;
  /**
   * We use this queue to ensure that asynchronous next()s are called in the same order and with the
   * same parameters as next()s on primary result scanner.
   */
  private final ConcurrentLinkedQueue<ScannerRequestContext> nextContextQueue;

  private final ResultScanner scanner;
  private final ListeningExecutorService executorService;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AsyncResultScannerWrapper(
      ResultScanner scanner,
      ListeningExecutorService executorService,
      MirroringTracer mirroringTracer) {
    this.scanner = scanner;
    this.mirroringTracer = mirroringTracer;
    this.executorService = executorService;
    this.nextContextQueue = new ConcurrentLinkedQueue<>();
  }

  public Supplier<ListenableFuture<AsyncScannerVerificationPayload>> next(
      final ScannerRequestContext context) {
    return new Supplier<ListenableFuture<AsyncScannerVerificationPayload>>() {
      @Override
      public ListenableFuture<AsyncScannerVerificationPayload> get() {
        nextContextQueue.add(context);
        return scheduleNext();
      }
    };
  }

  private ListenableFuture<AsyncScannerVerificationPayload> scheduleNext() {
    return this.executorService.submit(
        new Callable<AsyncScannerVerificationPayload>() {
          @Override
          public AsyncScannerVerificationPayload call() throws AsyncScannerExceptionWithContext {
            synchronized (AsyncResultScannerWrapper.this) {
              final ScannerRequestContext requestContext =
                  AsyncResultScannerWrapper.this.nextContextQueue.remove();
              try (Scope scope =
                  AsyncResultScannerWrapper.this.mirroringTracer.spanFactory.spanAsScope(
                      requestContext.span)) {
                return performNext(requestContext);
              }
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
    Result result =
        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Result>() {
              @Override
              public Result call() throws IOException {
                return AsyncResultScannerWrapper.this.scanner.next();
              }
            },
            HBaseOperation.NEXT);
    return new AsyncScannerVerificationPayload(requestContext, result);
  }

  private AsyncScannerVerificationPayload performNextMultiple(
      final ScannerRequestContext requestContext) throws IOException {
    Result[] result =
        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Result[]>() {
              @Override
              public Result[] call() throws IOException {
                return AsyncResultScannerWrapper.this.scanner.next(requestContext.numRequests);
              }
            },
            HBaseOperation.NEXT_MULTIPLE);
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
    /** Number of Results that were retrieved from this scanner before current request. */
    public final int startingIndex;
    /** Number of Results requested in current next call. */
    public final int numRequests;
    /** Tracing Span will be used as a parent span of current request. */
    public final Span span;
    /**
     * Marks whether this next was issued using {@link ResultScanner#next()} (true) or {@link
     * ResultScanner#next(int)} (false). Used to forward the same method call to underlying
     * secondary scanner and to pass the result to appropriate verifier method.
     */
    public final boolean singleNext;

    private ScannerRequestContext(
        Scan scan,
        Result[] result,
        int startingIndex,
        int numRequests,
        boolean singleNext,
        Span span) {
      this.scan = scan;
      this.result = result;
      this.startingIndex = startingIndex;
      this.numRequests = numRequests;
      this.span = span;
      this.singleNext = singleNext;
    }

    public ScannerRequestContext(
        Scan scan, Result[] result, int startingIndex, int numRequests, Span span) {
      this(scan, result, startingIndex, numRequests, false, span);
    }

    public ScannerRequestContext(Scan scan, Result result, int startingIndex, Span span) {
      this(scan, new Result[] {result}, startingIndex, 1, true, span);
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
