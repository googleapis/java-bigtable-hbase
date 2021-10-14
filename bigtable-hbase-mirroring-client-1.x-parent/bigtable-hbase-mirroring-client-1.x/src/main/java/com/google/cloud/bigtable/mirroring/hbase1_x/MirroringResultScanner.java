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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncResultScannerWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncResultScannerWrapper.ScannerRequestContext;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

/**
 * {@link ResultScanner} that performs reads on two underlying tables.
 *
 * <p>Every read is performed synchronously on `ResultScanner` from primary `Table` and then, if it
 * succeeded, asynchronously on secondary `ResultScanner`.
 */
@InternalApi("For internal usage only")
public class MirroringResultScanner extends AbstractClientScanner implements ListenableCloseable {
  private static final Log log = LogFactory.getLog(MirroringResultScanner.class);
  private final MirroringTracer mirroringTracer;

  private final Scan originalScan;
  private final ResultScanner primaryResultScanner;
  private final AsyncResultScannerWrapper secondaryResultScannerWrapper;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final ListenableReferenceCounter listenableReferenceCounter;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  /**
   * Keeps track of number of entries already read from this scanner to provide context for
   * MismatchDetectors.
   */
  private int readEntries;

  private FlowController flowController;
  private boolean isVerificationEnabled;

  public MirroringResultScanner(
      Scan originalScan,
      ResultScanner primaryResultScanner,
      AsyncTableWrapper secondaryTableWrapper,
      VerificationContinuationFactory verificationContinuationFactory,
      FlowController flowController,
      MirroringTracer mirroringTracer,
      boolean isVerificationEnabled)
      throws IOException {
    this.originalScan = originalScan;
    this.primaryResultScanner = primaryResultScanner;
    this.secondaryResultScannerWrapper = secondaryTableWrapper.getScanner(originalScan);
    this.verificationContinuationFactory = verificationContinuationFactory;
    this.listenableReferenceCounter = new ListenableReferenceCounter();
    this.flowController = flowController;
    this.readEntries = 0;

    this.listenableReferenceCounter.holdReferenceUntilClosing(this.secondaryResultScannerWrapper);
    this.mirroringTracer = mirroringTracer;
    this.isVerificationEnabled = isVerificationEnabled;
  }

  @Override
  public Result next() throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.NEXT)) {

      Result result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result>() {
                @Override
                public Result call() throws IOException {
                  return MirroringResultScanner.this.primaryResultScanner.next();
                }
              },
              HBaseOperation.NEXT);

      int startingIndex = this.readEntries;
      this.readEntries += 1;
      ScannerRequestContext context =
          new ScannerRequestContext(
              this.originalScan,
              result,
              startingIndex,
              this.mirroringTracer.spanFactory.getCurrentSpan());

      this.scheduleRequest(
          new RequestResourcesDescription(result),
          this.secondaryResultScannerWrapper.next(context),
          this.verificationContinuationFactory.scannerNext());
      return result;
    }
  }

  @Override
  public Result[] next(final int entriesToRead) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.NEXT_MULTIPLE)) {
      Result[] results =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result[]>() {
                @Override
                public Result[] call() throws IOException {
                  return MirroringResultScanner.this.primaryResultScanner.next(entriesToRead);
                }
              },
              HBaseOperation.NEXT_MULTIPLE);

      int startingIndex = this.readEntries;
      this.readEntries += entriesToRead;
      ScannerRequestContext context =
          new ScannerRequestContext(
              this.originalScan,
              results,
              startingIndex,
              entriesToRead,
              this.mirroringTracer.spanFactory.getCurrentSpan());
      this.scheduleRequest(
          new RequestResourcesDescription(results),
          this.secondaryResultScannerWrapper.next(context),
          this.verificationContinuationFactory.scannerNext());
      return results;
    }
  }

  @Override
  public void close() {
    if (this.closed.getAndSet(true)) {
      return;
    }

    AccumulatedExceptions exceptionsList = new AccumulatedExceptions();
    try {
      this.primaryResultScanner.close();
    } catch (RuntimeException e) {
      exceptionsList.add(e);
    } finally {
      try {
        this.asyncClose();
      } catch (RuntimeException e) {
        log.error("Exception while scheduling this.close().", e);
        exceptionsList.add(e);
      }
    }

    exceptionsList.rethrowAsRuntimeExceptionIfCaptured();
  }

  @VisibleForTesting
  ListenableFuture<Void> asyncClose() {
    this.secondaryResultScannerWrapper.asyncClose();
    this.listenableReferenceCounter.decrementReferenceCount();
    return this.listenableReferenceCounter.getOnLastReferenceClosed();
  }

  @Override
  public boolean renewLease() {
    boolean primaryLease = this.primaryResultScanner.renewLease();
    if (!primaryLease) {
      return false;
    }

    try {
      return this.secondaryResultScannerWrapper.renewLease().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      log.error("Execution exception in secondaryResultScannerWrapper.renewLease().", e.getCause());
      return false;
    } catch (Exception e) {
      log.error("Exception while scheduling secondaryResultScannerWrapper.renewLease().", e);
      return false;
    }
  }

  @Override
  public ScanMetrics getScanMetrics() {
    throw new UnsupportedOperationException();
  }

  private <T> void scheduleRequest(
      RequestResourcesDescription requestResourcesDescription,
      Supplier<ListenableFuture<T>> nextSupplier,
      FutureCallback<T> scannerNext) {
    if (!this.isVerificationEnabled) {
      return;
    }
    this.listenableReferenceCounter.holdReferenceUntilCompletion(
        RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
            requestResourcesDescription,
            nextSupplier,
            this.mirroringTracer.spanFactory.wrapReadVerificationCallback(scannerNext),
            this.flowController,
            this.mirroringTracer));
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.listenableReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }
}
