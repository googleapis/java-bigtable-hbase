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
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
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

  private Scan originalScan;
  private ResultScanner primaryResultScanner;
  private AsyncResultScannerWrapper secondaryResultScannerWrapper;
  private VerificationContinuationFactory verificationContinuationFactory;
  private ListenableReferenceCounter listenableReferenceCounter;
  private boolean closed = false;
  /**
   * Keeps track of number of entries already read from this scanner to provide context for
   * MismatchDetectors.
   */
  private int readEntries;

  private FlowController flowController;

  public MirroringResultScanner(
      Scan originalScan,
      ResultScanner primaryResultScanner,
      AsyncTableWrapper secondaryTableWrapper,
      VerificationContinuationFactory verificationContinuationFactory,
      FlowController flowController)
      throws IOException {
    this.originalScan = originalScan;
    this.primaryResultScanner = primaryResultScanner;
    this.secondaryResultScannerWrapper = secondaryTableWrapper.getScanner(originalScan);
    this.verificationContinuationFactory = verificationContinuationFactory;
    this.listenableReferenceCounter = new ListenableReferenceCounter();
    this.flowController = flowController;
    this.readEntries = 0;

    this.listenableReferenceCounter.holdReferenceUntilClosing(this.secondaryResultScannerWrapper);
  }

  @Override
  public Result next() throws IOException {
    Result result = this.primaryResultScanner.next();
    int startingIndex = this.readEntries;
    this.readEntries += 1;
    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryResultScannerWrapper.next(),
        this.verificationContinuationFactory.scannerNext(this.originalScan, startingIndex, result),
        this.flowController);
    return result;
  }

  @Override
  public Result[] next(int entriesToRead) throws IOException {
    Result[] results = this.primaryResultScanner.next(entriesToRead);
    int startingIndex = this.readEntries;
    this.readEntries += entriesToRead;
    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(results),
        this.secondaryResultScannerWrapper.next(entriesToRead),
        this.verificationContinuationFactory.scannerNext(
            this.originalScan, startingIndex, entriesToRead, results),
        this.flowController);
    return results;
  }

  @Override
  public synchronized void close() {
    if (this.closed) {
      return;
    }

    RuntimeException firstException = null;
    try {
      this.primaryResultScanner.close();
    } catch (RuntimeException e) {
      firstException = e;
    } finally {
      try {
        this.asyncClose();
      } catch (RuntimeException e) {
        log.error("Exception while scheduling this.close().", e);
        if (firstException == null) {
          firstException = e;
        } else {
          // Attach current exception as suppressed to make it visible in `firstException`s
          // stacktrace.
          firstException.addSuppressed(e);
        }
      }
    }

    this.closed = true;
    if (firstException != null) {
      throw firstException;
    }
  }

  public synchronized ListenableFuture<Void> asyncClose() {
    if (!this.closed) {
      this.secondaryResultScannerWrapper.asyncClose();
      this.listenableReferenceCounter.decrementReferenceCount();
    }
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
      ListenableFuture<T> next,
      FutureCallback<T> scannerNext) {
    this.listenableReferenceCounter.holdReferenceUntilCompletion(
        RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
            requestResourcesDescription, next, scannerNext, this.flowController));
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.listenableReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }
}
