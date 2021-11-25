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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.canBatchBePerformedConcurrently;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.reconcileBatchResultsConcurrent;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.reconcileBatchResultsSequential;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable.RequestScheduler;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.BatchData;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.FailedSuccessfulSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.ReadWriteSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.OperationUtils.RewrittenIncrementAndAppendIndicesInfo;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Helper class that handles logic related to mirroring {@link Table#batch(List, Object[])}
 * operation. Handles performing both sequential and concurrent operations.
 *
 * <p>Uses static helper methods from {@link BatchHelpers}.
 */
public class Batcher {
  private static final Logger Log = new Logger(Batcher.class);

  private final Table primaryTable;
  private final AsyncTableWrapper secondaryAsyncWrapper;
  private final RequestScheduler requestScheduler;
  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final ReadSampler readSampler;
  private final Predicate<Object> resultIsFaultyPredicate;
  private final boolean waitForSecondaryWrites;
  private final boolean performWritesConcurrently;
  private final MirroringTracer mirroringTracer;

  public Batcher(
      Table primaryTable,
      AsyncTableWrapper secondaryAsyncWrapper,
      RequestScheduler requestScheduler,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      VerificationContinuationFactory verificationContinuationFactory,
      ReadSampler readSampler,
      Predicate<Object> resultIsFaultyPredicate,
      boolean waitForSecondaryWrites,
      boolean performWritesConcurrently,
      MirroringTracer mirroringTracer) {
    this.primaryTable = primaryTable;
    this.secondaryAsyncWrapper = secondaryAsyncWrapper;
    this.requestScheduler = requestScheduler;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.verificationContinuationFactory = verificationContinuationFactory;
    this.readSampler = readSampler;
    this.resultIsFaultyPredicate = resultIsFaultyPredicate;
    this.waitForSecondaryWrites = waitForSecondaryWrites;
    this.performWritesConcurrently = performWritesConcurrently;
    this.mirroringTracer = mirroringTracer;
  }

  public void batchSingleWriteOperation(Row operation) throws IOException {
    Object[] results = new Object[1];
    try {
      batch(Collections.singletonList(operation), results);
    } catch (RetriesExhaustedWithDetailsException e) {
      Throwable exception = e.getCause(0);
      if (exception instanceof IOException) {
        throw (IOException) exception;
      }
      throw new IOException(exception);
    } catch (InterruptedException e) {
      InterruptedIOException interruptedIOException = new InterruptedIOException();
      interruptedIOException.initCause(e);
      throw interruptedIOException;
    }
  }

  public void batch(final List<? extends Row> inputOperations, final Object[] results)
      throws IOException, InterruptedException {
    batch(inputOperations, results, null);
  }

  /**
   * Performs batch operation as defined by HBase API. {@code results} array will contain instances
   * of {@link Result} for successful operations and {@code null} or {@link Throwable} for
   * operations that have failed (this behavior is not documented, but both hbase and java-bigtable
   * clients work this way). Moreover, if any of operations in batch have failed, {@link
   * RetriesExhaustedWithDetailsException} will be thrown with details of failed operations (which
   * is also not documented both clients consistently throw this exception).
   */
  public <R> void batch(
      final List<? extends Row> inputOperations,
      final Object[] results,
      @Nullable final Callback<R> callback)
      throws IOException, InterruptedException {
    final RewrittenIncrementAndAppendIndicesInfo<? extends Row> actions =
        new RewrittenIncrementAndAppendIndicesInfo<>(inputOperations);
    Log.trace(
        "[%s] batch(operations=%s, results)", this.primaryTable.getName(), actions.operations);

    // We store batch results in a internal variable to prevent the user from modifying it when it
    // might still be used by asynchronous secondary operation.
    final Object[] internalPrimaryResults = new Object[results.length];

    CallableThrowingIOAndInterruptedException<Void> primaryOperation =
        new CallableThrowingIOAndInterruptedException<Void>() {
          @Override
          public Void call() throws IOException, InterruptedException {
            if (callback == null) {
              primaryTable.batch(actions.operations, internalPrimaryResults);
            } else {
              primaryTable.batchCallback(actions.operations, internalPrimaryResults, callback);
            }
            return null;
          }
        };

    try {
      if (!this.performWritesConcurrently || !canBatchBePerformedConcurrently(actions.operations)) {
        sequentialBatch(internalPrimaryResults, actions.operations, primaryOperation);
      } else {
        concurrentBatch(internalPrimaryResults, actions.operations, primaryOperation);
      }
    } finally {
      actions.discardUnwantedResults(internalPrimaryResults);
      System.arraycopy(internalPrimaryResults, 0, results, 0, results.length);
    }
  }

  private void sequentialBatch(
      Object[] results,
      List<? extends Row> operations,
      CallableThrowingIOAndInterruptedException<Void> primaryOperation)
      throws IOException, InterruptedException {
    if (this.waitForSecondaryWrites) {
      sequentialSynchronousBatch(results, operations, primaryOperation);
    } else {
      sequentialAsynchronousBatch(results, operations, primaryOperation);
    }
  }

  /**
   * Performs batch of {@code operations} synchronously on primary and asynchronously on secondary.
   *
   * <p>Operations that failed on primary are not mirrored on secondary to prevent ghost writes to
   * secondary.
   *
   * <p>{@code results} contains results of primary batch and any exception thrown by primary batch
   * is forwarded to the user as-is.
   *
   * <p>This mode doesn't incur any additional latency and prevents ghost writes, but the user
   * cannot handle secondary errors manually, they are always handled by {@link
   * SecondaryWriteErrorConsumer}.
   */
  private void sequentialAsynchronousBatch(
      Object[] results,
      List<? extends Row> operations,
      CallableThrowingIOAndInterruptedException<Void> primaryOperation)
      throws IOException, InterruptedException {
    try {
      this.mirroringTracer.spanFactory.wrapPrimaryOperation(primaryOperation, HBaseOperation.BATCH);
    } finally {
      scheduleSecondaryWriteBatchOperations(operations, results);
    }
  }

  /**
   * Performs batch of {@code operations} synchronously on primary and on secondary.
   *
   * <p>Operations that failed on primary are not mirrored on secondary to prevent ghost writes to
   * secondary.
   *
   * <p>{@code results} contains results of operations that succeeded on both databases and {@link
   * Throwable}s for operations that failed on one of the databases. {@code Throwable}s are marked
   * with {@link MirroringOperationException} denoting which database rejected the operation. {@link
   * RetriesExhaustedWithDetailsException} is thrown by this method in case of any failures, causes
   * in that exception are also marked with {@link MirroringOperationException}.
   *
   * <p>This mode incurs additional latency and prevents ghost writes, but the user can handle
   * secondary errors manually, but they are also passed to {@link SecondaryWriteErrorConsumer}.
   */
  private void sequentialSynchronousBatch(
      Object[] results,
      List<? extends Row> operations,
      CallableThrowingIOAndInterruptedException<Void> primaryOperation)
      throws IOException, InterruptedException {
    BatchData primaryBatchData = new BatchData(operations, results);
    try {
      this.mirroringTracer.spanFactory.wrapPrimaryOperation(primaryOperation, HBaseOperation.BATCH);
    } catch (RetriesExhaustedWithDetailsException e) {
      primaryBatchData.setException(e);
    } catch (InterruptedException e) {
      throw MirroringOperationException.markedAsPrimaryException(e, null);
    } catch (IOException e) {
      throw MirroringOperationException.markedAsPrimaryException(e, null);
    }

    ListenableFuture<BatchData> secondaryResult =
        scheduleSecondaryWriteBatchOperations(operations, results);

    BatchData secondaryBatchData;
    try {
      secondaryBatchData = secondaryResult.get();
    } catch (ExecutionException e) {
      throw new IllegalStateException("secondaryResult thrown unexpected exception.");
    }
    reconcileBatchResultsSequential(
        results, primaryBatchData, secondaryBatchData, resultIsFaultyPredicate);
  }

  private static void throwBatchDataExceptionIfPresent(BatchData primaryBatchData)
      throws InterruptedException, IOException {
    Throwable exception = primaryBatchData.getException();
    if (exception != null) {
      if (exception instanceof InterruptedException) {
        throw (InterruptedException) exception;
      } else {
        throw (IOException) exception;
      }
    }
  }

  private ListenableFuture<BatchData> scheduleSecondaryWriteBatchOperations(
      final List<? extends Row> operations, final Object[] results) {
    final SettableFuture<BatchData> result = SettableFuture.create();

    boolean skipReads = !readSampler.shouldNextReadOperationBeSampled();
    final FailedSuccessfulSplit<? extends Row, Result> failedSuccessfulSplit =
        BatchHelpers.createOperationsSplit(
            operations, results, resultIsFaultyPredicate, Result.class, skipReads);

    if (failedSuccessfulSplit.successfulOperations.size() == 0) {
      result.set(new BatchData(Collections.<Row>emptyList(), new Object[0]));
      return result;
    }

    List<? extends Row> operationsToScheduleOnSecondary =
        BatchHelpers.rewriteIncrementsAndAppendsAsPuts(
            failedSuccessfulSplit.successfulOperations, failedSuccessfulSplit.successfulResults);

    final Object[] resultsSecondary = new Object[operationsToScheduleOnSecondary.size()];

    final BatchData secondaryBatchData =
        new BatchData(operationsToScheduleOnSecondary, resultsSecondary);

    // List of writes created by this call contains Puts instead of Increments and Appends and it
    // can be passed to secondaryWriteErrorConsumer.
    final ReadWriteSplit<? extends Row, Result> successfulReadWriteSplit =
        new ReadWriteSplit<>(
            failedSuccessfulSplit.successfulOperations,
            failedSuccessfulSplit.successfulResults,
            Result.class);

    final FutureCallback<Void> verificationFuture =
        BatchHelpers.createBatchVerificationCallback(
            failedSuccessfulSplit,
            successfulReadWriteSplit,
            resultsSecondary,
            verificationContinuationFactory.getMismatchDetector(),
            this.secondaryWriteErrorConsumer,
            resultIsFaultyPredicate,
            this.mirroringTracer);

    FutureCallback<Void> verificationCallback =
        new FutureCallback<Void>() {
          @Override
          public void onSuccess(@NullableDecl Void aVoid) {
            verificationFuture.onSuccess(aVoid);
          }

          @Override
          public void onFailure(Throwable throwable) {
            secondaryBatchData.setException(throwable);
            verificationFuture.onFailure(throwable);
          }
        };

    RequestResourcesDescription requestResourcesDescription =
        new RequestResourcesDescription(
            operationsToScheduleOnSecondary, successfulReadWriteSplit.readResults);

    // If flow controller errs and won't allow the request we will handle the error using this
    // handler.
    Function<Throwable, Void> flowControlReservationErrorConsumer =
        new Function<Throwable, Void>() {
          @Override
          public Void apply(Throwable throwable) {
            secondaryBatchData.setException(throwable);
            secondaryWriteErrorConsumer.consume(
                HBaseOperation.BATCH, successfulReadWriteSplit.writeOperations, throwable);
            return null;
          }
        };

    ListenableFuture<Void> verificationCompleted =
        this.requestScheduler.scheduleRequestWithCallback(
            requestResourcesDescription,
            this.secondaryAsyncWrapper.batch(operationsToScheduleOnSecondary, resultsSecondary),
            verificationCallback,
            flowControlReservationErrorConsumer);

    verificationCompleted.addListener(
        new Runnable() {
          @Override
          public void run() {
            result.set(secondaryBatchData);
          }
        },
        MoreExecutors.directExecutor());

    return result;
  }

  /**
   * Runs batch operation concurrently on both primary and secondary databases and waits for both of
   * them to finish. {@code primaryResults} parameter will contain correct {@link Result}s if
   * corresponding operation was successful on both databases and a {@link Throwable} marked with
   * {@link MirroringOperationException} if the operation failed on any database. If any operation
   * failed this method will throw appropriate {@link RetriesExhaustedWithDetailsException} with
   * causes marked with {@link MirroringOperationException}. The user can use those markers to
   * handle exceptions on primary and secondary database according to their needs.
   *
   * <p>This mode allows the user to manually handle errors on secondary database without additional
   * latency introduced by sequential synchronous mode, but it comes at a price of additional
   * inconsistency between primary and secondary databases - some of the writes might fail on
   * primary but succeed on primary.
   *
   * <p>This mode doesn't use {@link SecondaryWriteErrorConsumer} to handle failed writes on
   * secondary, errors are reported to the user as exceptions.
   *
   * <p>Only {@link org.apache.hadoop.hbase.client.Put}s, {@link
   * org.apache.hadoop.hbase.client.Delete}s and {@link org.apache.hadoop.hbase.client.RowMutations}
   * that consist of them can be executed concurrently, if {@code operations} contain any other
   * operation this method shouldn't be called.
   */
  private void concurrentBatch(
      final Object[] primaryResults,
      final List<? extends Row> operations,
      final CallableThrowingIOAndInterruptedException<Void> primaryOperation)
      throws IOException, InterruptedException {
    Preconditions.checkArgument(this.waitForSecondaryWrites && this.performWritesConcurrently);

    RequestResourcesDescription requestResourcesDescription =
        new RequestResourcesDescription(operations, new Result[0]);
    final Object[] secondaryResults = new Object[operations.size()];
    final Throwable[] flowControllerException = new Throwable[1];

    final BatchData primaryBatchData = new BatchData(operations, primaryResults);
    final BatchData secondaryBatchData = new BatchData(operations, secondaryResults);
    // This is a operation that will be run by
    // `RequestScheduler#scheduleRequestWithCallback` after it acquires flow controller resources.
    // It will schedule asynchronous secondary operation and run primary operation in the main
    // thread, to make them run concurrently. We will wait for the secondary to finish later in
    // this
    // method.
    final Supplier<ListenableFuture<Void>> invokeBothOperations =
        new Supplier<ListenableFuture<Void>>() {
          @Override
          public ListenableFuture<Void> get() {
            // We are scheduling secondary batch to run concurrently.
            // Call to `.get()` starts the asynchronous operation, it doesn't wait for it to
            // finish.
            ListenableFuture<Void> secondaryOperationEnded =
                secondaryAsyncWrapper.batch(operations, secondaryResults).get();
            // Primary operation is then performed synchronously.
            try {
              primaryOperation.call();
            } catch (IOException | InterruptedException e) {
              primaryBatchData.setException(e);
            }
            // Primary operation has ended and its results are available to the user.

            // We want the schedule verification to after the secondary operation.
            return secondaryOperationEnded;
          }
        };

    // Concurrent writes are also synchronous, errors will be thrown to the user after both ops
    // finish.
    FutureCallback<Void> verification =
        new FutureCallback<Void>() {
          @Override
          public void onSuccess(@NullableDecl Void result) {}

          @Override
          public void onFailure(Throwable throwable) {
            secondaryBatchData.setException(throwable);
          }
        };

    // If flow controller errs and won't allow the request we will handle the error using this
    // handler.
    Function<Throwable, Void> flowControlReservationErrorConsumer =
        new Function<Throwable, Void>() {
          @NullableDecl
          @Override
          public Void apply(@NullableDecl Throwable throwable) {
            flowControllerException[0] = throwable;
            return null;
          }
        };

    ListenableFuture<Void> verificationCompleted =
        this.requestScheduler.scheduleRequestWithCallback(
            requestResourcesDescription,
            invokeBothOperations,
            verification,
            flowControlReservationErrorConsumer);

    try {
      // Wait until all asynchronous operations are completed.
      verificationCompleted.get();
    } catch (ExecutionException e) {
      throw new IllegalStateException("secondaryResult thrown unexpected exception.");
    }

    // Checks results of primary and secondary operations, we consider a operation failed if at
    // least one of the operations have failed. This method will fill `primaryResults` with errors
    // from both operations and will throw appropriate RetriesExhaustedWithDetailsException.
    reconcileBatchResultsConcurrent(
        primaryResults, primaryBatchData, secondaryBatchData, resultIsFaultyPredicate);

    if (flowControllerException[0] != null) {
      throw MirroringOperationException.markedAsBothException(
          new IOException("FlowController rejected the request", flowControllerException[0]),
          null,
          null);
    }
  }
}
