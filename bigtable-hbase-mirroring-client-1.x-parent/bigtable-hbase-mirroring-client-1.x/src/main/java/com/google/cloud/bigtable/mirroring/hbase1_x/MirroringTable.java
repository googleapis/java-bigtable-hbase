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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.OperationUtils.emptyResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounterUtils.holdReferenceUntilCompletion;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.FailedSuccessfulSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Batcher;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.OperationUtils;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.WriteOperationInfo;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.HierarchicalReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Service;
import org.apache.hadoop.hbase.util.Bytes;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Table which mirrors every two mutations to two underlying tables.
 *
 * <p>Objects of this class present themselves as HBase 1.x `Table` objects. Every operation is
 * first performed on primary table and if it succeeded it is replayed on the secondary table
 * asynchronously. Read operations are mirrored to verify that content of both databases matches.
 */
@InternalApi("For internal usage only")
public class MirroringTable implements Table {

  private static final Logger Log = new Logger(MirroringTable.class);
  private static final Predicate<Object> resultIsFaultyPredicate =
      new Predicate<Object>() {
        @Override
        public boolean apply(@NullableDecl Object o) {
          return o == null || o instanceof Throwable;
        }
      };
  protected final Table primaryTable;
  private final AsyncTableWrapper secondaryAsyncWrapper;
  private final VerificationContinuationFactory verificationContinuationFactory;
  /** Counter for MirroringConnection and MirroringTable. */
  private final HierarchicalReferenceCounter referenceCounter;

  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;
  private final MirroringTracer mirroringTracer;
  private final ReadSampler readSampler;
  private final RequestScheduler requestScheduler;
  private final Batcher batcher;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final SettableFuture<Void> closedFuture = SettableFuture.create();
  private final int resultScannerBufferedMismatchedResults;
  private final Timestamper timestamper;
  /**
   * @param executorService ExecutorService is used to perform operations on secondaryTable and
   *     verification tasks.
   * @param mismatchDetector Detects mismatches in results from operations preformed on both
   *     databases.
   * @param secondaryWriteErrorConsumer Consumer secondary write errors.
   */
  public MirroringTable(
      Table primaryTable,
      Table secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      ReadSampler readSampler,
      Timestamper timestamper,
      boolean performWritesConcurrently,
      boolean waitForSecondaryWrites,
      MirroringTracer mirroringTracer,
      ReferenceCounter parentReferenceCounter,
      int resultScannerBufferedMismatchedResults) {
    this.primaryTable = primaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.readSampler = readSampler;
    this.secondaryAsyncWrapper =
        new AsyncTableWrapper(
            secondaryTable, MoreExecutors.listeningDecorator(executorService), mirroringTracer);
    this.referenceCounter = new HierarchicalReferenceCounter(parentReferenceCounter);
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    Preconditions.checkArgument(
        !(performWritesConcurrently && !waitForSecondaryWrites),
        "If concurrent writes are enabled, then waiting for secondary writes should also be enabled.");
    this.mirroringTracer = mirroringTracer;
    this.requestScheduler =
        new RequestScheduler(flowController, this.mirroringTracer, this.referenceCounter);
    this.timestamper = timestamper;
    this.batcher =
        new Batcher(
            this.primaryTable,
            this.secondaryAsyncWrapper,
            this.requestScheduler,
            this.secondaryWriteErrorConsumer,
            this.verificationContinuationFactory,
            this.readSampler,
            this.timestamper,
            resultIsFaultyPredicate,
            waitForSecondaryWrites,
            performWritesConcurrently,
            this.mirroringTracer);
    this.resultScannerBufferedMismatchedResults = resultScannerBufferedMismatchedResults;
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.EXISTS)) {
      Log.trace("[%s] exists(get=%s)", this.getName(), get);

      boolean result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Boolean>() {
                @Override
                public Boolean call() throws IOException {
                  return primaryTable.exists(get);
                }
              },
              HBaseOperation.EXISTS);

      scheduleSequentialReadOperationWithVerification(
          new RequestResourcesDescription(result),
          this.secondaryAsyncWrapper.exists(get),
          this.verificationContinuationFactory.exists(get, result));
      return result;
    }
  }

  @Override
  public boolean[] existsAll(final List<Get> inputList) throws IOException {
    final List<Get> list = new ArrayList<>(inputList);
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.EXISTS_ALL)) {
      Log.trace("[%s] existsAll(gets=%s)", this.getName(), list);

      boolean[] result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<boolean[]>() {
                @Override
                public boolean[] call() throws IOException {
                  return primaryTable.existsAll(list);
                }
              },
              HBaseOperation.EXISTS_ALL);

      scheduleSequentialReadOperationWithVerification(
          new RequestResourcesDescription(result),
          this.secondaryAsyncWrapper.existsAll(list),
          this.verificationContinuationFactory.existsAll(list, result));
      return result;
    }
  }

  @Override
  public Result get(final Get get) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.GET)) {
      Log.trace("[%s] get(get=%s)", this.getName(), get);

      Result result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result>() {
                @Override
                public Result call() throws IOException {
                  return primaryTable.get(get);
                }
              },
              HBaseOperation.GET);

      scheduleSequentialReadOperationWithVerification(
          new RequestResourcesDescription(result),
          this.secondaryAsyncWrapper.get(get),
          this.verificationContinuationFactory.get(get, result));
      return result;
    }
  }

  @Override
  public Result[] get(final List<Get> inputList) throws IOException {
    final List<Get> list = new ArrayList<>(inputList);
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.GET_LIST)) {
      Log.trace("[%s] get(gets=%s)", this.getName(), list);

      Result[] result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result[]>() {
                @Override
                public Result[] call() throws IOException {
                  return primaryTable.get(list);
                }
              },
              HBaseOperation.GET_LIST);

      scheduleSequentialReadOperationWithVerification(
          new RequestResourcesDescription(result),
          this.secondaryAsyncWrapper.get(list),
          this.verificationContinuationFactory.get(list, result));
      return result;
    }
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.GET_SCANNER)) {
      Log.trace("[%s] getScanner(scan=%s)", this.getName(), scan);
      MirroringResultScanner scanner =
          new MirroringResultScanner(
              scan,
              this.primaryTable.getScanner(scan),
              this.secondaryAsyncWrapper.getScanner(scan),
              this.verificationContinuationFactory,
              this.mirroringTracer,
              this.readSampler.shouldNextReadOperationBeSampled(),
              this.requestScheduler,
              this.referenceCounter,
              this.resultScannerBufferedMismatchedResults);
      return scanner;
    }
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getScanner(new Scan().addFamily(family));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  @Override
  public void put(final Put put) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.PUT)) {
      Log.trace("[%s] put(put=%s)", this.getName(), put);
      this.batcher.batchSingleWriteOperation(put);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.PUT_LIST)) {
      Log.trace("[%s] put(puts=%s)", this.getName(), puts);
      try {
        Object[] results = new Object[puts.size()];
        this.batcher.batch(puts, results);
      } catch (InterruptedException e) {
        IOException e2 = new InterruptedIOException();
        e2.initCause(e);
        throw e2;
      }
    }
  }

  @Override
  public void delete(final Delete delete) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.DELETE)) {
      Log.trace("[%s] delete(delete=%s)", this.getName(), delete);
      this.batcher.batchSingleWriteOperation(delete);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.DELETE_LIST)) {
      Log.trace("[%s] delete(deletes=%s)", this.getName(), deletes);
      Object[] results = new Object[deletes.size()];
      try {
        this.batcher.batch(deletes, results);
      } catch (InterruptedException e) {
        IOException e2 = new InterruptedIOException();
        e2.initCause(e);
        throw e2;
      } finally {
        final FailedSuccessfulSplit<Delete, Object> failedSuccessfulSplit =
            new FailedSuccessfulSplit<>(deletes, results, resultIsFaultyPredicate, Object.class);

        // Delete should remove successful operations from input list.
        // To conform to this requirement we are clearing the list and re-adding failed deletes.
        deletes.clear();
        deletes.addAll(failedSuccessfulSplit.failedOperations);
      }
    }
  }

  @Override
  public void mutateRow(final RowMutations rowMutations) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.MUTATE_ROW)) {
      Log.trace("[%s] mutateRow(rowMutations=%s)", this.getName(), rowMutations);
      this.batcher.batchSingleWriteOperation(rowMutations);
    }
  }

  @Override
  public Result append(final Append append) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.APPEND)) {
      Log.trace("[%s] append(append=%s)", this.getName(), append);
      boolean wantsResults = append.isReturnResults();
      append.setReturnResults(true);

      Result result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result>() {
                @Override
                public Result call() throws IOException {
                  return primaryTable.append(append);
                }
              },
              HBaseOperation.APPEND);

      Put put = OperationUtils.makePutFromResult(result);

      scheduleSequentialWriteOperation(
          new WriteOperationInfo(put), this.secondaryAsyncWrapper.put(put));

      // HBase's append() returns null when isReturnResults is false.
      return wantsResults ? result : null;
    }
  }

  @Override
  public Result increment(final Increment increment) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.INCREMENT)) {
      Log.trace("[%s] increment(increment=%s)", this.getName(), increment);
      boolean wantsResults = increment.isReturnResults();
      increment.setReturnResults(true);

      Result result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result>() {
                @Override
                public Result call() throws IOException {
                  return primaryTable.increment(increment);
                }
              },
              HBaseOperation.INCREMENT);

      Put put = OperationUtils.makePutFromResult(result);

      scheduleSequentialWriteOperation(
          new WriteOperationInfo(put), this.secondaryAsyncWrapper.put(put));
      return wantsResults ? result : emptyResult();
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    Log.trace(
        "[%s] incrementColumnValue(row=%s, family=%s, qualifier=%s, amount=%s)",
        this.getName(), row, family, qualifier, amount);
    Result result = increment((new Increment(row)).addColumn(family, qualifier, amount));
    Cell cell = result.getColumnLatestCell(family, qualifier);
    Preconditions.checkNotNull(cell);
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  @Override
  public long incrementColumnValue(
      byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
      throws IOException {
    Log.trace(
        "[%s] incrementColumnValue(row=%s, family=%s, qualifier=%s, amount=%s, durability=%s)",
        this.getName(), row, family, qualifier, amount, durability);
    Result result =
        increment(
            (new Increment(row)).addColumn(family, qualifier, amount).setDurability(durability));
    Cell cell = result.getColumnLatestCell(family, qualifier);
    Preconditions.checkNotNull(cell);
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  @Override
  public void batch(List<? extends Row> operations, Object[] results)
      throws IOException, InterruptedException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BATCH)) {
      this.batcher.batch(operations, results);
    }
  }

  @Override
  public Object[] batch(List<? extends Row> operations) throws IOException, InterruptedException {
    Log.trace("[%s] batch(operations=%s)", this.getName(), operations);
    Object[] results = new Object[operations.size()];
    this.batch(operations, results);
    return results;
  }

  @Override
  public <R> void batchCallback(
      List<? extends Row> inputOperations, Object[] results, final Callback<R> callback)
      throws IOException, InterruptedException {
    final List<? extends Row> operations = new ArrayList<>(inputOperations);
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BATCH_CALLBACK)) {
      Log.trace(
          "[%s] batchCallback(operations=%s, results, callback=%s)",
          this.getName(), operations, callback);

      this.batcher.batch(operations, results, callback);
    }
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> operations, Callback<R> callback)
      throws IOException, InterruptedException {
    Log.trace(
        "[%s] batchCallback(operations=%s, callback=%s)", this.getName(), operations, callback);
    Object[] results = new Object[operations.size()];
    this.batchCallback(operations, results, callback);
    return results;
  }

  @Override
  public boolean checkAndMutate(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOp compareOp,
      byte[] value,
      RowMutations rowMutations)
      throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.CHECK_AND_MUTATE)) {
      Log.trace(
          "[%s] checkAndMutate(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, rowMutations=%s)",
          this.getName(), row, family, qualifier, compareOp, value, rowMutations);

      return checkAndMutateWithSpan(row, family, qualifier, compareOp, value, rowMutations);
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    Log.trace(
        "[%s] checkAndPut(row=%s, family=%s, qualifier=%s, value=%s, put=%s)",
        this.getName(), row, family, qualifier, value, put);
    return this.checkAndPut(row, family, qualifier, CompareOp.EQUAL, value, put);
  }

  @Override
  public boolean checkAndPut(
      byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Put put)
      throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.CHECK_AND_PUT)) {
      Log.trace(
          "[%s] checkAndPut(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, put=%s)",
          this.getName(), row, family, qualifier, compareOp, value, put);
      RowMutations mutations = new RowMutations(row);
      mutations.add(put);
      return this.checkAndMutateWithSpan(row, family, qualifier, compareOp, value, mutations);
    }
  }

  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    Log.trace(
        "[%s] checkAndDelete(row=%s, family=%s, qualifier=%s, value=%s, delete=%s)",
        this.getName(), row, family, qualifier, value, delete);
    return this.checkAndDelete(row, family, qualifier, CompareOp.EQUAL, value, delete);
  }

  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Delete delete)
      throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.CHECK_AND_DELETE)) {
      Log.trace(
          "[%s] checkAndDelete(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, delete=%s)",
          this.getName(), row, family, qualifier, compareOp, value, delete);
      RowMutations mutations = new RowMutations(row);
      mutations.add(delete);
      return this.checkAndMutateWithSpan(row, family, qualifier, compareOp, value, mutations);
    }
  }

  private boolean checkAndMutateWithSpan(
      final byte[] row,
      final byte[] family,
      final byte[] qualifier,
      final CompareOp compareOp,
      final byte[] value,
      final RowMutations rowMutations)
      throws IOException {
    this.timestamper.fillTimestamp(rowMutations);
    boolean wereMutationsApplied =
        this.mirroringTracer.spanFactory.wrapPrimaryOperation(
            new CallableThrowingIOException<Boolean>() {
              @Override
              public Boolean call() throws IOException {
                return primaryTable.checkAndMutate(
                    row, family, qualifier, compareOp, value, rowMutations);
              }
            },
            HBaseOperation.CHECK_AND_MUTATE);

    if (wereMutationsApplied) {
      scheduleSequentialWriteOperation(
          new WriteOperationInfo(rowMutations), this.secondaryAsyncWrapper.mutateRow(rowMutations));
    }
    return wereMutationsApplied;
  }

  /**
   * Synchronously {@link Table#close()}s primary table and schedules closing of the secondary table
   * after finishing all secondary requests that are yet in-flight ({@link
   * AsyncTableWrapper#close()}).
   */
  @Override
  public void close() throws IOException {
    this.closePrimaryAndScheduleSecondaryClose();
  }

  private void closePrimaryAndScheduleSecondaryClose() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.TABLE_CLOSE)) {
      if (this.closed.getAndSet(true)) {
        return;
      }

      // We are freeing the initial reference to current level reference counter.
      this.referenceCounter.current.decrementReferenceCount();
      // But we are scheduling asynchronous secondary operation and we should increment our parent's
      // ref counter until this operation is finished.
      holdReferenceUntilCompletion(this.referenceCounter.parent, this.closedFuture);

      AccumulatedExceptions exceptionsList = new AccumulatedExceptions();
      try {
        this.mirroringTracer.spanFactory.wrapPrimaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                primaryTable.close();
                return null;
              }
            },
            HBaseOperation.TABLE_CLOSE);
      } catch (IOException e) {
        exceptionsList.add(e);
      }

      try {
        // Close secondary wrapper (what will close secondary table) after all scheduled requests
        // have finished.
        this.referenceCounter
            .current
            .getOnLastReferenceClosed()
            .addListener(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      secondaryAsyncWrapper.close();
                      closedFuture.set(null);
                    } catch (IOException e) {
                      closedFuture.setException(e);
                    }
                  }
                },
                MoreExecutors.directExecutor());
      } catch (RuntimeException e) {
        exceptionsList.add(e);
      }

      exceptionsList.rethrowIfCaptured();
    } finally {
      this.mirroringTracer.spanFactory.asyncCloseSpanWhenCompleted(
          this.referenceCounter.current.getOnLastReferenceClosed());
    }
  }

  private <T> void scheduleSequentialReadOperationWithVerification(
      final RequestResourcesDescription resourcesDescription,
      final Supplier<ListenableFuture<T>> secondaryOperationSupplier,
      final FutureCallback<T> verificationCallback) {
    if (!this.readSampler.shouldNextReadOperationBeSampled()) {
      return;
    }
    this.requestScheduler.scheduleRequestWithCallback(
        resourcesDescription,
        secondaryOperationSupplier,
        this.mirroringTracer.spanFactory.wrapReadVerificationCallback(verificationCallback));
  }

  private <T> void scheduleSequentialWriteOperation(
      final WriteOperationInfo writeOperationInfo,
      final Supplier<ListenableFuture<T>> secondaryOperationSupplier) {
    WriteOperationFutureCallback<T> writeErrorCallback =
        new WriteOperationFutureCallback<T>() {
          @Override
          public void onFailure(Throwable throwable) {
            secondaryWriteErrorConsumer.consume(
                writeOperationInfo.hBaseOperation, writeOperationInfo.operations, throwable);
          }
        };

    // If flow controller errs and won't allow the request we will handle the error using this
    // handler.
    Function<Throwable, Void> flowControlReservationErrorConsumer =
        new Function<Throwable, Void>() {
          @Override
          public Void apply(Throwable throwable) {
            secondaryWriteErrorConsumer.consume(
                writeOperationInfo.hBaseOperation, writeOperationInfo.operations, throwable);
            return null;
          }
        };

    this.requestScheduler.scheduleRequestWithCallback(
        writeOperationInfo.requestResourcesDescription,
        secondaryOperationSupplier,
        this.mirroringTracer.spanFactory.wrapWriteOperationCallback(
            writeOperationInfo.hBaseOperation, this.mirroringTracer, writeErrorCallback),
        flowControlReservationErrorConsumer);
  }

  @Override
  public TableName getName() {
    return this.primaryTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return this.primaryTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return this.primaryTable.getTableDescriptor();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call) throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> void coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call, Callback<R> callback)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWriteBufferSize(long l) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes1,
      R r,
      Callback<R> callback)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOperationTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getReadRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReadRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getWriteRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWriteRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  /**
   * Helper class that holds common parameters to {@link
   * RequestScheduling#scheduleRequestWithCallback(RequestResourcesDescription, Supplier,
   * FutureCallback, FlowController, MirroringTracer, Function)} for single instance of {@link
   * com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable}.
   *
   * <p>It also takes care of reference counting all scheduled operations.
   */
  public static class RequestScheduler {
    final FlowController flowController;
    final MirroringTracer mirroringTracer;
    final ReferenceCounter referenceCounter;

    public RequestScheduler(
        FlowController flowController,
        MirroringTracer mirroringTracer,
        ReferenceCounter referenceCounter) {
      this.flowController = flowController;
      this.mirroringTracer = mirroringTracer;
      this.referenceCounter = referenceCounter;
    }

    public RequestScheduler withReferenceCounter(ReferenceCounter referenceCounter) {
      return new RequestScheduler(this.flowController, this.mirroringTracer, referenceCounter);
    }

    public <T> ListenableFuture<Void> scheduleRequestWithCallback(
        final RequestResourcesDescription requestResourcesDescription,
        final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
        final FutureCallback<T> verificationCallback) {
      return this.scheduleRequestWithCallback(
          requestResourcesDescription,
          secondaryResultFutureSupplier,
          verificationCallback,
          // noop flowControlReservationErrorConsumer
          new Function<Throwable, Void>() {
            @Override
            public Void apply(Throwable t) {
              return null;
            }
          });
    }

    public <T> ListenableFuture<Void> scheduleRequestWithCallback(
        final RequestResourcesDescription requestResourcesDescription,
        final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
        final FutureCallback<T> verificationCallback,
        final Function<Throwable, Void> flowControlReservationErrorConsumer) {
      ListenableFuture<Void> future =
          RequestScheduling.scheduleRequestWithCallback(
              requestResourcesDescription,
              secondaryResultFutureSupplier,
              verificationCallback,
              this.flowController,
              this.mirroringTracer,
              flowControlReservationErrorConsumer);
      holdReferenceUntilCompletion(this.referenceCounter, future);
      return future;
    }
  }
}
