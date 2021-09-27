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
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
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
public class MirroringTable implements Table, ListenableCloseable {
  private static final Logger Log = new Logger(MirroringTable.class);
  private static final Predicate<Object> resultIsFaultyPredicate =
      new Predicate<Object>() {
        @Override
        public boolean apply(@NullableDecl Object o) {
          return o == null || o instanceof Throwable;
        }
      };

  Table primaryTable;
  Table secondaryTable;
  AsyncTableWrapper secondaryAsyncWrapper;
  VerificationContinuationFactory verificationContinuationFactory;
  private FlowController flowController;
  private ListenableReferenceCounter referenceCounter;
  private boolean closed = false;
  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;

  /**
   * @param executorService ExecutorService is used to perform operations on secondaryTable and
   *     verification tasks.
   * @param mismatchDetector Detects mismatches in results from operations preformed on both
   *     databases.
   */
  public MirroringTable(
      Table primaryTable,
      Table secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.secondaryAsyncWrapper =
        new AsyncTableWrapper(
            this.secondaryTable, MoreExecutors.listeningDecorator(executorService));
    this.flowController = flowController;
    this.referenceCounter = new ListenableReferenceCounter();
    this.referenceCounter.holdReferenceUntilClosing(this.secondaryAsyncWrapper);
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
  }

  private static Put makePutFromResult(Result result) {
    Put put = new Put(result.getRow());
    for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry :
        result.getMap().entrySet()) {
      byte[] family = familyEntry.getKey();
      for (Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
          familyEntry.getValue().entrySet()) {
        byte[] qualifier = qualifierEntry.getKey();
        for (Entry<Long, byte[]> valueEntry : qualifierEntry.getValue().entrySet()) {
          long timestamp = valueEntry.getKey();
          byte[] value = valueEntry.getValue();
          put.addColumn(family, qualifier, timestamp, value);
        }
      }
    }
    return put;
  }

  @Override
  public TableName getName() {
    return this.primaryTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    Log.trace("[%s] exists(get=%s)", this.getName(), get);
    boolean result = this.primaryTable.exists(get);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.exists(get),
        this.verificationContinuationFactory.exists(get, result));
    return result;
  }

  @Override
  public boolean[] existsAll(List<Get> list) throws IOException {
    Log.trace("[%s] existsAll(gets=%s)", this.getName(), list);
    boolean[] result = this.primaryTable.existsAll(list);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.existsAll(list),
        this.verificationContinuationFactory.existsAll(list, result));
    return result;
  }

  @Override
  public void batch(List<? extends Row> operations, Object[] results)
      throws IOException, InterruptedException {
    Log.trace("[%s] batch(operations=%s, results)", this.getName(), operations);
    try {
      this.primaryTable.batch(operations, results);
    } finally {
      scheduleSecondaryWriteBatchOperations(operations, results);
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
      List<? extends Row> operations, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    Log.trace(
        "[%s] batchCallback(operations=%s, results, callback=%s)",
        this.getName(), operations, callback);
    try {
      this.primaryTable.batchCallback(operations, results, callback);
    } finally {
      scheduleSecondaryWriteBatchOperations(operations, results);
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
  public Result get(Get get) throws IOException {
    Log.trace("[%s] get(get=%s)", this.getName(), get);
    Result result = this.primaryTable.get(get);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.get(get),
        this.verificationContinuationFactory.get(get, result));

    return result;
  }

  @Override
  public Result[] get(List<Get> list) throws IOException {
    Log.trace("[%s] get(gets=%s)", this.getName(), list);
    Result[] result = this.primaryTable.get(list);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.get(list),
        this.verificationContinuationFactory.get(list, result));

    return result;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    Log.trace("[%s] getScanner(scan=%s)", this.getName(), scan);
    MirroringResultScanner scanner =
        new MirroringResultScanner(
            scan,
            this.primaryTable.getScanner(scan),
            this.secondaryAsyncWrapper,
            this.verificationContinuationFactory,
            this.flowController);
    this.referenceCounter.holdReferenceUntilClosing(scanner);
    return scanner;
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getScanner(new Scan().addFamily(family));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  /**
   * `close()` won't perform the actual close if there are any in-flight requests, in such a case
   * the `close` operation is scheduled and will be performed after all requests have finished.
   */
  @Override
  public void close() throws IOException {
    this.asyncClose();
  }

  public synchronized ListenableFuture<Void> asyncClose() throws IOException {
    Log.trace("[%s] asyncClose()", this.getName());
    if (closed) {
      return this.referenceCounter.getOnLastReferenceClosed();
    }

    this.closed = true;
    this.referenceCounter.decrementReferenceCount();

    IOException primaryException = null;
    try {
      this.primaryTable.close();
    } catch (IOException e) {
      primaryException = e;
    }

    try {
      this.secondaryAsyncWrapper.asyncClose();
    } catch (RuntimeException e) {
      if (primaryException != null) {
        primaryException.addSuppressed(e);
        throw primaryException;
      } else {
        throw e;
      }
    }

    if (primaryException != null) {
      throw primaryException;
    }
    return this.referenceCounter.getOnLastReferenceClosed();
  }

  @Override
  public void put(Put put) throws IOException {
    Log.trace("[%s] put(put=%s)", this.getName(), put);
    this.primaryTable.put(put);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(put), this.secondaryAsyncWrapper.put(put), this.flowController);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    Log.trace("[%s] put(puts=%s)", this.getName(), puts);
    Object[] results = new Object[puts.size()];
    try {
      this.batch(puts, results);
    } catch (InterruptedException e) {
      IOException e2 = new InterruptedIOException();
      e2.initCause(e);
      throw e2;
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
    Log.trace(
        "[%s] checkAndPut(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, put=%s)",
        this.getName(), row, family, qualifier, compareOp, value, put);
    RowMutations mutations = new RowMutations(row);
    mutations.add(put);
    return this.checkAndMutate(row, family, qualifier, compareOp, value, mutations);
  }

  @Override
  public void delete(Delete delete) throws IOException {
    Log.trace("[%s] delete(delete=%s)", this.getName(), delete);
    this.primaryTable.delete(delete);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(delete),
        this.secondaryAsyncWrapper.delete(delete),
        this.flowController);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    Log.trace("[%s] delete(deletes=%s)", this.getName(), deletes);
    // Delete should remove successfully deleted rows from input list.
    Object[] results = new Object[deletes.size()];
    try {
      this.batch(deletes, results);
    } catch (InterruptedException e) {
      IOException e2 = new InterruptedIOException();
      e2.initCause(e);
      throw e2;
    } finally {
      final BatchHelpers.SplitBatchResponse<Delete> splitResponse =
          new BatchHelpers.SplitBatchResponse<>(deletes, results, resultIsFaultyPredicate);

      deletes.clear();
      deletes.addAll(splitResponse.failedWrites);
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
    Log.trace(
        "[%s] checkAndDelete(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, delete=%s)",
        this.getName(), row, family, qualifier, compareOp, value, delete);
    RowMutations mutations = new RowMutations(row);
    mutations.add(delete);
    return this.checkAndMutate(row, family, qualifier, compareOp, value, mutations);
  }

  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    Log.trace("[%s] mutateRow(rowMutations=%s)", this.getName(), rowMutations);
    this.primaryTable.mutateRow(rowMutations);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(rowMutations),
        this.secondaryAsyncWrapper.mutateRow(rowMutations),
        this.flowController);
  }

  @Override
  public Result append(Append append) throws IOException {
    Log.trace("[%s] append(append=%s)", this.getName(), append);
    Result result = this.primaryTable.append(append);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(append),
        this.secondaryAsyncWrapper.put(makePutFromResult(result)),
        this.flowController);
    return result;
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    Log.trace("[%s] increment(increment=%s)", this.getName(), increment);
    Result result = this.primaryTable.increment(increment);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(increment),
        this.secondaryAsyncWrapper.put(makePutFromResult(result)),
        this.flowController);
    return result;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    Log.trace(
        "[%s] incrementColumnValue(row=%s, family=%s, qualifier=%s, amount=%s)",
        this.getName(), row, family, qualifier, amount);
    Result result = increment((new Increment(row)).addColumn(family, qualifier, amount));
    Cell cell = result.getColumnLatestCell(family, qualifier);
    assert cell != null;
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
    assert cell != null;
    return Bytes.toLong(CellUtil.cloneValue(cell));
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
  public boolean checkAndMutate(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOp compareOp,
      byte[] value,
      RowMutations rowMutations)
      throws IOException {
    Log.trace(
        "[%s] checkAndMutate(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, rowMutations=%s)",
        this.getName(), row, family, qualifier, compareOp, value, rowMutations);
    boolean wereMutationsApplied =
        this.primaryTable.checkAndMutate(row, family, qualifier, compareOp, value, rowMutations);
    if (wereMutationsApplied) {
      scheduleWriteWithControlFlow(
          new WriteOperationInfo(rowMutations),
          this.secondaryAsyncWrapper.mutateRow(rowMutations),
          this.flowController);
    }
    return wereMutationsApplied;
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

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.referenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }

  private <T> void scheduleVerificationAndRequestWithFlowControl(
      final RequestResourcesDescription resultInfo,
      final Supplier<ListenableFuture<T>> secondaryGetFutureSupplier,
      final FutureCallback<T> verificationCallback) {
    this.referenceCounter.holdReferenceUntilCompletion(
        RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
            resultInfo, secondaryGetFutureSupplier, verificationCallback, this.flowController));
  }

  private <T> void scheduleWriteWithControlFlow(
      final WriteOperationInfo writeOperationInfo,
      final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
      final FlowController flowController) {
    this.referenceCounter.holdReferenceUntilCompletion(
        RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
            writeOperationInfo.requestResourcesDescription,
            secondaryResultFutureSupplier,
            new FutureCallback<T>() {
              @Override
              public void onSuccess(@NullableDecl T t) {}

              @Override
              public void onFailure(Throwable throwable) {
                secondaryWriteErrorConsumer.consume(writeOperationInfo.operations);
              }
            },
            flowController,
            new Runnable() {
              @Override
              public void run() {
                secondaryWriteErrorConsumer.consume(writeOperationInfo.operations);
              }
            }));
  }

  private void scheduleSecondaryWriteBatchOperations(
      final List<? extends Row> operations, final Object[] results) {

    final BatchHelpers.SplitBatchResponse<?> primarySplitResponse =
        new BatchHelpers.SplitBatchResponse<>(operations, results, resultIsFaultyPredicate);

    if (primarySplitResponse.allSuccessfulOperations.size() == 0) {
      return;
    }

    final Object[] resultsSecondary =
        new Object[primarySplitResponse.allSuccessfulOperations.size()];

    FutureCallback<Void> verificationFuture =
        BatchHelpers.createBatchVerificationCallback(
            primarySplitResponse,
            resultsSecondary,
            verificationContinuationFactory.getMismatchDetector(),
            this.secondaryWriteErrorConsumer,
            resultIsFaultyPredicate);

    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        new MirroringTable.WriteOperationInfo(primarySplitResponse).requestResourcesDescription,
        this.secondaryAsyncWrapper.batch(
            primarySplitResponse.allSuccessfulOperations, resultsSecondary),
        verificationFuture,
        this.flowController,
        new Runnable() {
          @Override
          public void run() {
            secondaryWriteErrorConsumer.consume(primarySplitResponse.successfulWrites);
          }
        });
  }

  public static class WriteOperationInfo {
    public final RequestResourcesDescription requestResourcesDescription;
    public final List<? extends Row> operations;

    public WriteOperationInfo(BatchHelpers.SplitBatchResponse<? extends Row> primarySplitResponse) {
      this.operations = primarySplitResponse.allSuccessfulOperations;
      this.requestResourcesDescription =
          new RequestResourcesDescription(
              this.operations, primarySplitResponse.successfulReadsResults);
    }

    public WriteOperationInfo(Put operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(Delete operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(Append operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(Increment operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(RowMutations operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    private WriteOperationInfo(
        RequestResourcesDescription requestResourcesDescription, Row operation) {
      this.requestResourcesDescription = requestResourcesDescription;
      this.operations = Collections.singletonList(operation);
    }
  }
}
