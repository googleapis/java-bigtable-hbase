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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  Table primaryTable;
  Table secondaryTable;
  AsyncTableWrapper secondaryAsyncWrapper;
  VerificationContinuationFactory verificationContinuationFactory;
  private List<Runnable> onCloseListeners = new ArrayList<>();
  private ListenableFuture<Void> closeFuture;
  private FlowController flowController;

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
      FlowController flowController) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.secondaryAsyncWrapper =
        new AsyncTableWrapper(
            this.secondaryTable, MoreExecutors.listeningDecorator(executorService));
    this.flowController = flowController;
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
    boolean result = this.primaryTable.exists(get);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.exists(get),
        this.verificationContinuationFactory.exists(get, result));
    return result;
  }

  @Override
  public boolean[] existsAll(List<Get> list) throws IOException {
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
    try {
      this.primaryTable.batch(operations, results);
    } finally {
      BatchHelpers.scheduleSecondaryWriteBatchOperations(operations, results, this);
    }
  }

  @Override
  public Object[] batch(List<? extends Row> operations) throws IOException, InterruptedException {
    Object[] results = new Object[operations.size()];
    this.batch(operations, results);
    return results;
  }

  @Override
  public <R> void batchCallback(
      List<? extends Row> operations, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    try {
      this.primaryTable.batchCallback(operations, results, callback);
    } finally {
      BatchHelpers.scheduleSecondaryWriteBatchOperations(operations, results, this);
    }
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> operations, Callback<R> callback)
      throws IOException, InterruptedException {
    Object[] results = new Object[operations.size()];
    this.batchCallback(operations, results, callback);
    return results;
  }

  @Override
  public Result get(Get get) throws IOException {
    Result result = this.primaryTable.get(get);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.get(get),
        this.verificationContinuationFactory.get(get, result));

    return result;
  }

  @Override
  public Result[] get(List<Get> list) throws IOException {
    Result[] result = this.primaryTable.get(list);
    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.get(list),
        this.verificationContinuationFactory.get(list, result));

    return result;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new MirroringResultScanner(
        scan,
        this.primaryTable.getScanner(scan),
        this.secondaryAsyncWrapper,
        this.verificationContinuationFactory,
        this.flowController);
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
    if (this.closeFuture != null) {
      return this.closeFuture;
    }

    IOException primaryException = null;
    try {
      this.primaryTable.close();
    } catch (IOException e) {
      primaryException = e;
    }

    try {
      this.closeFuture = this.secondaryAsyncWrapper.asyncClose();
      this.closeFuture.addListener(
          new Runnable() {
            @Override
            public void run() {
              MirroringTable.this.runOnCloseListeners();
            }
          },
          MoreExecutors.directExecutor());
    } catch (RuntimeException e) {
      // If scheduling close failed, run listeners now, and behave as if we have closed for
      // correct reference counting.
      this.runOnCloseListeners();

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
    return closeFuture;
  }

  @Override
  public void put(Put put) throws IOException {
    this.primaryTable.put(put);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(put), this.secondaryAsyncWrapper.put(put), this.flowController);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
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
    return this.checkAndPut(row, family, qualifier, CompareOp.EQUAL, value, put);
  }

  @Override
  public boolean checkAndPut(
      byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Put put)
      throws IOException {
    RowMutations mutations = new RowMutations(row);
    mutations.add(put);
    return this.checkAndMutate(row, family, qualifier, compareOp, value, mutations);
  }

  @Override
  public void delete(Delete delete) throws IOException {
    this.primaryTable.delete(delete);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(delete),
        this.secondaryAsyncWrapper.delete(delete),
        this.flowController);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    // Delete should remove successfully deleted rows from input list.
    Object[] results = new Object[deletes.size()];
    try {
      this.batch(deletes, results);
      deletes.clear();
    } catch (InterruptedException e) {
      final BatchHelpers.SplitBatchResponse<Delete> splitResponse =
          new BatchHelpers.SplitBatchResponse<>(deletes, results);

      deletes.clear();
      deletes.addAll(splitResponse.failedWrites);

      IOException e2 = new InterruptedIOException();
      e2.initCause(e);
      throw e2;
    }
  }

  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    return this.checkAndDelete(row, family, qualifier, CompareOp.EQUAL, value, delete);
  }

  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Delete delete)
      throws IOException {
    RowMutations mutations = new RowMutations(row);
    mutations.add(delete);
    return this.checkAndMutate(row, family, qualifier, compareOp, value, mutations);
  }

  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    this.primaryTable.mutateRow(rowMutations);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(rowMutations),
        this.secondaryAsyncWrapper.mutateRow(rowMutations),
        this.flowController);
  }

  @Override
  public Result append(Append append) throws IOException {
    Result result = this.primaryTable.append(append);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(append),
        this.secondaryAsyncWrapper.append(append),
        this.flowController);
    return result;
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    Result result = this.primaryTable.increment(increment);
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(increment),
        this.secondaryAsyncWrapper.increment(increment),
        this.flowController);
    return result;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    Result result = increment((new Increment(row)).addColumn(family, qualifier, amount));
    Cell cell = result.getColumnLatestCell(family, qualifier);
    assert cell != null;
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  @Override
  public long incrementColumnValue(
      byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
      throws IOException {
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
    this.onCloseListeners.add(listener);
  }

  public void runOnCloseListeners() {
    for (Runnable listener : this.onCloseListeners) {
      listener.run();
    }
  }

  private <T> void scheduleVerificationAndRequestWithFlowControl(
      final RequestResourcesDescription resultInfo,
      final ListenableFuture<T> secondaryGetFuture,
      final FutureCallback<T> verificationCallback) {
    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        resultInfo, secondaryGetFuture, verificationCallback, this.flowController);
  }

  public <T> void scheduleWriteWithControlFlow(
      final WriteOperationInfo writeOperationInfo,
      final ListenableFuture<T> secondaryResultFuture,
      final FlowController flowController) {
    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        writeOperationInfo.requestResourcesDescription,
        secondaryResultFuture,
        new FutureCallback<T>() {
          @Override
          public void onSuccess(@NullableDecl T t) {}

          @Override
          public void onFailure(Throwable throwable) {
            handleFailedOperations(writeOperationInfo.operations);
          }
        },
        flowController);
  }

  public static class WriteOperationInfo {
    final RequestResourcesDescription requestResourcesDescription;
    final List<? extends Row> operations;

    public WriteOperationInfo(List<? extends Row> operations) {
      this.requestResourcesDescription = new RequestResourcesDescription(operations);
      this.operations = operations;
    }

    public WriteOperationInfo(Row operation) {
      this.requestResourcesDescription = new RequestResourcesDescription(operation);
      this.operations = Collections.singletonList(operation);
    }
  }

  public void handleFailedOperations(List<? extends Row> operations) {
    // TODO(mwalkiewicz): call write error handler.
  }

  static class BatchHelpers {
    private static void scheduleSecondaryWriteBatchOperations(
        final List<? extends Row> operations, final Object[] results, MirroringTable table) {

      final SplitBatchResponse<?> primarySplitResponse =
          new SplitBatchResponse<>(operations, results);

      if (primarySplitResponse.allSuccessfulOperations.size() == 0) {
        return;
      }

      final Object[] resultsSecondary =
          new Object[primarySplitResponse.allSuccessfulOperations.size()];

      FutureCallback<Void> verificationFuture =
          createBatchVerificationCallback(
              primarySplitResponse,
              resultsSecondary,
              table.verificationContinuationFactory.getMismatchDetector(),
              table);

      RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
          new WriteOperationInfo(primarySplitResponse.allSuccessfulOperations)
              .requestResourcesDescription,
          table.secondaryAsyncWrapper.batch(
              primarySplitResponse.allSuccessfulOperations, resultsSecondary),
          verificationFuture,
          table.flowController);
    }

    private static FutureCallback<Void> createBatchVerificationCallback(
        final SplitBatchResponse<?> primarySplitResponse,
        final Object[] secondaryResults,
        final MismatchDetector mismatchDetector,
        final MirroringTable table) {
      return new FutureCallback<Void>() {
        @Override
        public void onSuccess(@NullableDecl Void t) {
          // Batch is successful - all results are correct.
          List<? extends Row> secondaryOperations = primarySplitResponse.allSuccessfulOperations;

          final SplitBatchResponse<?> secondarySplitResponse =
              new SplitBatchResponse<>(secondaryOperations, secondaryResults);

          if (secondarySplitResponse.successfulReads.size() > 0) {
            mismatchDetector.batch(
                secondarySplitResponse.successfulReads,
                primarySplitResponse.successfulReadsResults,
                secondarySplitResponse.successfulReadsResults);
          }
        }

        @Override
        public void onFailure(Throwable throwable) {
          // Batch has thrown - partial results might be available.
          List<? extends Row> secondaryOperations = primarySplitResponse.allSuccessfulOperations;

          final SplitBatchResponse<?> secondarySplitResponse =
              new SplitBatchResponse<>(secondaryOperations, secondaryResults);

          if (secondarySplitResponse.failedWrites.size() > 0) {
            table.handleFailedOperations(secondarySplitResponse.failedWrites);
          }

          if (secondarySplitResponse.allReads.size() > 0) {
            // Some of the reads in this batch might have been not successful.
            // We want to verify successful reads and report the others.

            // We are using `secondaryResults` to select indices of operations that were successful.
            // Using those indices we select Get operations that have results from both primary and
            // secondary database, and pass them to `mismatchDetector.batch()`.
            // We also gather failed gets to pass them to `batchGetFailure`.
            MatchingSuccessfulReadsResults matchingSuccessfulReads =
                selectMatchingSuccessfulReads(
                    secondaryOperations,
                    primarySplitResponse.allSuccessfulResults,
                    secondaryResults);

            mismatchDetector.batch(
                secondarySplitResponse.successfulReads,
                matchingSuccessfulReads.primaryResults,
                matchingSuccessfulReads.secondaryResults);

            if (!matchingSuccessfulReads.failedReads.isEmpty()) {
              mismatchDetector.batch(matchingSuccessfulReads.failedReads, throwable);
            }
          }
        }
      };
    }

    /**
     * Helper class that facilitates analysing results of partially completed batch operation
     * containing {@link Get}s. Contains matching results from first and secondary databases, Get
     * operations that produced those results, and Gets that failed on secondary.
     */
    private static class MatchingSuccessfulReadsResults {
      final Result[] primaryResults;
      final Result[] secondaryResults;
      final List<Get> failedReads;
      final List<Get> successfulReads;

      private MatchingSuccessfulReadsResults(
          Result[] primaryResults,
          Result[] secondaryResults,
          List<Get> failedReads,
          List<Get> successfulReads) {
        this.primaryResults = primaryResults;
        this.secondaryResults = secondaryResults;
        this.failedReads = failedReads;
        this.successfulReads = successfulReads;
      }
    }

    /**
     * Creates a {@link MatchingSuccessfulReadsResults} based on arrays of results from primary and
     * secondary databases and list of performed operations. All inputs are iterated simultaneously,
     * Get operations are identified using isinstance and f their results from both databases are
     * available, they are added to lists of matching reads and successful operations. In the other
     * case the Get operation is placed on failed operations list.
     */
    private static MatchingSuccessfulReadsResults selectMatchingSuccessfulReads(
        List<? extends Row> operations, Object[] primaryResults, Object[] secondaryResults) {
      assert operations.size() == secondaryResults.length;
      assert primaryResults.length == secondaryResults.length;

      List<Result> primaryMatchingReads = new ArrayList<>();
      List<Result> secondaryMatchingReads = new ArrayList<>();

      List<Get> failedReads = new ArrayList<>();
      List<Get> successfulReads = new ArrayList<>();

      for (int i = 0; i < secondaryResults.length; i++) {
        if (!(operations.get(i) instanceof Get)) {
          continue;
        }

        // We are sure casts are correct, and non-null results to Gets are always Results.
        if (secondaryResults[i] == null) {
          failedReads.add((Get) operations.get(i));
        } else {
          primaryMatchingReads.add((Result) primaryResults[i]);
          secondaryMatchingReads.add((Result) secondaryResults[i]);
          successfulReads.add((Get) operations.get(i));
        }
      }

      return new MatchingSuccessfulReadsResults(
          primaryMatchingReads.toArray(new Result[0]),
          secondaryMatchingReads.toArray(new Result[0]),
          failedReads,
          successfulReads);
    }

    /**
     * Helper class facilitating analysis of batch results. Basing on issued operations and results
     * array splits provided operations into reads/writes, failed/successful.
     */
    private static class SplitBatchResponse<T extends Row> {
      final List<Get> successfulReads = new ArrayList<>();
      final List<T> failedWrites = new ArrayList<>();
      final List<T> successfulWrites = new ArrayList<>();
      final List<T> allSuccessfulOperations = new ArrayList<>();
      final Result[] successfulReadsResults;
      final List<Get> allReads = new ArrayList<>();
      final Result[] allReadsResults;
      final Object[] allSuccessfulResults;

      SplitBatchResponse(List<T> operations, Object[] results) {
        final List<Result> successfulReadsResults = new ArrayList<>();
        final List<Result> allReadsResults = new ArrayList<>();
        final List<Object> allSuccessfulResultsList = new ArrayList<>();

        for (int i = 0; i < operations.size(); i++) {
          T operation = operations.get(i);
          boolean isRead = operation instanceof Get;
          boolean isFailed = results[i] == null;
          if (isFailed) {
            if (isRead) {
              this.allReads.add((Get) operation);
              allReadsResults.add(null);
            } else {
              this.failedWrites.add(operation);
            }
          } else {
            if (isRead) {
              this.successfulReads.add((Get) operation);
              successfulReadsResults.add((Result) results[i]);

              this.allReads.add((Get) operation);
              allReadsResults.add((Result) results[i]);
            } else {
              this.successfulWrites.add(operation);
            }
            this.allSuccessfulOperations.add(operation);
            allSuccessfulResultsList.add(results[i]);
          }
        }
        this.successfulReadsResults = successfulReadsResults.toArray(new Result[0]);
        this.allReadsResults = allReadsResults.toArray(new Result[0]);
        this.allSuccessfulResults = allSuccessfulResultsList.toArray(new Object[0]);
      }
    }
  }
}
