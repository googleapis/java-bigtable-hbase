/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.google.api.client.util.Preconditions;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.GeneratedMessageV3;

/**
 * Class to help BigtableTable with batch operations on an BigtableClient.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BatchExecutor {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BatchExecutor.class);

  /**
   * For callbacks that take a region, this is the region we will use.
   */
  public static final byte[] NO_REGION = new byte[0];

  private static final Function<List<com.google.bigtable.v2.Row>, com.google.bigtable.v2.Row> ROWS_TO_ROW_CONVERTER =
      new Function<List<com.google.bigtable.v2.Row>, com.google.bigtable.v2.Row>() {
        @Override
        public com.google.bigtable.v2.Row apply(List<com.google.bigtable.v2.Row> rows) {
          if (rows.isEmpty()) {
            return null;
          } else {
            return rows.get(0);
          }
        }
      };

  /**
   * A callback for ListenableFutures issued as a result of an RPC
   * @param <T> The type of message the hbase callback requires.
   */
  static class RpcResultFutureCallback<T> implements FutureCallback<GeneratedMessageV3> {
    private final Row row;
    private final Batch.Callback<T> callback;

    // TODO(sduskis): investigate if we really need both the results array and result future.
    private final int index;
    private final Object[] resultsArray;
    private final SettableFuture<Result> resultFuture;

    public RpcResultFutureCallback(
        Row row,
        Batch.Callback<T> callback,
        int index,
        Object[] resultsArray,
        SettableFuture<Result> resultFuture) {
      this.row = row;
      this.callback = callback;
      this.index = index;
      this.resultsArray = resultsArray;
      this.resultFuture = resultFuture;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void onSuccess(GeneratedMessageV3 message) {
      try {
        Result result = Result.EMPTY_RESULT;
        if (message instanceof com.google.bigtable.v2.Row) {
          result = Adapters.ROW_ADAPTER.adaptResponse((com.google.bigtable.v2.Row) message);
        } else if (message instanceof ReadModifyWriteRowResponse) {
          result =
              Adapters.ROW_ADAPTER.adaptResponse(((ReadModifyWriteRowResponse) message).getRow());
        }
        resultsArray[index] = result;
        resultFuture.set(result);
        if (callback != null) {
          callback.update(NO_REGION, row.getRow(), (T) result);
        }
      } catch (Throwable throwable) {
        resultFuture.setException(throwable);
      }
    }

    @Override
    public final void onFailure(Throwable throwable) {
      resultsArray[index] = null;
      resultFuture.setException(throwable);
      if (callback != null) {
        callback.update(NO_REGION, row.getRow(), null);
      }
    }
  }

  protected static class BulkOperation {
    private final AsyncExecutor asyncExecutor;
    private final BigtableOptions options;
    private BulkMutation bulkMutation;
    private BulkRead bulkRead;

    public BulkOperation(
        BigtableSession session,
        AsyncExecutor asyncExecutor,
        BigtableTableName tableName) {
      this.asyncExecutor = asyncExecutor;
      this.options = session.getOptions();
      this.bulkRead = session.createBulkRead(tableName);
      this.bulkMutation = session.createBulkMutation(tableName, asyncExecutor);
    }

    public ListenableFuture<? extends GeneratedMessageV3> mutateRowAsync(MutateRowRequest request)
        throws InterruptedException {
      if (!options.getBulkOptions().useBulkApi()) {
        return asyncExecutor.mutateRowAsync(request);
      } else {
        return bulkMutation.add(request);
      }
    }

    public ListenableFuture<? extends GeneratedMessageV3> readRowsAsync(ReadRowsRequest request)
        throws InterruptedException {
      if (!options.getBulkOptions().useBulkApi()) {
        return Futures.transform(asyncExecutor.readRowsAsync(request), ROWS_TO_ROW_CONVERTER);
      } else {
        return Futures.transform(bulkRead.add(request), ROWS_TO_ROW_CONVERTER);
      }
    }

    public void flush() {
      // If there is a bulk mutation in progress, then send it.
      bulkMutation.flush();
      bulkRead.flush();
    }
  }

  protected final BigtableSession session;
  protected final AsyncExecutor asyncExecutor;
  protected final BigtableOptions options;
  protected final HBaseRequestAdapter requestAdapter;
  protected final Timer batchTimer = BigtableClientMetrics.timer(MetricLevel.Debug, "batch.latency");

  /**
   * Constructor for BatchExecutor.
   *
   * @param session a {@link com.google.cloud.bigtable.grpc.BigtableSession} object.
   * @param requestAdapter a {@link com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter}
   *     object.
   */
  public BatchExecutor(BigtableSession session, HBaseRequestAdapter requestAdapter) {
    this.session = session;
    this.asyncExecutor = session.createAsyncExecutor();
    this.options = session.getOptions();
    this.requestAdapter = requestAdapter;
  }

  /**
   * Issue a single RPC recording the result into {@code results[index]} and if not-null, invoking
   * the supplied callback.
   * @param bulkOperation An object that encapsulates a set of Mutations and Reads that happen via a
   *          bulk API.
   * @param row The action to perform
   * @param callback The callback to invoke when the RPC completes and we have results
   * @param results An array of results, into which we should store the result of the operation
   * @param index The into into the array of results where we should store our result
   * @param <R> The action type
   * @param <T> The type of the callback.
   * @return A ListenableFuture that will have the result when the RPC completes.
   */
  private <R extends Row, T> ListenableFuture<Result> issueAsyncRowRequest(
      BulkOperation bulkOperation, Row row, Batch.Callback<T> callback, Object[] results,
      int index) {
    LOG.trace("issueRowRequest(BulkOperation, Row, Batch.Callback, Object[], index");
    SettableFuture<Result> resultFuture = SettableFuture.create();
    RpcResultFutureCallback<T> futureCallback =
        new RpcResultFutureCallback<T>(row, callback, index, results, resultFuture);
    results[index] = null;
    Futures.addCallback(issueAsyncRequest(bulkOperation, row), futureCallback);
    return resultFuture;
  }

  private ListenableFuture<? extends GeneratedMessageV3>
      issueAsyncRequest(BulkOperation bulkOperation, Row row) {
    try {
      if (row instanceof Get) {
        return bulkOperation.readRowsAsync(requestAdapter.adapt((Get) row));
      } else if (row instanceof Put) {
        return bulkOperation.mutateRowAsync(requestAdapter.adapt((Put) row));
      } else if (row instanceof Delete) {
        return bulkOperation.mutateRowAsync(requestAdapter.adapt((Delete) row));
      } else if (row instanceof Append) {
        return asyncExecutor.readModifyWriteRowAsync(requestAdapter.adapt((Append) row));
      } else if (row instanceof Increment) {
        return asyncExecutor.readModifyWriteRowAsync(requestAdapter.adapt((Increment) row));
      } else if (row instanceof RowMutations) {
        return bulkOperation.mutateRowAsync(requestAdapter.adapt((RowMutations) row));
      }
    } catch (Exception e) {
      return Futures.immediateFailedFuture(new IOException("Could not process the batch", e));
    }
    LOG.error("Encountered unknown action type %s", row.getClass());
    return Futures.immediateFailedFuture(
      new IllegalArgumentException("Encountered unknown action type: " + row.getClass()));
  }

  /**
   * <p>batch.</p>
   *
   * @param actions a {@link java.util.List} object.
   * @param results an array of {@link java.lang.Object} objects.
   * @throws java.io.IOException if any.
   * @throws java.lang.InterruptedException if any.
   */
  public void batch(List<? extends Row> actions, @Nullable Object[] results)
      throws IOException, InterruptedException {
    if (results == null) {
      results = new Object[actions.size()];
    }
    Preconditions.checkArgument(results.length == actions.size(),
        "Result array must have same dimensions as actions list.");
    Timer.Context timerContext = batchTimer.time();
    List<ListenableFuture<?>> resultFutures = issueAsyncRowRequests(actions, results, null);
    try {
      // Don't want to throw an exception for failed futures, instead the place in results is
      // set to null.
      Futures.successfulAsList(resultFutures).get();
      List<Throwable> problems = new ArrayList<>();
      List<Row> problemActions = new ArrayList<>();
      List<String> hosts = new ArrayList<>();
      for (int i = 0; i < resultFutures.size(); i++){
        try {
          resultFutures.get(i).get();
        } catch (ExecutionException e) {
          problemActions.add(actions.get(i));
          problems.add(e.getCause());
          hosts.add(options.getDataHost().toString());
        }
      }
      if (problems.size() > 0) {
        throw new RetriesExhaustedWithDetailsException(problems, problemActions, hosts);
      }
    } catch (ExecutionException e) {
      LOG.error("Encountered exception in batch(List<>, Object[]).", e);
      throw new IOException("Batch error", e);
    } finally {
      timerContext.close();
    }
  }

  private <R> List<ListenableFuture<?>> issueAsyncRowRequests(List<? extends Row> actions,
      Object[] results, Batch.Callback<R> callback) {
    BulkOperation bulkOperation = new BulkOperation(session, asyncExecutor,
        requestAdapter.getBigtableTableName());
    try {
      List<ListenableFuture<?>> resultFutures = new ArrayList<>(actions.size());
      for (int i = 0; i < actions.size(); i++) {
        resultFutures.add(issueAsyncRowRequest(bulkOperation, actions.get(i), callback, results, i));
      }
      return resultFutures;
    } finally {
      bulkOperation.flush();
    }
  }

  /**
   * <p>batch.</p>
   *
   * @param actions a {@link java.util.List} object.
   * @return an array of {@link org.apache.hadoop.hbase.client.Result} objects.
   * @throws java.io.IOException if any.
   */
  public Result[] batch(List<? extends Row> actions) throws IOException {
    try {
      Result[] results = new Result[actions.size()];
      batch(actions, results);
      return results;
    } catch (InterruptedException e) {
      LOG.error("Encountered exception in batch(List<>).", e);
      throw new IOException("Batch error", e);
    }
  }

  /**
   * Implementation of
   * {@link org.apache.hadoop.hbase.client.HTable#batchCallback(List, Object[], Batch.Callback)}
   *
   * @param actions a {@link java.util.List} object.
   * @param results an array of {@link java.lang.Object} objects.
   * @param callback a {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback} object.
   * @throws java.io.IOException if any.
   * @throws java.lang.InterruptedException if any.
   * @param <R> a R object.
   */
  public <R> void batchCallback(List<? extends Row> actions,
      Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {
    Preconditions.checkArgument(results.length == actions.size(),
        "Result array must be the same length as actions.");
    Timer.Context timerContext = batchTimer.time();
    try {
      // Don't want to throw an exception for failed futures, instead the place in results is
      // set to null.
      Futures.successfulAsList(issueAsyncRowRequests(actions, results, callback)).get();
    } catch (ExecutionException e) {
      LOG.error("Encountered exception in batchCallback(List<>, Object[], Batch.Callback). ", e);
      throw new IOException("batchCallback error", e);
    } finally {
      timerContext.close();
    }
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.Table#existsAll(List)}.
   *
   * @param gets a {@link java.util.List} object.
   * @return an array of {@link java.lang.Boolean} objects.
   * @throws java.io.IOException if any.
   */
  public Boolean[] exists(List<Get> gets) throws IOException {
    // get(gets) will throw if there are any errors:
    Result[] getResults = batch(gets);

    Boolean[] exists = new Boolean[getResults.length];
    for (int index = 0; index < getResults.length; index++) {
      exists[index] = !getResults[index].isEmpty();
    }
    return exists;
  }
}
