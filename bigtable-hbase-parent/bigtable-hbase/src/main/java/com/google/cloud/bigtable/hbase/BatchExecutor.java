/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.common.base.Preconditions;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.util.Bytes;

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

  /**
   * A callback for ListenableFutures issued as a result of an RPC
   * @param <T> The type of message the hbase callback requires.
   */
  static class RpcResultFutureCallback<T> implements FutureCallback<Object> {
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
    public final void onSuccess(Object message) {
      Result result = Result.EMPTY_RESULT;

      try {
        if (message instanceof FlatRow) {
          result = Adapters.FLAT_ROW_ADAPTER.adaptResponse((FlatRow) message);
        } else if (message instanceof com.google.bigtable.v2.Row) {
          result = Adapters.ROW_ADAPTER.adaptResponse((com.google.bigtable.v2.Row) message);
        } else if (message instanceof ReadModifyWriteRowResponse) {
          result = Adapters.ROW_ADAPTER.adaptResponse(((ReadModifyWriteRowResponse) message).getRow());
        }
      } catch(Throwable throwable) {
        onFailure(throwable);
        return;
      }

      resultsArray[index] = result;
      resultFuture.set(result);

      if (callback != null) {
        try {
          callback.update(NO_REGION, row.getRow(), (T) result);
        } catch (Throwable t) {
          LOG.error("User callback threw an exception for " + Bytes.toString(result.getRow()));
        }
      }
    }

    @Override
    public final void onFailure(Throwable throwable) {
      resultsArray[index] = throwable;
      resultFuture.setException(throwable);
    }
  }

  protected static class BulkOperation {
    private BulkMutation bulkMutation;
    private BulkRead bulkRead;

    protected BulkOperation(
        BigtableSession session,
        BigtableTableName tableName) {
      this.bulkRead = session.createBulkRead(tableName);
      this.bulkMutation = session.createBulkMutation(tableName);
    }

    protected void flush() throws InterruptedException {
      // If there is a bulk mutation in progress, then send it.
      bulkMutation.flush();
      bulkRead.flush();
    }
  }

  protected final BigtableSession session;
  protected final AsyncExecutor asyncExecutor;
  protected final BigtableOptions options;
  protected final HBaseRequestAdapter requestAdapter;
  protected final Timer batchTimer = BigtableClientMetrics.timer(MetricLevel.Info, "batch.latency");

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

  private ListenableFuture<?> issueAsyncRequest(BulkOperation bulkOperation, Row row) {
    try {
      if (row instanceof Get) {
        return bulkOperation.bulkRead.add(requestAdapter.adapt((Get) row));
      } else if (row instanceof Put) {
        return bulkOperation.bulkMutation.add(requestAdapter.adaptEntry((Put) row));
      } else if (row instanceof Delete) {
        return bulkOperation.bulkMutation.add(requestAdapter.adaptEntry((Delete) row));
      } else if (row instanceof Append) {
        return asyncExecutor.readModifyWriteRowAsync(requestAdapter.adapt((Append) row));
      } else if (row instanceof Increment) {
        return asyncExecutor.readModifyWriteRowAsync(requestAdapter.adapt((Increment) row));
      } else if (row instanceof RowMutations) {
        return bulkOperation.bulkMutation.add(requestAdapter.adaptEntry((RowMutations) row));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Futures.immediateFailedFuture(new IOException("Could not process the batch due to interrupt", e));
    } catch (Throwable e) {
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
    batchCallback(actions, results, null);
  }

  private <R> List<ListenableFuture<?>> issueAsyncRowRequests(List<? extends Row> actions,
      Object[] results, Batch.Callback<R> callback) throws InterruptedException {
    BulkOperation bulkOperation = new BulkOperation(session, requestAdapter.getBigtableTableName());
    try {
      List<ListenableFuture<?>> resultFutures = new ArrayList<>(actions.size());
      for (int i = 0; i < actions.size(); i++) {
        resultFutures
            .add(issueAsyncRowRequest(bulkOperation, actions.get(i), callback, results, i));
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
      Object[] resultsOrErrors = new Object[actions.size()];
      batchCallback(actions, resultsOrErrors, null);
      // At this point we are guaranteed that the array only contains results,
      // if it had any errors, batch would've thrown an exception
      Result[] results = new Result[resultsOrErrors.length];
      System.arraycopy(resultsOrErrors, 0, results, 0, results.length);
      return results;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
        "Result array must have same dimensions as actions list.");
    Timer.Context timerContext = batchTimer.time();
    List<ListenableFuture<?>> resultFutures = issueAsyncRowRequests(actions, results, callback);
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
      LOG.error("Encountered exception in batchCallback(List<>, Object[], callback).", e);
      throw new IOException("Batch error", e);
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
  public boolean[] exists(List<Get> gets) throws IOException {
    // get(gets) will throw if there are any errors:
    Result[] getResults = batch(gets);

    boolean[] exists = new boolean[getResults.length];
    for (int index = 0; index < getResults.length; index++) {
      exists[index] = !getResults[index].isEmpty();
    }
    return exists;
  }
}
