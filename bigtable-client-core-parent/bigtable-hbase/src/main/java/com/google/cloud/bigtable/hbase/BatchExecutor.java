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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.google.common.base.Preconditions;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Class to help BigtableTable with batch operations on an BigtableClient, such as
 * {@link org.apache.hadoop.hbase.client.Table#batch(List, Object[])}.
 * {@link org.apache.hadoop.hbase.client.Table#put(List)} and
 * {@link org.apache.hadoop.hbase.client.Table#get(List)}.  This class relies on implementations found
 * in {@link BulkRead} and in {@link BigtableBufferedMutatorHelper}.
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
        } else if (message instanceof com.google.cloud.bigtable.data.v2.models.Row) {
          result = Adapters.ROW_ADAPTER.adaptResponse((com.google.cloud.bigtable.data.v2.models.Row) message);
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

  protected final BigtableOptions options;
  protected final HBaseRequestAdapter requestAdapter;
  protected final Timer batchTimer = BigtableClientMetrics.timer(MetricLevel.Info, "batch.latency");
  // Once the IBigtableDataClient interface is implemented, this will be removed.
  private final BigtableBufferedMutatorHelper bufferedMutatorHelper;
  private final BulkRead bulkRead;

  /**
   * Constructor for BatchExecutor.
   *
   * @param session a {@link com.google.cloud.bigtable.grpc.BigtableSession} object.
   * @param requestAdapter a {@link com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter}
   *     object.
   */
  public BatchExecutor(BigtableSession session, HBaseRequestAdapter requestAdapter) {
    this.requestAdapter = requestAdapter;
    this.options = session.getOptions();
    this.bulkRead = session.createBulkRead(requestAdapter.getBigtableTableName());
    this.bufferedMutatorHelper = new BigtableBufferedMutatorHelper(
        requestAdapter,
        null, // configuration isn't passed in, but also isn't used in BigtableBufferedMutatorHelper
        session);
  }

  /**
   * Issue a single RPC recording the result into {@code results[index]} and if not-null, invoking
   * the supplied callback.
   * @param row The action to perform
   * @param callback The callback to invoke when the RPC completes and we have results
   * @param results An array of results, into which we should store the result of the operation
   * @param index The into into the array of results where we should store our result
   * @param <R> The action type
   * @param <T> The type of the callback.
   * @return A ListenableFuture that will have the result when the RPC completes.
   */
  private <R extends Row, T> ListenableFuture<Result> issueAsyncRowRequest(
      Row row, Batch.Callback<T> callback, Object[] results,
      int index) {
    LOG.trace("issueRowRequest(Row, Batch.Callback, Object[], index");
    SettableFuture<Result> resultFuture = SettableFuture.create();
    RpcResultFutureCallback<T> futureCallback =
        new RpcResultFutureCallback<T>(row, callback, index, results, resultFuture);
    results[index] = null;
    Futures.addCallback(issueAsyncRequest(row),
        futureCallback, MoreExecutors.directExecutor());
    return resultFuture;
  }

  private ListenableFuture<?> issueAsyncRequest(Row row) {
    try {
      if (row instanceof Get) {
        return bulkRead.add(requestAdapter.adapt((Get) row));
      } else if (row instanceof Mutation) {
        return bufferedMutatorHelper.mutate((Mutation) row);
      } else if (row instanceof RowMutations) {
        return bufferedMutatorHelper.mutate((RowMutations) row);
      }
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

  public <R> List<ListenableFuture<?>> issueAsyncRowRequests(List<? extends Row> actions,
      Object[] results, Batch.Callback<R> callback) {
    try {
      List<ListenableFuture<?>> resultFutures = new ArrayList<>(actions.size());
      for (int i = 0; i < actions.size(); i++) {
        resultFutures.add(issueAsyncRowRequest(actions.get(i), callback, results, i));
      }
      return resultFutures;
    } finally {
      // If there is a bulk mutation in progress, then send it.
      bufferedMutatorHelper.sendUnsent();
      bulkRead.flush();
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
          hosts.add(options.getDataHost());
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
