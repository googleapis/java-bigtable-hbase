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

import com.google.api.client.util.Preconditions;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.OperationAdapter;
import com.google.cloud.bigtable.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.hbase.adapters.ReadHooks;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;

/**
 * Class to help BigtableTable with batch operations on a BigtableAsyncExecutor.
 */
public class BatchExecutor {

  protected static final Logger LOG = new Logger(BatchExecutor.class);

  /**
   * For callbacks that take a region, this is the region we will use.
   */
  public static final byte[] NO_REGION = new byte[0];

  private static final Function<List<com.google.bigtable.v1.Row>, com.google.bigtable.v1.Row> ROWS_TO_ROW_CONVERTER =
      new Function<List<com.google.bigtable.v1.Row>, com.google.bigtable.v1.Row>() {
        @Override
        public com.google.bigtable.v1.Row apply(List<com.google.bigtable.v1.Row> rows) {
          if (rows.isEmpty()) {
            return null;
          } else {
            return rows.get(0);
          }
        }
      };

  private static final Function<GeneratedMessage, Object> ROW_RESULT_CONVERTER =
      new Function<GeneratedMessage, Object>() {

        @Override
        public Object apply(GeneratedMessage response) {
          if (response instanceof com.google.bigtable.v1.Row) {
            return Adapters.ROW_ADAPTER.adaptResponse((com.google.bigtable.v1.Row) response);
          } else {
            return new Result();
          }
        }
      };

  /**
   * A callback for ListenableFutures issued as a result of an RPC
   * @param <R>
   * @param <T> The response messsage type.
   */
  static class RpcResultFutureCallback<R, T extends GeneratedMessage>
      implements FutureCallback<T> {

    private final Row row;
    private final Batch.Callback<R> callback;
    private final int index;
    private final Object[] resultsArray;
    private final SettableFuture<Object> resultFuture;
    private final Function<T, Object> adapter;

    public RpcResultFutureCallback(
        Row row,
        Batch.Callback<R> callback,
        int index,
        Object[] resultsArray,
        SettableFuture<Object> resultFuture,
        Function<T, Object> adapter) {
      this.row = row;
      this.callback = callback;
      this.index = index;
      this.resultsArray = resultsArray;
      this.resultFuture = resultFuture;
      this.adapter = adapter;
    }

    @SuppressWarnings("unchecked")
    R unchecked(Object o) {
      return (R)o;
    }

    @Override
    public void onSuccess(T t) {
      try {
        Object result = adapter.apply(t);
        resultsArray[index] = result;
        if (callback != null) {
          callback.update(NO_REGION, row.getRow(), unchecked(result));
        }
        resultFuture.set(result);
      } catch (Throwable throwable) {
        resultFuture.setException(throwable);
      }
    }

    @Override
    public void onFailure(Throwable throwable) {
      try {
        if (callback != null) {
          callback.update(NO_REGION, row.getRow(), null);
        }
      } finally {
        resultsArray[index] = null;
        resultFuture.setException(throwable);
      }
    }
  }

//  protected final BigtableDataClient client;
  protected final BigtableTableName bigtableTableName;
  protected final ListeningExecutorService service;
  protected final OperationAdapter<Put, MutateRowRequest.Builder> putAdapter;
  protected final RowMutationsAdapter rowMutationsAdapter;
  protected final BigtableAsyncExecutor asyncExecutor;

  public BatchExecutor(
      BigtableTableName bigtableTableName,
      ListeningExecutorService service,
      OperationAdapter<Put, MutateRowRequest.Builder> putAdapter,
      RowMutationsAdapter rowMutationsAdapter,
      BigtableAsyncExecutor asyncExecutor) {
    this.bigtableTableName = bigtableTableName;
    this.service = service;
    this.putAdapter = putAdapter;
    this.rowMutationsAdapter = rowMutationsAdapter;
    this.asyncExecutor = asyncExecutor;
  }

  /**
   * Adapt and issue a single Delete request returning a ListenableFuture for the MutateRowResponse.
   */
  private ListenableFuture<Empty> issueDeleteRequest(Delete delete) {
    LOG.trace("issueDeleteRequest(Delete)");
    MutateRowRequest.Builder requestBuilder = Adapters.DELETE_ADAPTER.adapt(delete);
    requestBuilder.setTableName(bigtableTableName.toString());
    return asyncExecutor.mutateRowAsync(requestBuilder.build());
  }

  /**
   * Adapt and issue a single Get request returning a ListenableFuture
   * for the GetRowResponse.
   */
  private ListenableFuture<com.google.bigtable.v1.Row> issueGetRequest(Get get) {
    LOG.trace("issueGetRequest(Get)");
    ReadHooks readHooks = new DefaultReadHooks();
    ReadRowsRequest.Builder builder = Adapters.GET_ADAPTER.adapt(get, readHooks);
    builder.setTableName(bigtableTableName.toString());
    ReadRowsRequest request = readHooks.applyPreSendHook(builder.build());

    return Futures.transform(asyncExecutor.readRowsAsync(request), ROWS_TO_ROW_CONVERTER);
  }

  /**
   * Adapt and issue a single Append request returning a ListenableFuture
   * for the AppendRowResponse.
   */
  private ListenableFuture<com.google.bigtable.v1.Row> issueAppendRequest(Append append) {
    LOG.trace("issueAppendRequest(Append)");
    ReadModifyWriteRowRequest.Builder builder = Adapters.APPEND_ADAPTER.adapt(append);
    builder.setTableName(bigtableTableName.toString());
    ReadModifyWriteRowRequest request = builder.build();

    return asyncExecutor.readModifyWriteRowAsync(request);
  }

  /**
   * Adapt and issue a single Increment request returning a ListenableFuture
   * for the IncrementRowResponse.
   */
  private ListenableFuture<com.google.bigtable.v1.Row> issueIncrementRequest(Increment increment) {
    LOG.trace("issueIncrementRequest(Increment)");
    ReadModifyWriteRowRequest.Builder builder = Adapters.INCREMENT_ADAPTER.adapt(increment);
    builder.setTableName(bigtableTableName.toString());
    ReadModifyWriteRowRequest request = builder.build();

    return asyncExecutor.readModifyWriteRowAsync(request);
  }

  /**
   * Adapt and issue a single Put request returning a ListenableFuture for the MutateRowResponse.
   */
  private ListenableFuture<Empty> issuePutRequest(Put put) {
    LOG.trace("issuePutRequest(Put)");
    MutateRowRequest.Builder requestBuilder = putAdapter.adapt(put);
    requestBuilder.setTableName(bigtableTableName.toString());

    return asyncExecutor.mutateRowAsync(requestBuilder.build());
  }

  /**
   * Adapt and issue a single Put request returning a ListenableFuture for the MutateRowResponse.
   */
  private ListenableFuture<Empty> issueRowMutationsRequest(RowMutations mutations) {
    MutateRowRequest.Builder requestBuilder = rowMutationsAdapter.adapt(mutations);
    requestBuilder.setTableName(bigtableTableName.toString());

    return asyncExecutor.mutateRowAsync(requestBuilder.build());
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
  private <R extends Row,T> ListenableFuture<Object> issueRowRequest(
      final Row row, final Batch.Callback<T> callback, final Object[] results, final int index) {
    LOG.trace("issueRowRequest(Row, Batch.Callback, Object[], index");
    SettableFuture<Object> resultFuture = SettableFuture.create();
    results[index] = null;
    ListenableFuture<? extends GeneratedMessage> future = issueRequest(row);
    Futures.addCallback(future,
      new RpcResultFutureCallback<T, GeneratedMessage>(
          row, callback, index, results, resultFuture, ROW_RESULT_CONVERTER),
      service);
    return resultFuture;
  }

  private ListenableFuture<? extends GeneratedMessage> issueRequest(Row row) {
    if (row instanceof Put) {
      return issuePutRequest((Put) row);
    } else if (row instanceof Delete) {
      return issueDeleteRequest((Delete) row);
    } else if (row instanceof Append) {
      return issueAppendRequest((Append) row);
    } else if (row instanceof Increment) {
      return issueIncrementRequest((Increment) row);
    } else if (row instanceof Get) {
      return issueGetRequest((Get) row);
    } else if (row instanceof RowMutations) {
      return issueRowMutationsRequest((RowMutations) row);
    }

    LOG.error("Encountered unknown action type %s", row.getClass());
    return Futures.immediateFailedFuture(
        new IllegalArgumentException("Encountered unknown action type: " + row.getClass()));
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#batch(List, Object[])}
   */
  public void batch(List<? extends Row> actions, @Nullable Object[] results)
      throws IOException, InterruptedException {
    LOG.trace("batch(List<>, Object[])");
    if (results == null) {
      results = new Object[actions.size()];
    }
    Preconditions.checkArgument(results.length == actions.size(),
        "Result array must have same dimensions as actions list.");
    List<ListenableFuture<Object>> resultFutures = issueRowRequests(actions, results);
    try {
      // Don't want to throw an exception for failed futures, instead the place in results is
      // set to null.
      Futures.successfulAsList(resultFutures).get();
      List<Throwable> problems = new ArrayList<Throwable>();
      List<Row> problemActions = new ArrayList<Row>();
      for (int i = 0; i < resultFutures.size(); i++) {
        try {
          resultFutures.get(i).get();
        } catch (ExecutionException e) {
          problemActions.add(actions.get(i));
          problems.add(e.getCause());
        }
      }
      if (problems.size() > 0) {
        throw new RetriesExhaustedWithDetailsException(
            problems, problemActions, new ArrayList<String>(problems.size()));
      }
    } catch (ExecutionException e) {
      LOG.error("Encountered exception in batch(List<>, Object[]).", e);
      throw new IOException("Batch error", e);
    }
  }

  private List<ListenableFuture<Object>> issueRowRequests(List<? extends Row> actions,
      Object[] results) {
    List<ListenableFuture<Object>> resultFutures = new ArrayList<>(actions.size());
    for (int i = 0; i < actions.size(); i++) {
      resultFutures.add(issueRowRequest(actions.get(i), null, results, i));
    }
    return resultFutures;
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#batch(List)}
   */
  public Object[] batch(List<? extends Row> actions) throws IOException {
    LOG.trace("batch(List<>)");
    Result[] results = new Result[actions.size()];
    try {
      batch(actions, results);
    } catch (InterruptedException e) {
      LOG.error("Encountered exception in batch(List<>).", e);
      throw new IOException("Batch error", e);
    }
    return results;
  }

  /**
   * Implementation of
   * {@link org.apache.hadoop.hbase.client.HTable#batchCallback(List, Batch.Callback)}
   */
  public <R> Object[] batchCallback(
      List<? extends Row> actions,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Batch.Callback)");
    Result[] results = new Result[actions.size()];
    List<ListenableFuture<Object>> resultFutures = issueRowRequests(actions, results);
    try {
      Futures.allAsList(resultFutures).get();
    } catch (ExecutionException e) {
      LOG.error("Encountered exception in batchCallback(List<>, Batch.Callback). ", e);
      throw new IOException("batchCallback error", e);
    }
    return results;
  }

  /**
   * Implementation of
   * {@link org.apache.hadoop.hbase.client.HTable#batchCallback(List, Object[], Batch.Callback)}
   */
  public <R> void batchCallback(List<? extends Row> actions,
      Object[] results, Batch.Callback<R> callback) throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Object[], Batch.Callback)");
    Preconditions.checkArgument(results.length == actions.size(),
        "Result array must be the same length as actions.");
    List<ListenableFuture<Object>> resultFutures = issueRowRequests(actions, results);
    try {
      // Don't want to throw an exception for failed futures, instead the place in results is
      // set to null.
      Futures.successfulAsList(resultFutures).get();
    } catch (ExecutionException e) {
      LOG.error("Encountered exception in batchCallback(List<>, Object[], Batch.Callback). ", e);
      throw new IOException("batchCallback error", e);
    }
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#exists(List)}.
   */
  public Boolean[] exists(List<Get> gets) throws IOException {
    LOG.trace("exists(List<>)");
    // get(gets) will throw if there are any errors:
    Result[] getResults = (Result[]) batch(gets);

    Boolean[] exists = new Boolean[getResults.length];
    for (int index = 0; index < getResults.length; index++) {
      exists[index] = !getResults[index].isEmpty();
    }
    return exists;
  }
}
