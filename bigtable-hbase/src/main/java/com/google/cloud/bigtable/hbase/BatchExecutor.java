package com.google.cloud.bigtable.hbase;

import com.google.api.client.util.Preconditions;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.cloud.bigtable.hbase.adapters.AppendAdapter;
import com.google.cloud.bigtable.hbase.adapters.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.IncrementAdapter;
import com.google.cloud.bigtable.hbase.adapters.OperationAdapter;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

/**
 * Class to help AnviltopTable with batch operations on an AnviltopClient.
 */
public class BatchExecutor {

  protected static final Logger LOG = new Logger(BatchExecutor.class);

  /**
   * For callbacks that take a region, this is the region we will use.
   */
  public static final byte[] NO_REGION = new byte[0];

  /**
   * A callback for ListenableFutures issued as a result of an RPC
   * @param <R>
   * @param <T> The response messsage type.
   */
  static abstract class RpcResultFutureCallback<R, T extends GeneratedMessage>
      implements FutureCallback<T> {

    private final Row row;
    private final Batch.Callback<R> callback;
    private final int index;
    private final Object[] resultsArray;
    private final SettableFuture<Object> resultFuture;

    public RpcResultFutureCallback(
        Row row,
        Batch.Callback<R> callback,
        int index,
        Object[] resultsArray,
        SettableFuture<Object> resultFuture) {
      this.row = row;
      this.callback = callback;
      this.index = index;
      this.resultsArray = resultsArray;
      this.resultFuture = resultFuture;
    }

    /**
     * Adapt a proto result into a client result
     */
    abstract Object adaptResponse(T response);

    @SuppressWarnings("unchecked")
    R unchecked(Object o) {
      return (R)o;
    }

    @Override
    public void onSuccess(T t) {
      try {
        Object result = adaptResponse(t);
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

  protected final BigtableClient client;
  protected final BigtableOptions options;
  protected final TableMetadataSetter tableMetadataSetter;
  protected final ListeningExecutorService service;
  protected final GetAdapter getAdapter;
  protected final OperationAdapter<Put, MutateRowRequest.Builder> putAdapter;
  protected final OperationAdapter<Delete, MutateRowRequest.Builder> deleteAdapter;
  protected final RowMutationsAdapter rowMutationsAdapter;
  protected final AppendAdapter appendAdapter;
  protected final IncrementAdapter incrementAdapter;
  protected final ResponseAdapter<com.google.bigtable.v1.Row, Result> rowToResultAdapter;

  public BatchExecutor(
      BigtableClient client,
      BigtableOptions options,
      TableMetadataSetter tableMetadataSetter,
      ListeningExecutorService service,
      GetAdapter getAdapter,
      OperationAdapter<Put, MutateRowRequest.Builder> putAdapter,
      OperationAdapter<Delete, MutateRowRequest.Builder> deleteAdapter,
      RowMutationsAdapter rowMutationsAdapter,
      AppendAdapter appendAdapter,
      IncrementAdapter incrementAdapter,
      ResponseAdapter<com.google.bigtable.v1.Row, Result> rowToResultAdapter) {
    this.client = client;
    this.options = options;
    this.tableMetadataSetter = tableMetadataSetter;
    this.service = service;
    this.getAdapter = getAdapter;
    this.putAdapter = putAdapter;
    this.deleteAdapter = deleteAdapter;
    this.rowMutationsAdapter = rowMutationsAdapter;
    this.appendAdapter = appendAdapter;
    this.incrementAdapter = incrementAdapter;
    this.rowToResultAdapter = rowToResultAdapter;
  }

  /**
   * Adapt and issue a single Delete request returning a ListenableFuture for the MutateRowResponse.
   */
  ListenableFuture<Empty> issueDeleteRequest(Delete delete) {
    LOG.trace("issueDeleteRequest(Delete)");
    MutateRowRequest.Builder requestBuilder = deleteAdapter.adapt(delete);
    tableMetadataSetter.setMetadata(requestBuilder);
    return client.mutateRowAsync(requestBuilder.build());
  }

  /**
   * Adapt and issue a single Get request returning a ListenableFuture
   * for the GetRowResponse.
   */
  ListenableFuture<com.google.bigtable.v1.Row> issueGetRequest(Get get) {
    LOG.trace("issueGetRequest(Get)");
    ReadRowsRequest.Builder builder = getAdapter.adapt(get);
    tableMetadataSetter.setMetadata(builder);

    ReadRowsRequest request = builder.build();

    ListenableFuture<List<com.google.bigtable.v1.Row>> rowsFuture =
        client.readRowsAsync(request);

    return Futures.transform(
        rowsFuture,
        new Function<List<com.google.bigtable.v1.Row>, com.google.bigtable.v1.Row>() {
          @Override
          public com.google.bigtable.v1.Row apply(List<com.google.bigtable.v1.Row> rows) {
            if (rows.isEmpty()) {
              return null;
            } else {
              return rows.get(0);
            }
          }
        });
  }

  /**
   * Adapt and issue a single Append request returning a ListenableFuture
   * for the AppendRowResponse.
   */
  ListenableFuture<com.google.bigtable.v1.Row> issueAppendRequest(Append append) {
    LOG.trace("issueAppendRequest(Append)");
    ReadModifyWriteRowRequest.Builder builder = appendAdapter.adapt(append);
    tableMetadataSetter.setMetadata(builder);
    ReadModifyWriteRowRequest request = builder.build();

    return client.readModifyWriteRowAsync(request);
  }

  /**
   * Adapt and issue a single Increment request returning a ListenableFuture
   * for the IncrementRowResponse.
   */
  ListenableFuture<com.google.bigtable.v1.Row> issueIncrementRequest(Increment increment) {
    LOG.trace("issueIncrementRequest(Increment)");
    ReadModifyWriteRowRequest.Builder builder = incrementAdapter.adapt(increment);
    tableMetadataSetter.setMetadata(builder);
    ReadModifyWriteRowRequest request = builder.build();

    return client.readModifyWriteRowAsync(request);
  }

  /**
   * Adapt and issue a single Put request returning a ListenableFuture for the MutateRowResponse.
   */
  ListenableFuture<Empty> issuePutRequest(Put put) {
    LOG.trace("issuePutRequest(Put)");
    MutateRowRequest.Builder requestBuilder = putAdapter.adapt(put);
    tableMetadataSetter.setMetadata(requestBuilder);

    return client.mutateRowAsync(requestBuilder.build());
  }

  /**
   * Adapt and issue a single Put request returning a ListenableFuture for the MutateRowResponse.
   */
  ListenableFuture<Empty> issueRowMutationsRequest(RowMutations mutations) {
    MutateRowRequest.Builder requestBuilder = rowMutationsAdapter.adapt(mutations);

    return client.mutateRowAsync(requestBuilder.build());
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
  <R extends Row,T> ListenableFuture<Object> issueRowRequest(
      final Row row, final Batch.Callback<T> callback, final Object[] results, final int index) {
    LOG.trace("issueRowRequest(Row, Batch.Callback, Object[], index");
    final SettableFuture<Object> resultFuture = SettableFuture.create();
    results[index] = null;
    if (row instanceof Delete) {
      ListenableFuture<Empty> rpcResponseFuture =
          issueDeleteRequest((Delete) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, Empty>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(Empty response) {
              return new Result();
            }
          },
          service);
    } else  if (row instanceof Get) {
      ListenableFuture<com.google.bigtable.v1.Row> rpcResponseFuture =
          issueGetRequest((Get) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, com.google.bigtable.v1.Row>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(com.google.bigtable.v1.Row response) {
              return rowToResultAdapter.adaptResponse(response);
            }
          },
          service);
    } else if (row instanceof Append) {
      ListenableFuture<com.google.bigtable.v1.Row> rpcResponseFuture =
          issueAppendRequest((Append) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, com.google.bigtable.v1.Row>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(com.google.bigtable.v1.Row response) {
              return rowToResultAdapter.adaptResponse(response);
            }
          },
          service);
    } else if (row instanceof Increment) {
      ListenableFuture<com.google.bigtable.v1.Row> rpcResponseFuture =
          issueIncrementRequest((Increment) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, com.google.bigtable.v1.Row>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(com.google.bigtable.v1.Row response) {
              return rowToResultAdapter.adaptResponse(response);
            }
          },
          service);
    } else if (row instanceof Put) {
      ListenableFuture<Empty> rpcResponseFuture =
          issuePutRequest((Put) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, Empty>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(Empty response) {
              return new Result();
            }
          },
          service);
    } else if (row instanceof RowMutations) {
      ListenableFuture<Empty> rpcResponseFuture =
          issueRowMutationsRequest((RowMutations) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, Empty>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(Empty response) {
              return new Result();
            }
          },
          service);
    } else {
      LOG.error("Encountered unknown action type %s", row.getClass());
      resultFuture.setException(new UnsupportedOperationException(
          String.format("Unknown action type %s", row.getClass().getCanonicalName())));
    }
    return resultFuture;
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
    int index = 0;
    List<ListenableFuture<Object>> resultFutures = new ArrayList<>(actions.size());
    for (Row row : actions) {
      resultFutures.add(issueRowRequest(row, null, results, index++));
    }
    try {
      // Don't want to throw an exception for failed futures, instead the place in results is
      // set to null.
      Futures.successfulAsList(resultFutures).get();
      Iterator<? extends Row> actionIt = actions.iterator();
      Iterator<ListenableFuture<Object>> resultIt = resultFutures.iterator();
      List<Throwable> problems = new ArrayList<Throwable>();
      List<Row> problemActions = new ArrayList<Row>();
      while (actionIt.hasNext() && resultIt.hasNext()) {
        try {
          resultIt.next().get();
          actionIt.next();
        } catch (ExecutionException e) {
          problemActions.add(actionIt.next());
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
    int index = 0;
    List<ListenableFuture<Object>> resultFutures = new ArrayList<>(actions.size());
    for (Row row : actions) {
      resultFutures.add(issueRowRequest(row, callback, results, index++));
    }
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
    int index = 0;
    List<ListenableFuture<Object>> resultFutures = new ArrayList<>(actions.size());
    for (Row row : actions) {
      resultFutures.add(issueRowRequest(row, callback, results, index++));
    }
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
