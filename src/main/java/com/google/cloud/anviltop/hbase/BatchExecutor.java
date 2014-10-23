package com.google.cloud.anviltop.hbase;

import com.google.api.client.util.Preconditions;
import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServices;
import com.google.cloud.anviltop.hbase.adapters.DeleteAdapter;
import com.google.cloud.anviltop.hbase.adapters.GetAdapter;
import com.google.cloud.anviltop.hbase.adapters.GetRowResponseAdapter;
import com.google.cloud.anviltop.hbase.adapters.IncrementAdapter;
import com.google.cloud.anviltop.hbase.adapters.IncrementRowResponseAdapter;
import com.google.cloud.anviltop.hbase.adapters.OperationAdapter;
import com.google.cloud.anviltop.hbase.adapters.PutAdapter;
import com.google.cloud.anviltop.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.anviltop.hbase.adapters.UnsupportedOperationAdapter;
import com.google.cloud.hadoop.hbase.AnviltopClient;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.cloud.hadoop.hbase.repackaged.protobuf.GeneratedMessage;
import com.google.cloud.hadoop.hbase.repackaged.protobuf.ServiceException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

/**
 * Class to help AnviltopTable with batch operations on an AnviltopClient.
 */
public class BatchExecutor {

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

  protected final AnviltopClient client;
  protected final AnviltopOptions options;
  protected final TableName tableName;
  protected final ExecutorService service;
  protected final GetAdapter getAdapter;
  protected final GetRowResponseAdapter getRowResponseAdapter;
  protected final PutAdapter putAdapter;
  protected final DeleteAdapter deleteAdapter;
  protected final RowMutationsAdapter rowMutationsAdapter;
  protected final IncrementAdapter incrementAdapter;
  protected final IncrementRowResponseAdapter incrRespAdapter;
  protected final OperationAdapter<Append, ?> appendAdapter;

  public BatchExecutor(
      AnviltopClient client,
      AnviltopOptions options,
      TableName tableName,
      ExecutorService service,
      GetAdapter getAdapter,
      GetRowResponseAdapter getRowResponseAdapter,
      PutAdapter putAdapter,
      DeleteAdapter deleteAdapter,
      RowMutationsAdapter rowMutationsAdapter,
      IncrementAdapter incrementAdapter,
      IncrementRowResponseAdapter incrRespAdapter) {
    this.client = client;
    this.options = options;
    this.tableName = tableName;
    this.service = service;
    this.getAdapter = getAdapter;
    this.getRowResponseAdapter = getRowResponseAdapter;
    this.putAdapter = putAdapter;
    this.deleteAdapter = deleteAdapter;
    this.rowMutationsAdapter = rowMutationsAdapter;
    this.incrementAdapter = incrementAdapter;
    this.incrRespAdapter = incrRespAdapter;
    this.appendAdapter = new UnsupportedOperationAdapter<>("append");
  }

  /**
   * Helper to construct a proper MutateRowRequest populated with project, table and mutations.
   */
  AnviltopServices.MutateRowRequest.Builder makeMutateRowRequest(
      AnviltopData.RowMutation.Builder mutation) {
    AnviltopServices.MutateRowRequest.Builder requestBuilder =
        AnviltopServices.MutateRowRequest.newBuilder();
    return requestBuilder
        .setMutation(mutation)
        .setTableName(tableName.getQualifierAsString())
        .setProjectId(options.getProjectId());
  }

  /**
   * Adapt and issue a single Delete request returning a ListenableFuture for the MutateRowResponse.
   */
  ListenableFuture<AnviltopServices.MutateRowResponse> issueDeleteRequest(Delete delete) {
    AnviltopData.RowMutation.Builder mutationBuilder = deleteAdapter.adapt(delete);
    AnviltopServices.MutateRowRequest.Builder requestBuilder =
        makeMutateRowRequest(mutationBuilder);

    try {
      return client.mutateAtomicAsync(requestBuilder.build());
    } catch (ServiceException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * Adapt and issue multiple Delete requests returning a list of ListenableFuture instances
   * for the MutateRowResponses.
   */
  List<ListenableFuture<AnviltopServices.MutateRowResponse>> issueDeleteRequests(
      List<Delete> deletes) {
    List<ListenableFuture<AnviltopServices.MutateRowResponse>> responseFutures =
        Lists.transform(deletes,
            new Function<Delete,
                ListenableFuture<AnviltopServices.MutateRowResponse>>() {
              @Override
              public ListenableFuture<AnviltopServices.MutateRowResponse> apply(Delete delete) {
                return issueDeleteRequest(delete);
              }
            });

    // Force evaluation of the lazy transforms:
    return Lists.newArrayList(responseFutures);
  }

  /**
   * Adapt and issue a single Get request returning a ListenableFuture
   * for the GetRowResponse.
   */
  ListenableFuture<AnviltopServices.GetRowResponse> issueGetRequest(Get get) {
    AnviltopServices.GetRowRequest.Builder builder = getAdapter.adapt(get);
    AnviltopServices.GetRowRequest request =
        builder
            .setTableName(tableName.getQualifierAsString())
            .setProjectId(options.getProjectId())
            .build();

    try {
      return client.getRowAsync(request);
    } catch (ServiceException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * Adapt and issue multiple Get requests returning a list of ListenableFuture instances
   * for the GetRowResponses.
   */
  List<ListenableFuture<AnviltopServices.GetRowResponse>> issueGetRequests(List<Get> gets) {
    List<ListenableFuture<AnviltopServices.GetRowResponse>> responseFutures =
        Lists.transform(gets,
            new Function<Get, ListenableFuture<AnviltopServices.GetRowResponse>>() {
              @Override
              public ListenableFuture<AnviltopServices.GetRowResponse> apply(Get get) {
                return issueGetRequest(get);
              }
            });
    // Force evaluation of the lazy transforms:
    return Lists.newArrayList(responseFutures);
  }

  /**
   * Adapt and issue a single Increment request returning a ListenableFuture
   * for the IncrementRowResponse.
   */
  ListenableFuture<AnviltopServices.IncrementRowResponse> issueIncrementRequest(
      Increment increment) {
    AnviltopServices.IncrementRowRequest request =
        incrementAdapter.adapt(increment)
            .setTableName(tableName.getQualifierAsString())
            .setProjectId(options.getProjectId())
            .build();

    try {
      return client.incrementRowAsync(request);
    } catch (ServiceException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * Adapt and issue a single Put request returning a ListenableFuture for the MutateRowResponse.
   */
  ListenableFuture<AnviltopServices.MutateRowResponse> issuePutRequest(Put put) {
    AnviltopData.RowMutation.Builder mutationBuilder = putAdapter.adapt(put);
    AnviltopServices.MutateRowRequest.Builder requestBuilder =
        makeMutateRowRequest(mutationBuilder);

    try {
      return client.mutateAtomicAsync(requestBuilder.build());
    } catch (ServiceException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * Adapt and issue multiple Put requests returning a list of ListenableFuture instances
   * for the MutateRowResponses.
   */
  List<ListenableFuture<AnviltopServices.MutateRowResponse>> issuePutRequests(List<Put> puts) {
    List<ListenableFuture<AnviltopServices.MutateRowResponse>> responseFutures =
        Lists.transform(puts,
            new Function<Put,
                ListenableFuture<AnviltopServices.MutateRowResponse>>() {
              @Override
              public ListenableFuture<AnviltopServices.MutateRowResponse> apply(Put put) {
                return issuePutRequest(put);
              }
            });
    // Force evaluation of the lazy transforms:
    return Lists.newArrayList(responseFutures);
  }

  /**
   * Adapt and issue a single Put request returning a ListenableFuture for the MutateRowResponse.
   */
  ListenableFuture<AnviltopServices.MutateRowResponse> issueRowMutationsRequest(
      RowMutations mutations) {
    AnviltopData.RowMutation.Builder mutationBuilder = rowMutationsAdapter.adapt(mutations);
    AnviltopServices.MutateRowRequest.Builder requestBuilder =
        makeMutateRowRequest(mutationBuilder);

    try {
      return client.mutateAtomicAsync(requestBuilder.build());
    } catch (ServiceException e) {
      return Futures.immediateFailedFuture(e);
    }
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
    final SettableFuture<Object> resultFuture = SettableFuture.create();
    results[index] = null;
    if (row instanceof Delete) {
      ListenableFuture<AnviltopServices.MutateRowResponse> rpcResponseFuture =
          issueDeleteRequest((Delete) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, AnviltopServices.MutateRowResponse>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(AnviltopServices.MutateRowResponse response) {
              return new Result();
            }
          },
          service);
    } else  if (row instanceof Get) {
      ListenableFuture<AnviltopServices.GetRowResponse> rpcResponseFuture =
          issueGetRequest((Get) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, AnviltopServices.GetRowResponse>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(AnviltopServices.GetRowResponse response) {
              return getRowResponseAdapter.adaptResponse(response);
            }
          },
          service);
    } else if (row instanceof Increment) {
      ListenableFuture<AnviltopServices.IncrementRowResponse> rpcResponseFuture =
          issueIncrementRequest((Increment) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, AnviltopServices.IncrementRowResponse>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(AnviltopServices.IncrementRowResponse response) {
              return incrRespAdapter.adaptResponse(response);
            }
          },
          service);
    } else if (row instanceof Put) {
      ListenableFuture<AnviltopServices.MutateRowResponse> rpcResponseFuture =
          issuePutRequest((Put) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, AnviltopServices.MutateRowResponse>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(AnviltopServices.MutateRowResponse response) {
              return new Result();
            }
          },
          service);
    } else if (row instanceof RowMutations) {
      ListenableFuture<AnviltopServices.MutateRowResponse> rpcResponseFuture =
          issueRowMutationsRequest((RowMutations) row);
      Futures.addCallback(rpcResponseFuture,
          new RpcResultFutureCallback<T, AnviltopServices.MutateRowResponse>(
              row, callback, index, results, resultFuture) {
            @Override
            Object adaptResponse(AnviltopServices.MutateRowResponse response) {
              return new Result();
            }
          },
          service);
    } else if (row instanceof Append) {
      resultFuture.setException(new UnsupportedOperationException("Append in batch."));
    } else {
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
    } catch (ExecutionException e) {
      throw new IOException("Batch error", e);
    }
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#batch(List)}
   */
  public Object[] batch(List<? extends Row> actions) throws IOException {
    Result[] results = new Result[actions.size()];
    int index = 0;
    List<ListenableFuture<Object>> resultFutures = new ArrayList<>(actions.size());
    for (Row row : actions) {
      resultFutures.add(issueRowRequest(row, null, results, index++));
    }
    try {
      Futures.allAsList(resultFutures).get();
    } catch (InterruptedException | ExecutionException e) {
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
    Result[] results = new Result[actions.size()];
    int index = 0;
    List<ListenableFuture<Object>> resultFutures = new ArrayList<>(actions.size());
    for (Row row : actions) {
      resultFutures.add(issueRowRequest(row, callback, results, index++));
    }
    try {
      Futures.allAsList(resultFutures).get();
    } catch (ExecutionException e) {
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
      throw new IOException("batchCallback error", e);
    }
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#delete(List)}
   */
  public List<AnviltopServices.MutateRowResponse> delete(List<Delete> deletes) throws IOException {
    List<AnviltopServices.MutateRowResponse> responses;
    try {
      /* The following is from Table#delete(List): "If there are any failures even after retries,
       * there will be a null in the results array for those Gets, AND an exception will be thrown."
       * This sentence makes my head hurt. The best interpretation I can come up with is "execute
       * all gets and then throw an exception". This is what allAsList will do. Wait for all to
       * complete and throw an exception for failed futures.
       */
      responses = Futures.allAsList(issueDeleteRequests(deletes)).get();
    } catch (ExecutionException | InterruptedException e) {
      // TODO: For Execution exception, add inspection of ExecutionException#getCause to get the
      // real issue.
      throw new IOException("Error in batch delete", e);
    }

    return responses;
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#exists(List)}.
   */
  public Boolean[] exists(List<Get> gets) throws IOException {
    // get(gets) will throw if there are any errors:
    Result[] getResults = get(gets);

    Boolean[] exists = new Boolean[getResults.length];
    for (int index = 0; index < getResults.length; index++) {
      exists[index] = !getResults[index].isEmpty();
    }
    return exists;
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#get(List)}
   */
  public Result[] get(List<Get> gets) throws IOException {
    List<AnviltopServices.GetRowResponse> responses;
    try {
      /* The following is from Table#get(List): "If there are any failures even after retries,
       * there will be a null in the results array for those Gets, AND an exception will be thrown."
       * This sentence makes my head hurt. The best interpretation I can come up with is "execute
       * all gets and then throw an exception". This is what allAsList will do. Wait for all to
       * complete and throw an exception for failed futures.
       */
      responses = Futures.allAsList(issueGetRequests(gets)).get();
    } catch (ExecutionException | InterruptedException e) {
      // TODO: For Execution exception, add inspection of ExecutionException#getCause to get the
      // real issue.
      throw new IOException("Error in batch get", e);
    }

    List<Result> resultList =
        Lists.transform(responses, new Function<AnviltopServices.GetRowResponse, Result>(){
          @Override
          public Result apply(@Nullable AnviltopServices.GetRowResponse getRowResponse) {
            return getRowResponseAdapter.adaptResponse(getRowResponse);
          }
        });

    int resultCount = resultList.size();
    Result[] results = new Result[resultCount];
    resultList.toArray(results);
    return results;
  }

  /**
   * Implementation of {@link org.apache.hadoop.hbase.client.HTable#put(List)}
   */
  public List<AnviltopServices.MutateRowResponse> put(List<Put> puts) throws IOException {
    List<AnviltopServices.MutateRowResponse> responses;
    try {
      /* The following is from Table#put(List): "If there are any failures even after retries,
       * there will be a null in the results array for those Gets, AND an exception will be thrown."
       * This sentence makes my head hurt. The best interpretation I can come up with is "execute
       * all gets and then throw an exception". This is what allAsList will do. Wait for all to
       * complete and throw an exception for failed futures.
       */
      responses = Futures.allAsList(issuePutRequests(puts)).get();
    } catch (ExecutionException | InterruptedException e) {
      // TODO: For Execution exception, add inspection of ExecutionException#getCause to get the
      // real issue.
      throw new IOException("Error in batch put", e);
    }

    return responses;
  }
}
