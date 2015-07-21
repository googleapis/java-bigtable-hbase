/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.grpc.StreamingBigtableResultScanner.RowMerger;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.Calls;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A gRPC client to access the v1 Bigtable service.
 */
public class BigtableGrpcClient implements BigtableClient {

  /**
   * A StreamObserver for unary async operations. It assumes that the operation is complete
   * as soon as a single response is received.
   * @param <T> The response type.
   */
  static class AsyncUnaryOperationObserver<T> implements StreamObserver<T> {
    private final SettableFuture<T> completionFuture = SettableFuture.create();

    @Override
    public void onValue(T t) {
      completionFuture.set(t);
    }

    @Override
    public void onError(Throwable throwable) {
      completionFuture.setException(throwable);
    }

    @Override
    public void onCompleted() {
    }

    public ListenableFuture<T> getCompletionFuture() {
      return completionFuture;
    }
  }

  /**
   * A StreamObserver that sends results to a StreamingBigtableResultScanner.
   */
  public static class ReadRowsStreamObserver
      implements StreamObserver<ReadRowsResponse> {
    private final StreamingBigtableResultScanner scanner;

    public ReadRowsStreamObserver(StreamingBigtableResultScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public void onValue(ReadRowsResponse readTableResponse) {
      scanner.addResult(readTableResponse);
    }

    @Override
    public void onError(Throwable throwable) {
      scanner.setError(throwable);
    }

    @Override
    public void onCompleted() {
      scanner.complete();
    }
  }

  /**
   * CollectingStreamObserver buffers all stream messages in an internal
   * List and signals the result of {@link #getResponseCompleteFuture()} when complete.
   */
  public static class CollectingStreamObserver<T> implements StreamObserver<T> {
    private final SettableFuture<List<T>> responseCompleteFuture = SettableFuture.create();
    private final List<T> buffer = new ArrayList<>();

    public ListenableFuture<List<T>> getResponseCompleteFuture() {
      return responseCompleteFuture;
    }

    @Override
    public void onValue(T value) {
      buffer.add(value);
    }

    @Override
    public void onError(Throwable throwable) {
      responseCompleteFuture.setException(throwable);
    }

    @Override
    public void onCompleted() {
      responseCompleteFuture.set(buffer);
    }
  }

  /**
   * The number of rows to read in before blocking.
   * TODO: Wire this into a settable option.
   */
  public static final int SCANNER_BUFFER_SIZE = 32;
  
  private final Channel channel;
  private final ExecutorService executorService;
  private final BigtableGrpcClientOptions clientOptions;

  public BigtableGrpcClient(
      Channel channel,
      ExecutorService executorService,
      BigtableGrpcClientOptions clientOptions) {
    this.channel = channel;
    this.executorService = executorService;
    this.clientOptions = clientOptions;
  }

  protected static <T, V> ListenableFuture<V> listenableAsyncCall(
      Channel channel,
      MethodDescriptor<T, V> method, T request) {
    Call<T, V> call = channel.newCall(method);
    AsyncUnaryOperationObserver<V> observer = new AsyncUnaryOperationObserver<>();
    Calls.asyncUnaryCall(call, request, observer);
    return observer.getCompletionFuture();
  }

  @Override
  public Empty mutateRow(MutateRowRequest request) throws ServiceException {
    return Calls.blockingUnaryCall(
        channel.newCall(BigtableServiceGrpc.CONFIG.mutateRow), request);
  }

  @Override
  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request) {
    return listenableAsyncCall(
        channel,
        BigtableServiceGrpc.CONFIG.mutateRow, request);
  }

  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request)
      throws ServiceException {
    return Calls.blockingUnaryCall(
        channel.newCall(BigtableServiceGrpc.CONFIG.checkAndMutateRow), request);
  }

  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return listenableAsyncCall(
        channel,
        BigtableServiceGrpc.CONFIG.checkAndMutateRow, request);
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return Calls.blockingUnaryCall(
        channel.newCall(BigtableServiceGrpc.CONFIG.readModifyWriteRow), request);
  }

  @Override
  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    return listenableAsyncCall(
        channel,
        BigtableServiceGrpc.CONFIG.readModifyWriteRow, request);
  }

  @Override
  public ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return ImmutableList.copyOf(Calls.blockingServerStreamingCall(
        channel.newCall(BigtableServiceGrpc.CONFIG.sampleRowKeys), request));
  }

  @Override
  public ListenableFuture<ImmutableList<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    CollectingStreamObserver<SampleRowKeysResponse> responseBuffer =
        new CollectingStreamObserver<>();
    Calls.asyncServerStreamingCall(
        channel.newCall(BigtableServiceGrpc.CONFIG.sampleRowKeys),
        request,
        responseBuffer);
    return Futures.transform(
        responseBuffer.getResponseCompleteFuture(),
        new Function<List<SampleRowKeysResponse>, ImmutableList<SampleRowKeysResponse>>() {
          @Override
          public ImmutableList<SampleRowKeysResponse> apply(
              List<SampleRowKeysResponse> sampleRowKeysResponses) {
            return ImmutableList.copyOf(sampleRowKeysResponses);
          }
        });
  }

  @Override
  public ResultScanner<Row> readRows(ReadRowsRequest request) {
    return readRows(request, clientOptions.getStreamingRetryOptions().enableRetries());
  }

  /**
   * Begin reading rows, optionally with a resumable scanner.
   */
  private ResultScanner<Row> readRows(ReadRowsRequest request, boolean resumable) {
    // Delegate all resumable operations to the scanner. It will request a non-resumable
    // scanner during operation.
    if (resumable) {
      return new ResumingStreamingResultScanner(
          clientOptions.getStreamingRetryOptions(),
          request,
          new BigtableResultScannerFactory() {
            @Override
            public ResultScanner<Row> createScanner(ReadRowsRequest request) {
              return readRows(request, false);
            }
          });
    }

    final Call<ReadRowsRequest , ReadRowsResponse> readRowsCall =
        channel.newCall(BigtableServiceGrpc.CONFIG.readRows);

    // If the scanner is close()d before we're done streaming, we want to cancel the RPC:
    CancellationToken cancellationToken = new CancellationToken();
    cancellationToken.addListener(new Runnable() {
      @Override
      public void run() {
        readRowsCall.cancel();
      }
    }, executorService);

    StreamingBigtableResultScanner resultScanner =
        new StreamingBigtableResultScanner(
            clientOptions.getStreamingBufferSize(),
            clientOptions.getReadPartialRowTimeoutMillis(),
            cancellationToken);

    Calls.asyncServerStreamingCall(
        readRowsCall,
        request,
        new ReadRowsStreamObserver(resultScanner));

    return resultScanner;
  }

  @Override
  public ListenableFuture<List<Row>> readRowsAsync(final ReadRowsRequest request) {
    final Call<ReadRowsRequest , ReadRowsResponse> readRowsCall =
        channel.newCall(BigtableServiceGrpc.CONFIG.readRows);

    CollectingStreamObserver<ReadRowsResponse> responseCollector =
        new CollectingStreamObserver<>();

    Calls.asyncServerStreamingCall(
        readRowsCall,
        request,
        responseCollector);

    return Futures.transform(
        responseCollector.getResponseCompleteFuture(),
        new Function<List<ReadRowsResponse>, List<Row>>() {
          @Override
          public List<Row> apply(List<ReadRowsResponse> responses) {
            List<Row> result = new ArrayList<>();
            Iterator<ReadRowsResponse> responseIterator = responses.iterator();
            while (responseIterator.hasNext()) {
              RowMerger currentRowMerger = new RowMerger();
              while (responseIterator.hasNext() && !currentRowMerger.isRowCommitted()) {
                currentRowMerger.addPartialRow(responseIterator.next());
              }
              result.add(currentRowMerger.buildRow());
            }
            return result;
          }
        });
  }

  @VisibleForTesting
  BigtableGrpcClientOptions getClientOptions() {
    return clientOptions;
  }

  @VisibleForTesting
  public Channel getChannel() {
    return channel;
  }
}
