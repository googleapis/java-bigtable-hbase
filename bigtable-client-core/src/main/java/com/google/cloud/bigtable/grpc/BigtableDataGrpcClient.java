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
package com.google.cloud.bigtable.grpc;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
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
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ScanRetriesExhaustedException;
import com.google.cloud.bigtable.grpc.scanner.StreamingBigtableResultScanner;
import com.google.cloud.bigtable.grpc.scanner.StreamingBigtableResultScanner.RowMerger;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A gRPC client to access the v1 Bigtable service.
 */
public class BigtableDataGrpcClient implements BigtableDataClient {

  private static class ImmutableListFunction<T> implements Function<List<T>, ImmutableList<T>> {
    @Override
    public ImmutableList<T> apply(List<T> list) {
      return ImmutableList.copyOf(list);
    }
  }

  private static final ImmutableListFunction<SampleRowKeysResponse> IMMUTABLE_LIST_FUNCTION =
      new ImmutableListFunction<>();

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
    private final ClientCall<ReadRowsRequest, ReadRowsResponse> call;

    public ReadRowsStreamObserver(StreamingBigtableResultScanner scanner,
        ClientCall<ReadRowsRequest, ReadRowsResponse> call) {
      this.scanner = scanner;
      this.call = call;
    }

    @Override
    public void onValue(ReadRowsResponse readTableResponse) {
      call.request(1);
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
    private final ClientCall<?, T> call;

    public CollectingStreamObserver(ClientCall<?, T> call) {
      this.call = call;
    }

    public ListenableFuture<List<T>> getResponseCompleteFuture() {
      return responseCompleteFuture;
    }

    @Override
    public void onValue(T value) {
      buffer.add(value);
      call.request(1);
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

  private final Channel channel;

  private final ExecutorService executorService;
  private final RetryOptions retryOptions;

  private final BigtableResultScannerFactory streamingScannerFactory =
      new BigtableResultScannerFactory() {
        @Override
        public ResultScanner<Row> createScanner(ReadRowsRequest request) {
          return streamRows(request);
        }
      };

  public BigtableDataGrpcClient(
      Channel channel,
      ExecutorService executorService,
      RetryOptions retryOptions) {
    this.channel = channel;
    this.executorService = executorService;
    this.retryOptions = retryOptions;
  }

  protected static <T, V> ListenableFuture<V> listenableAsyncCall(
      Channel channel,
      MethodDescriptor<T, V> method, T request) {
    ClientCall<T, V> call = channel.newCall(method, CallOptions.DEFAULT);
    AsyncUnaryOperationObserver<V> observer = new AsyncUnaryOperationObserver<>();
    ClientCalls.asyncUnaryCall(call, request, observer);
    return observer.getCompletionFuture();
  }

  @Override
  public Empty mutateRow(MutateRowRequest request) throws ServiceException {
    return ClientCalls.blockingUnaryCall(
      channel.newCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT), request);
  }

  @Override
  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request) {
    return listenableAsyncCall(
        channel,
        BigtableServiceGrpc.METHOD_MUTATE_ROW, request);
  }

  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request)
      throws ServiceException {
    return ClientCalls.blockingUnaryCall(
      channel.newCall(BigtableServiceGrpc.METHOD_CHECK_AND_MUTATE_ROW, CallOptions.DEFAULT),
      request);
  }

  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return listenableAsyncCall(channel, BigtableServiceGrpc.METHOD_CHECK_AND_MUTATE_ROW, request);
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return ClientCalls.blockingUnaryCall(
      channel.newCall(BigtableServiceGrpc.METHOD_READ_MODIFY_WRITE_ROW, CallOptions.DEFAULT),
      request);
  }

  @Override
  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    return listenableAsyncCall(channel, BigtableServiceGrpc.METHOD_READ_MODIFY_WRITE_ROW, request);
  }

  @Override
  public ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return ImmutableList.copyOf(ClientCalls.blockingServerStreamingCall(
      channel.newCall(BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS, CallOptions.DEFAULT), request));
  }

  @Override
  public ListenableFuture<ImmutableList<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    ClientCall<SampleRowKeysRequest, SampleRowKeysResponse> call =
        channel.newCall(BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS, CallOptions.DEFAULT);
    CollectingStreamObserver<SampleRowKeysResponse> responseBuffer =
        new CollectingStreamObserver<>(call);
    ClientCalls.asyncServerStreamingCall(
        call,
        request,
        responseBuffer);
    return Futures.transform(
        responseBuffer.getResponseCompleteFuture(),
        IMMUTABLE_LIST_FUNCTION);
  }

  @Override
  public ResultScanner<Row> readRows(ReadRowsRequest request) {
    // Delegate all resumable operations to the scanner. It will request a non-resumable
    // scanner during operation.
    if (retryOptions.enableRetries()) {
      return new ResumingStreamingResultScanner(retryOptions, request, streamingScannerFactory);
    } else {
      return streamRows(request);
    }
  }

  private ResultScanner<Row> streamRows(ReadRowsRequest request) {
    final ClientCall<ReadRowsRequest , ReadRowsResponse> readRowsCall =
        channel.newCall(BigtableServiceGrpc.METHOD_READ_ROWS, CallOptions.DEFAULT);

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
          retryOptions.getStreamingBufferSize(),
          retryOptions.getReadPartialRowTimeoutMillis(),
          cancellationToken);

    ClientCalls.asyncServerStreamingCall(
        readRowsCall,
        request,
        new ReadRowsStreamObserver(resultScanner, readRowsCall));

    return resultScanner;
  }

  @Override
  public ListenableFuture<List<Row>> readRowsAsync(final ReadRowsRequest request) {
    if (retryOptions.enableRetries()) {
      return Futures.withFallback(
          doReadRowsAsync(request), new ReadRowsFallback(request));
    }
    return doReadRowsAsync(request);
  }

  private class ReadRowsFallback implements FutureFallback<List<Row>> {
    protected final Log LOG = LogFactory.getLog(ReadRowsFallback.class);
    
    private final ReadRowsRequest request;
    private final BackOff currentBackoff;
    private final Sleeper sleeper = Sleeper.DEFAULT;

    private ReadRowsFallback(ReadRowsRequest request) {
      this.request = request;
      ExponentialBackOff.Builder backOffBuilder =
          new ExponentialBackOff.Builder()
              .setInitialIntervalMillis(retryOptions.getInitialBackoffMillis())
              .setMaxElapsedTimeMillis(retryOptions.getMaxElaspedBackoffMillis())
              .setMultiplier(retryOptions.getBackoffMultiplier());
      this.currentBackoff = backOffBuilder.build();
    }

    @Override
    public ListenableFuture<List<Row>> create(Throwable t) throws Exception {
      if (t instanceof StatusRuntimeException) {
        StatusRuntimeException statusException = (StatusRuntimeException) t;
        Status.Code code = statusException.getStatus().getCode();
        if (retryOptions.isRetryableRead(code)){
          return backOffAndRetry(t);
        }
      }
      return Futures.immediateFailedFuture(t);
    }

    private ListenableFuture<List<Row>> backOffAndRetry(Throwable cause) throws IOException,
        ScanRetriesExhaustedException {
      long nextBackOff = currentBackoff.nextBackOffMillis();
      if (nextBackOff == BackOff.STOP) {
        LOG.warn("RetriesExhausted: ", cause);
        throw new ScanRetriesExhaustedException("Exhausted streaming retries.", cause);
      }

      sleep(nextBackOff);
      return BigtableDataGrpcClient.this.doReadRowsAsync(request);
    }

    private void sleep(long millis) throws IOException {
      try {
        sleeper.sleep(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while sleeping for resume", e);
      }
    }
  }

  private ListenableFuture<List<Row>> doReadRowsAsync(final ReadRowsRequest request) {
    final ClientCall<ReadRowsRequest , ReadRowsResponse> readRowsCall =
        channel.newCall(BigtableServiceGrpc.METHOD_READ_ROWS, CallOptions.DEFAULT);

    CollectingStreamObserver<ReadRowsResponse> responseCollector =
        new CollectingStreamObserver<>(readRowsCall);

    ClientCalls.asyncServerStreamingCall(
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
}
