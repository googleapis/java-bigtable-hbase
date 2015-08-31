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
import com.google.cloud.bigtable.grpc.async.AsyncUnaryOperationObserver;
import com.google.cloud.bigtable.grpc.async.ReadAsync;
import com.google.cloud.bigtable.grpc.async.ReadAsyncFactory;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ScanRetriesExhaustedException;
import com.google.cloud.bigtable.grpc.scanner.StreamingBigtableResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A gRPC client to access the v1 Bigtable service.
 */
public class BigtableDataGrpcClient implements BigtableDataClient {

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
  private final ReadAsync<SampleRowKeysRequest, SampleRowKeysResponse> sampleRowKeysAsync;
  private final ReadAsync<ReadRowsRequest, Row> readRowsAsync;

  public BigtableDataGrpcClient(
      Channel channel,
      ExecutorService executorService,
      RetryOptions retryOptions) {
    this.channel = channel;
    this.executorService = executorService;
    this.retryOptions = retryOptions;

    this.sampleRowKeysAsync = ReadAsyncFactory.createSampleRowKeyAsyncReader(this.channel);
    this.readRowsAsync = ReadAsyncFactory.createRowKeyAysncReader(this.channel);
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
  public ListenableFuture<List<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    return doReadAsync(request, sampleRowKeysAsync);
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
        new ReadRowsStreamObserver(resultScanner));

    return resultScanner;
  }

  @Override
  public ListenableFuture<List<Row>> readRowsAsync(final ReadRowsRequest request) {
    return doReadAsync(request, readRowsAsync);
  }

  private class ReadFallback<REQUEST, RESPONSE> implements FutureFallback<List<RESPONSE>> {
    protected final Log LOG = LogFactory.getLog(ReadFallback.class);

    private final REQUEST request;
    private BackOff currentBackoff;
    private final Sleeper sleeper = Sleeper.DEFAULT;
    private final ReadAsync<REQUEST, RESPONSE> callback;

    private ReadFallback(REQUEST request, ReadAsync<REQUEST, RESPONSE> callback) {
      this.request = request;
      this.callback = callback;
    }

    @Override
    public ListenableFuture<List<RESPONSE>> create(Throwable t)
        throws Exception {
      if (t instanceof StatusRuntimeException) {
        StatusRuntimeException statusException = (StatusRuntimeException) t;
        Status.Code code = statusException.getStatus().getCode();
        if (retryOptions.isRetryableRead(code)) {
          return backOffAndRetry(t);
        }
      }
      return Futures.immediateFailedFuture(t);
    }

    private ListenableFuture<List<RESPONSE>> backOffAndRetry(Throwable cause) throws IOException,
        ScanRetriesExhaustedException {
      if (this.currentBackoff == null) {
        ExponentialBackOff.Builder backOffBuilder =
            new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(retryOptions.getInitialBackoffMillis())
                .setMaxElapsedTimeMillis(retryOptions.getMaxElaspedBackoffMillis())
                .setMultiplier(retryOptions.getBackoffMultiplier());
        this.currentBackoff = backOffBuilder.build();
      }
      long nextBackOff = currentBackoff.nextBackOffMillis();
      if (nextBackOff == BackOff.STOP) {
        LOG.warn("RetriesExhausted: ", cause);
        throw new ScanRetriesExhaustedException("Exhausted streaming retries.", cause);
      }

      sleep(nextBackOff);
      return callback.readAsync(request);
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

  private <REQUEST, RESPONSE> ListenableFuture<List<RESPONSE>> doReadAsync(
      final REQUEST request, ReadAsync<REQUEST, RESPONSE> readAsync) {
    if (retryOptions.enableRetries()) {
      ReadFallback<REQUEST, RESPONSE> readFallback = new ReadFallback<>(request, readAsync);
      return Futures.withFallback(readAsync.readAsync(request), readFallback);
    }
    return readAsync.readAsync(request);
  }
}
