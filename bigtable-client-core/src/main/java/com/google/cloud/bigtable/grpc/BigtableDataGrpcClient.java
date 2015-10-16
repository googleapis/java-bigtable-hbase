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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.ExecutorService;

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
import com.google.cloud.bigtable.grpc.async.BigtableAsyncUtilities;
import com.google.cloud.bigtable.grpc.async.RetryableRpc;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.grpc.io.ClientCallService;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.StreamingBigtableResultScanner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

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
    public void onNext(ReadRowsResponse readTableResponse) {
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
  private RetryableRpc<SampleRowKeysRequest, List<SampleRowKeysResponse>> sampleRowKeysAsync;
  private RetryableRpc<ReadRowsRequest, List<Row>> readRowsAsync;

  @VisibleForTesting
  private ClientCallService clientCallService;

  public BigtableDataGrpcClient(
      Channel channel,
      ExecutorService executorService,
      RetryOptions retryOptions,
      ClientCallService clientCallService) {
    this.channel = channel;
    this.executorService = executorService;
    this.retryOptions = retryOptions;
    this.clientCallService = clientCallService;
    this.sampleRowKeysAsync =
        BigtableAsyncUtilities.createSampleRowKeyAsyncReader(this.channel, clientCallService);
    this.readRowsAsync =
        BigtableAsyncUtilities.createRowKeyAysncReader(this.channel, clientCallService);
  }

  @Override
  public Empty mutateRow(MutateRowRequest request) throws ServiceException {
    return clientCallService.blockingUnaryCall(
      channel.newCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT), request);
  }

  @Override
  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request) {
    return clientCallService.listenableAsyncCall(request,
      channel.newCall(BigtableServiceGrpc.METHOD_MUTATE_ROW, CallOptions.DEFAULT));
  }

  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request)
      throws ServiceException {
    return clientCallService.blockingUnaryCall(
      channel.newCall(BigtableServiceGrpc.METHOD_CHECK_AND_MUTATE_ROW, CallOptions.DEFAULT),
      request);
  }

  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return clientCallService.listenableAsyncCall(request,
      channel.newCall(BigtableServiceGrpc.METHOD_CHECK_AND_MUTATE_ROW, CallOptions.DEFAULT));
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return clientCallService.blockingUnaryCall(
      channel.newCall(BigtableServiceGrpc.METHOD_READ_MODIFY_WRITE_ROW, CallOptions.DEFAULT),
      request);
  }

  @Override
  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    return clientCallService.listenableAsyncCall(request,
      channel.newCall(BigtableServiceGrpc.METHOD_READ_MODIFY_WRITE_ROW, CallOptions.DEFAULT));
  }

  @Override
  public ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return ImmutableList.copyOf(clientCallService.blockingServerStreamingCall(
      channel.newCall(BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS, CallOptions.DEFAULT), request));
  }

  @Override
  public ListenableFuture<List<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    return BigtableAsyncUtilities.doReadAsync(retryOptions, request, sampleRowKeysAsync,
      executorService);
  }

  @Override
  public ListenableFuture<List<Row>> readRowsAsync(final ReadRowsRequest request) {
    return BigtableAsyncUtilities
        .doReadAsync(retryOptions, request, readRowsAsync, executorService);
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
    final ClientCall<ReadRowsRequest, ReadRowsResponse> readRowsCall =
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

    clientCallService.asyncServerStreamingCall(
        readRowsCall,
        request,
        new ReadRowsStreamObserver(resultScanner));

    return resultScanner;
  }
}
