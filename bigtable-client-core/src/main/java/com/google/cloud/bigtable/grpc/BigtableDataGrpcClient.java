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
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
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
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.ChannelPool.PooledChannel;
import com.google.cloud.bigtable.grpc.io.ClientCallService;
import com.google.cloud.bigtable.grpc.io.RetryingCall;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.StreamingBigtableResultScanner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

/**
 * A gRPC client to access the v1 Bigtable service.
 */
public class BigtableDataGrpcClient implements BigtableDataClient {

  @VisibleForTesting
  public static final Predicate<MutateRowRequest> IS_RETRYABLE_MUTATION =
      new Predicate<MutateRowRequest>() {
        @Override
        public boolean apply(@Nullable MutateRowRequest mutateRowRequest) {
          return mutateRowRequest != null
              && allCellsHaveTimestamps(mutateRowRequest.getMutationsList());
        }
      };

  @VisibleForTesting
  public static final Predicate<CheckAndMutateRowRequest> IS_RETRYABLE_CHECK_AND_MUTATE =
      new Predicate<CheckAndMutateRowRequest>() {
        @Override
        public boolean apply(@Nullable CheckAndMutateRowRequest checkAndMutateRowRequest) {
          return checkAndMutateRowRequest != null
              && allCellsHaveTimestamps(checkAndMutateRowRequest.getTrueMutationsList())
              && allCellsHaveTimestamps(checkAndMutateRowRequest.getFalseMutationsList());
        }
      };

  private static final boolean allCellsHaveTimestamps(Iterable<Mutation> mutations) {
    for (Mutation mut : mutations) {
      if (mut.getSetCell().getTimestampMicros() == -1) {
        return false;
      }
    }
    return true;
  }

  private final ChannelPool channelPool;

  private final ScheduledExecutorService retryExecutorService;
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

  private ClientCallService clientCallService;

  public BigtableDataGrpcClient(
      ChannelPool channelPool,
      ExecutorService executorService,
      ScheduledExecutorService retryExecutorService,
    RetryOptions retryOptions) {
    this(channelPool, executorService, retryExecutorService, retryOptions,
        ClientCallService.DEFAULT);
  }

  @VisibleForTesting
  BigtableDataGrpcClient(
      ChannelPool channelPool,
      ExecutorService executorService,
      ScheduledExecutorService retryExecutorService,
      RetryOptions retryOptions,
      ClientCallService clientCallService) {
    this.channelPool = channelPool;
    this.executorService = executorService;
    this.retryOptions = retryOptions;
    this.clientCallService = clientCallService;
    this.retryExecutorService = retryExecutorService;

    this.sampleRowKeysAsync =
        BigtableAsyncUtilities.createSampleRowKeyAsyncReader(this.channelPool, clientCallService);
    this.readRowsAsync =
        BigtableAsyncUtilities.createRowKeyAysncReader(this.channelPool, clientCallService);
  }

  @Override
  public Empty mutateRow(MutateRowRequest request) throws ServiceException {
    return clientCallService.blockingUnaryCall(createMutateRowCall(request), request);
  }

  @Override
  public ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request) {
    return clientCallService.listenableAsyncCall(createMutateRowCall(request), request);
  }

  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request)
      throws ServiceException {
    return clientCallService.blockingUnaryCall(createCheckAndMutateRowCall(request), request);
  }

  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return clientCallService.listenableAsyncCall(createCheckAndMutateRowCall(request), request);
  }

  private ClientCall<MutateRowRequest, Empty> createMutateRowCall(MutateRowRequest request) {
    return createRetryableCall(BigtableServiceGrpc.METHOD_MUTATE_ROW,
      IS_RETRYABLE_MUTATION, request);
  }

  private ClientCall<CheckAndMutateRowRequest, CheckAndMutateRowResponse>
      createCheckAndMutateRowCall(CheckAndMutateRowRequest request) {
    return createRetryableCall(BigtableServiceGrpc.METHOD_CHECK_AND_MUTATE_ROW,
      IS_RETRYABLE_CHECK_AND_MUTATE, request);
  }

  private <ReqT, RespT> ClientCall<ReqT, RespT> createRetryableCall(
      MethodDescriptor<ReqT, RespT> method, Predicate<ReqT> isRetryable, ReqT request) {
    if (retryOptions.enableRetries() && isRetryable.apply(request)) {
      return new RetryingCall<ReqT, RespT>(channelPool, method, CallOptions.DEFAULT,
          retryExecutorService, retryOptions);
    } else {
      return channelPool.newCall(method, CallOptions.DEFAULT);
    }
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return clientCallService.blockingUnaryCall(
      channelPool.newCall(BigtableServiceGrpc.METHOD_READ_MODIFY_WRITE_ROW, CallOptions.DEFAULT),
      request);
  }

  @Override
  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request) {
    return clientCallService.listenableAsyncCall(
      channelPool.newCall(BigtableServiceGrpc.METHOD_READ_MODIFY_WRITE_ROW, CallOptions.DEFAULT),
      request);
  }

  @Override
  public ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return ImmutableList
        .copyOf(clientCallService.blockingServerStreamingCall(
          channelPool.newCall(BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS, CallOptions.DEFAULT),
          request));
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
    PooledChannel reservedChannel = channelPool.reserveChannel();
    final ClientCall<ReadRowsRequest, ReadRowsResponse> readRowsCall =
        reservedChannel.newCall(BigtableServiceGrpc.METHOD_READ_ROWS, CallOptions.DEFAULT);

    // If the scanner is close()d before we're done streaming, we want to cancel the RPC:
    CancellationToken cancellationToken = new CancellationToken();
    cancellationToken.addListener(new Runnable() {
      @Override
      public void run() {
        readRowsCall.cancel();
      }
    }, executorService);

    int timeout = retryOptions.getReadPartialRowTimeoutMillis();

    int streamingBufferSize = retryOptions.getStreamingBufferSize();
    int batchRequestSize = streamingBufferSize / 2;
    ResponseQueueReader responseQueueReader =
        new ResponseQueueReader(timeout, streamingBufferSize, batchRequestSize,
            batchRequestSize, readRowsCall);

    StreamingBigtableResultScanner resultScanner =
        new StreamingBigtableResultScanner(reservedChannel, responseQueueReader, cancellationToken);

    clientCallService.asyncServerStreamingCall(
        readRowsCall,
        request,
        batchRequestSize,
        createClientCallListener(resultScanner));

    return resultScanner;
  }

  private ClientCall.Listener<ReadRowsResponse> createClientCallListener(
      final StreamingBigtableResultScanner resultScanner) {
    return new ClientCall.Listener<ReadRowsResponse>() {
      @Override
      public void onMessage(ReadRowsResponse readRowResponse) {
        resultScanner.addResult(readRowResponse);
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        if (status.isOk()) {
          resultScanner.complete();
        } else {
          resultScanner.setError(status.asRuntimeException());
        }
      }
    };
  }
}
