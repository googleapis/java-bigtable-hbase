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

import static com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor.GRPC_RESOURCE_PREFIX_KEY;
import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncUtilities;
import com.google.cloud.bigtable.grpc.async.RetryingCollectingClientCallListener;
import com.google.cloud.bigtable.grpc.async.RetryingUnaryRpcCallListener;
import com.google.cloud.bigtable.grpc.async.AbstractRetryingRpcListener;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.grpc.scanner.ScanHandler;
import com.google.cloud.bigtable.grpc.scanner.ReadRowsRetryListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A gRPC client to access the v2 Bigtable data service.
 * <p>
 * In addition to calling the underlying data service API via grpc, this class adds retry logic and
 * some useful headers.
 * <p>
 * Most of the methods are unary (single response). The only exception is ReadRows which is a
 * streaming call.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableDataGrpcClient implements BigtableDataClient {

  // Retryable Predicates
  /** Constant <code>IS_RETRYABLE_MUTATION</code> */
  @VisibleForTesting
  public static final Predicate<MutateRowRequest> IS_RETRYABLE_MUTATION =
      new Predicate<MutateRowRequest>() {
        @Override
        public boolean apply(MutateRowRequest mutateRowRequest) {
          return mutateRowRequest != null
              && allCellsHaveTimestamps(mutateRowRequest.getMutationsList());
        }
      };

  /** Constant <code>ARE_RETRYABLE_MUTATIONS</code> */
  @VisibleForTesting
  public static final Predicate<MutateRowsRequest> ARE_RETRYABLE_MUTATIONS =
      new Predicate<MutateRowsRequest>() {
        @Override
        public boolean apply(MutateRowsRequest mutateRowsRequest) {
          if (mutateRowsRequest == null) {
            return false;
          }
          for (Entry entry : mutateRowsRequest.getEntriesList()) {
            if (!allCellsHaveTimestamps(entry.getMutationsList())) {
              return false;
            }
          }
          return true;
        }
      };

  /** Constant <code>IS_RETRYABLE_CHECK_AND_MUTATE</code> */
  @VisibleForTesting
  public static final Predicate<CheckAndMutateRowRequest> IS_RETRYABLE_CHECK_AND_MUTATE =
      new Predicate<CheckAndMutateRowRequest>() {
        @Override
        public boolean apply(CheckAndMutateRowRequest checkAndMutateRowRequest) {
          return checkAndMutateRowRequest != null
              && allCellsHaveTimestamps(checkAndMutateRowRequest.getTrueMutationsList())
              && allCellsHaveTimestamps(checkAndMutateRowRequest.getFalseMutationsList());
        }
      };

  private static final boolean allCellsHaveTimestamps(Iterable<Mutation> mutations) {
    for (Mutation mut : mutations) {
      if (mut.getSetCell().getTimestampMicros() <= 0) {
        return false;
      }
    }
    return true;
  }

  // Streaming API transformers
  private static Function<FlatRow, Row> FLAT_ROW_TRANSFORMER = new Function<FlatRow, Row>(){
    @Override
    public Row apply(FlatRow input) {
      return FlatRowConverter.convert(input);
    }
  };

  private static Function<List<ReadRowsResponse>, List<FlatRow>> FLAT_ROW_LIST_TRANSFORMER =
      new Function<List<ReadRowsResponse>, List<FlatRow>>() {
        @Override
        public List<FlatRow> apply(List<ReadRowsResponse> responses) {
          return RowMerger.toRows(responses);
        }
      };

  private static Function<List<ReadRowsResponse>, List<Row>> ROW_LIST_TRANSFORMER =
    new Function<List<ReadRowsResponse>, List<Row>>() {
      @Override
      public List<Row> apply(List<ReadRowsResponse> responses) {
        return Lists.transform(RowMerger.toRows(responses), FLAT_ROW_TRANSFORMER);
      }
    };
      
  // Member variables
  private final ScheduledExecutorService retryExecutorService;
  private final RetryOptions retryOptions;

  private CallOptionsFactory callOptionsFactory = new CallOptionsFactory.Default();

  private final BigtableAsyncRpc<SampleRowKeysRequest, SampleRowKeysResponse> sampleRowKeysAsync;
  private final BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> readRowsAsync;

  private final BigtableAsyncRpc<MutateRowRequest, MutateRowResponse> mutateRowRpc;
  private final BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> mutateRowsRpc;
  private final BigtableAsyncRpc<CheckAndMutateRowRequest, CheckAndMutateRowResponse> checkAndMutateRpc;
  private final BigtableAsyncRpc<ReadModifyWriteRowRequest, ReadModifyWriteRowResponse> readWriteModifyRpc;

  /**
   * <p>Constructor for BigtableDataGrpcClient.</p>
   *
   * @param channelPool a {@link com.google.cloud.bigtable.grpc.io.ChannelPool} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param bigtableOptions a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  public BigtableDataGrpcClient(
      ChannelPool channelPool,
      ScheduledExecutorService retryExecutorService,
      BigtableOptions bigtableOptions) {
    this(
        retryExecutorService,
        bigtableOptions,
        new BigtableAsyncUtilities.Default(channelPool));
  }

  @VisibleForTesting
  BigtableDataGrpcClient(
      ScheduledExecutorService retryExecutorService,
      BigtableOptions bigtableOptions,
      BigtableAsyncUtilities asyncUtilities) {
    this.retryExecutorService = retryExecutorService;
    this.retryOptions = bigtableOptions.getRetryOptions();

    this.sampleRowKeysAsync =
        asyncUtilities.createAsyncRpc(
             BigtableGrpc.METHOD_SAMPLE_ROW_KEYS,
             Predicates.<SampleRowKeysRequest> alwaysTrue());
    this.readRowsAsync =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.METHOD_READ_ROWS,
            Predicates.<ReadRowsRequest> alwaysTrue());
    this.mutateRowRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.METHOD_MUTATE_ROW,
            getMutationRetryableFunction(IS_RETRYABLE_MUTATION));
    this.mutateRowsRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.METHOD_MUTATE_ROWS,
            getMutationRetryableFunction(ARE_RETRYABLE_MUTATIONS));
    this.checkAndMutateRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.METHOD_CHECK_AND_MUTATE_ROW,
            getMutationRetryableFunction(IS_RETRYABLE_CHECK_AND_MUTATE));
    this.readWriteModifyRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.METHOD_READ_MODIFY_WRITE_ROW,
            Predicates.<ReadModifyWriteRowRequest> alwaysFalse());
  }

  /** {@inheritDoc} */
  @Override
  public void setCallOptionsFactory(CallOptionsFactory callOptionsFactory) {
    this.callOptionsFactory = callOptionsFactory;
  }

  private <T> Predicate<T> getMutationRetryableFunction(Predicate<T> isRetryableMutation) {
    if (retryOptions.allowRetriesWithoutTimestamp()) {
      return Predicates.<T> alwaysTrue();
    } else {
      return isRetryableMutation;
    }
  }

  /** {@inheritDoc} */
  @Override
  public MutateRowResponse mutateRow(MutateRowRequest request) {
    return getBlockingUnaryResult(request, mutateRowRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<MutateRowResponse> mutateRowAsync(MutateRowRequest request) {
    return getUnaryFuture(request, mutateRowRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public List<MutateRowsResponse> mutateRows(MutateRowsRequest request) {
    return getBlockingStreamingResult(request, mutateRowsRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<MutateRowsResponse>> mutateRowsAsync(MutateRowsRequest request) {
    return getStreamingFuture(request, mutateRowsRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request) {
    return getBlockingUnaryResult(request, checkAndMutateRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return getUnaryFuture(request, checkAndMutateRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ReadModifyWriteRowResponse readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return getBlockingUnaryResult(request, readWriteModifyRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<ReadModifyWriteRowResponse> readModifyWriteRowAsync(
      ReadModifyWriteRowRequest request) {
    return getUnaryFuture(request, readWriteModifyRpc, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return ImmutableList.copyOf(
        getBlockingStreamingResult(request, sampleRowKeysAsync, request.getTableName()));
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    return getStreamingFuture(request, sampleRowKeysAsync, request.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<Row>> readRowsAsync(ReadRowsRequest request) {
    return Futures.transform(getStreamingFuture(request, readRowsAsync, request.getTableName()),
      ROW_LIST_TRANSFORMER);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<FlatRow>> readFlatRowsAsync(ReadRowsRequest request) {
    return Futures.transform(getStreamingFuture(request, readRowsAsync, request.getTableName()),
      FLAT_ROW_LIST_TRANSFORMER);
  }
  
  // Helper methods
  /**
   * <p>getStreamingFuture.</p>
   *
   * @param request a ReqT object.
   * @param rpc a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc} object.
   * @param tableName a {@link java.lang.String} object.
   * @param <ReqT> a ReqT object.
   * @param <RespT> a RespT object.
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} object.
   */
  protected <ReqT, RespT> ListenableFuture<List<RespT>> getStreamingFuture(ReqT request,
      BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    return getCompletionFuture(createStreamingListener(request, rpc, tableName));
  }

  private <ReqT, RespT> List<RespT> getBlockingStreamingResult(ReqT request,
      BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    return getBlockingResult(createStreamingListener(request, rpc, tableName));
  }

  private <ReqT, RespT> ListenableFuture<RespT> getUnaryFuture(ReqT request,
      BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    return getCompletionFuture(createUnaryListener(request, rpc, tableName));
  }

  private <ReqT, RespT> RespT getBlockingUnaryResult(ReqT request,
      BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    return getBlockingResult(createUnaryListener(request, rpc, tableName));
  }

  private <ReqT, RespT> RetryingUnaryRpcCallListener<ReqT, RespT> createUnaryListener(ReqT request,
      BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    CallOptions callOptions = getCallOptions(rpc.getMethodDescriptor(), request);
    return new RetryingUnaryRpcCallListener<>(retryOptions, request, rpc, callOptions,
        retryExecutorService, createMetadata(tableName));
  }

  private <ReqT, RespT> RetryingCollectingClientCallListener<ReqT, RespT>
      createStreamingListener(ReqT request, BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    CallOptions callOptions = getCallOptions(rpc.getMethodDescriptor(), request);
    return new RetryingCollectingClientCallListener<>(retryOptions, request, rpc, callOptions,
        retryExecutorService, createMetadata(tableName));
  }

  private <ReqT> CallOptions getCallOptions(final MethodDescriptor<ReqT, ?> methodDescriptor,
      ReqT request) {
    return callOptionsFactory.create(methodDescriptor, request);
  }

  private static <ReqT, RespT, OutputT> ListenableFuture<OutputT>
      getCompletionFuture(AbstractRetryingRpcListener<ReqT, RespT, OutputT> listener) {
    listener.start();
    return listener.getCompletionFuture();
  }

  private static <ReqT, RespT, OutputT> OutputT getBlockingResult(
      AbstractRetryingRpcListener<ReqT, RespT, OutputT> listener) {
    try {
      listener.start();
      return listener.getCompletionFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      listener.cancel();
      throw Status.CANCELLED.withCause(e).asRuntimeException();
    } catch (ExecutionException e) {
      listener.cancel();
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  /**
   * Creates a {@link Metadata} that contains pertinent headers.
   */
  private Metadata createMetadata(String tableName) {
    Metadata metadata = new Metadata();
    if (tableName != null) {
      metadata.put(GRPC_RESOURCE_PREFIX_KEY, tableName);
    }
    return metadata;
  }

  // Scanner methods

  /** {@inheritDoc} */
  @Override
  public ResultScanner<Row> readRows(ReadRowsRequest request) {
    // Delegate all resumable operations to the scanner. It will request a non-resumable scanner
    // during operation.
    // TODO(sduskis): Figure out a way to perform operation level metrics with the
    // AbstractBigtableResultScanner implementations.
    final ResultScanner<FlatRow> delegate = readFlatRows(request);
    return new ResultScanner<Row>() {

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public Row[] next(int count) throws IOException {
        FlatRow[] flatRows = delegate.next(count);
        Row[] rows = new Row[flatRows.length];
        for (int i = 0; i < flatRows.length; i++) {
          rows[i] = FlatRowConverter.convert(flatRows[i]);
        }
        return rows;
      }

      @Override
      public Row next() throws IOException {
        return FlatRowConverter.convert(delegate.next());
      }

      @Override
      public int available() {
        return delegate.available();
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner<FlatRow> readFlatRows(ReadRowsRequest request) {
    // Delegate all resumable operations to the scanner. It will request a non-resumable scanner
    // during operation.
    ResponseQueueReader reader = new ResponseQueueReader(retryOptions.getStreamingBufferSize());
    final ReadRowsRetryListener listener = createReadRowsRetryListener(request, reader);
    return new ResumingStreamingResultScanner(reader, listener);
  }

  /** {@inheritDoc} */
  @Override
  public ScanHandler readFlatRows(ReadRowsRequest request, StreamObserver<FlatRow> observer) {
    return createReadRowsRetryListener(request, observer);
  }

  private ReadRowsRetryListener createReadRowsRetryListener(ReadRowsRequest request,
      StreamObserver<FlatRow> observer) {
    ReadRowsRetryListener listener =
      new ReadRowsRetryListener(
          observer,
          retryOptions,
          request,
          readRowsAsync,
          getCallOptions(readRowsAsync.getMethodDescriptor(), request),
          retryExecutorService,
          createMetadata(request.getTableName()));
    listener.start();
    return listener;
  }
}