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
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
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
import com.google.cloud.bigtable.grpc.async.RetryingMutateRowsOperation;
import com.google.cloud.bigtable.grpc.async.RetryingStreamOperation;
import com.google.cloud.bigtable.grpc.async.RetryingUnaryOperation;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.grpc.scanner.ScanHandler;
import com.google.cloud.bigtable.grpc.scanner.RetryingReadRowsOperation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
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
  private static final Predicate<MutateRowsRequest> ARE_RETRYABLE_MUTATIONS =
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
  private static final Predicate<CheckAndMutateRowRequest> IS_RETRYABLE_CHECK_AND_MUTATE =
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

  @VisibleForTesting
  final BigtableAsyncRpc<MutateRowRequest, MutateRowResponse> mutateRowRpc;
  @VisibleForTesting
  final BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> mutateRowsRpc;
  @VisibleForTesting
  final BigtableAsyncRpc<CheckAndMutateRowRequest, CheckAndMutateRowResponse> checkAndMutateRpc;
  private final BigtableAsyncRpc<ReadModifyWriteRowRequest, ReadModifyWriteRowResponse> readWriteModifyRpc;

  /**
   * <p>Constructor for BigtableDataGrpcClient.</p>
   *
   * @param channel a {@link Channel} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param bigtableOptions a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  public BigtableDataGrpcClient(
      Channel channel,
      ScheduledExecutorService retryExecutorService,
      BigtableOptions bigtableOptions) {
    this.retryExecutorService = retryExecutorService;
    this.retryOptions = bigtableOptions.getRetryOptions();

    BigtableAsyncUtilities asyncUtilities = new BigtableAsyncUtilities.Default(channel);
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
      return new Predicate<T>() {
        @Override
        public boolean apply(@Nullable T input) {
          return input != null;
        }
      };
    } else {
      return isRetryableMutation;
    }
  }

  /** {@inheritDoc} */
  @Override
  public MutateRowResponse mutateRow(MutateRowRequest request) {
    return createUnaryListener(request, mutateRowRpc, request.getTableName()).getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<MutateRowResponse> mutateRowAsync(MutateRowRequest request) {
    return createUnaryListener(request, mutateRowRpc, request.getTableName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public List<MutateRowsResponse> mutateRows(MutateRowsRequest request) {
    return createMutateRowsOperation(request).getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<MutateRowsResponse>> mutateRowsAsync(MutateRowsRequest request) {
    return createMutateRowsOperation(request).getAsyncResult();
  }

  private RetryingMutateRowsOperation createMutateRowsOperation(MutateRowsRequest request) {
    CallOptions callOptions = getCallOptions(mutateRowsRpc.getMethodDescriptor(), request);
    Metadata metadata = createMetadata(request.getTableName());
    return new RetryingMutateRowsOperation(
        retryOptions, request, mutateRowsRpc, callOptions, retryExecutorService, metadata);
  }

  /** {@inheritDoc} */
  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request) {
    return createUnaryListener(request, checkAndMutateRpc, request.getTableName())
        .getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    return createUnaryListener(request, checkAndMutateRpc, request.getTableName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ReadModifyWriteRowResponse readModifyWriteRow(ReadModifyWriteRowRequest request) {
    return createUnaryListener(request, readWriteModifyRpc, request.getTableName())
        .getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<ReadModifyWriteRowResponse> readModifyWriteRowAsync(
      ReadModifyWriteRowRequest request) {
    return createUnaryListener(request, readWriteModifyRpc, request.getTableName())
        .getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public List<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    return createStreamingListener(request, sampleRowKeysAsync, request.getTableName())
        .getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    return createStreamingListener(request, sampleRowKeysAsync, request.getTableName())
        .getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<Row>> readRowsAsync(ReadRowsRequest request) {
    return Futures.transform(
        createStreamingListener(request, readRowsAsync, request.getTableName()).getAsyncResult(),
        ROW_LIST_TRANSFORMER);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<FlatRow>> readFlatRowsAsync(ReadRowsRequest request) {
    return Futures.transform(
        createStreamingListener(request, readRowsAsync, request.getTableName()).getAsyncResult(),
        FLAT_ROW_LIST_TRANSFORMER);
  }

  /** {@inheritDoc} */
  @Override
  public List<FlatRow> readFlatRowsList(ReadRowsRequest request) {
    return FLAT_ROW_LIST_TRANSFORMER.apply(
      createStreamingListener(request, readRowsAsync, request.getTableName()).getBlockingResult());
  }

  private <ReqT, RespT> RetryingUnaryOperation<ReqT, RespT> createUnaryListener(
      ReqT request, BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    CallOptions callOptions = getCallOptions(rpc.getMethodDescriptor(), request);
    Metadata metadata = createMetadata(tableName);
    return new RetryingUnaryOperation<>(
        retryOptions, request, rpc, callOptions, retryExecutorService, metadata);
  }

  private <ReqT, RespT> RetryingStreamOperation<ReqT, RespT> createStreamingListener(
      ReqT request, BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    CallOptions callOptions = getCallOptions(rpc.getMethodDescriptor(), request);
    Metadata metadata = createMetadata(tableName);
    return new RetryingStreamOperation<>(
        retryOptions, request, rpc, callOptions, retryExecutorService, metadata);
  }

  private <ReqT> CallOptions getCallOptions(final MethodDescriptor<ReqT, ?> methodDescriptor,
      ReqT request) {
    return callOptionsFactory.create(methodDescriptor, request);
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
    final ResponseQueueReader reader = new ResponseQueueReader();
    RetryingReadRowsOperation operation = createReadRowsRetryListener(request, reader);
    operation.setResultObserver(new StreamObserver<ReadRowsResponse>(){
      @Override
      public void onNext(ReadRowsResponse value) {
        reader.addRequestResultMarker();
      }
      @Override public void onError(Throwable t) {}
      @Override public void onCompleted() {}
    });

    // Start the operation.
    operation.getAsyncResult();

    return new ResumingStreamingResultScanner(reader, operation);
  }

  /** {@inheritDoc} */
  @Override
  public ScanHandler readFlatRows(ReadRowsRequest request, StreamObserver<FlatRow> observer) {
    RetryingReadRowsOperation operation = createReadRowsRetryListener(request, observer);

    // Start the operation.
    operation.getAsyncResult();

    return operation;
  }

  private RetryingReadRowsOperation createReadRowsRetryListener(ReadRowsRequest request,
      StreamObserver<FlatRow> observer) {
    return new RetryingReadRowsOperation(
        observer,
        retryOptions,
        request,
        readRowsAsync,
        getCallOptions(readRowsAsync.getMethodDescriptor(), request),
        retryExecutorService,
        createMetadata(request.getTableName()));
  }
}