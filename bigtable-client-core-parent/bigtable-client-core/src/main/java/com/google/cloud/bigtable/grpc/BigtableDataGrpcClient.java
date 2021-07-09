/*
 * Copyright 2015 Google LLC
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

import com.google.api.core.ApiClock;
import com.google.api.core.InternalApi;
import com.google.api.core.NanoClock;
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
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncUtilities;
import com.google.cloud.bigtable.grpc.async.RetryingMutateRowsOperation;
import com.google.cloud.bigtable.grpc.async.RetryingStreamOperation;
import com.google.cloud.bigtable.grpc.async.RetryingUnaryOperation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResponseQueueReader;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RetryingReadRowsOperation;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.grpc.scanner.ScanHandler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * A gRPC client to access the v2 Bigtable data service.
 *
 * <p>In addition to calling the underlying data service API via grpc, this class adds retry logic
 * and some useful headers.
 *
 * <p>Most of the methods are unary (single response). The only exception is ReadRows which is a
 * streaming call.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableDataGrpcClient implements BigtableDataClient {

  private static final ApiClock CLOCK = NanoClock.getDefaultClock();

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

  private static final boolean allCellsHaveTimestamps(Iterable<Mutation> mutations) {
    for (Mutation mut : mutations) {
      if (mut.getSetCell().getTimestampMicros() <= 0) {
        return false;
      }
    }
    return true;
  }

  // Streaming API transformers
  private static Function<FlatRow, Row> FLAT_ROW_TRANSFORMER =
      new Function<FlatRow, Row>() {
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
  private final String clientDefaultAppProfileId;
  private final ScheduledExecutorService retryExecutorService;
  private final RetryOptions retryOptions;

  private DeadlineGeneratorFactory deadlineGeneratorFactory = DeadlineGeneratorFactory.DEFAULT;

  private final BigtableAsyncRpc<SampleRowKeysRequest, SampleRowKeysResponse> sampleRowKeysAsync;
  private final BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> readRowsAsync;

  @VisibleForTesting final BigtableAsyncRpc<MutateRowRequest, MutateRowResponse> mutateRowRpc;
  @VisibleForTesting final BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> mutateRowsRpc;

  @VisibleForTesting
  final BigtableAsyncRpc<CheckAndMutateRowRequest, CheckAndMutateRowResponse> checkAndMutateRpc;

  private final BigtableAsyncRpc<ReadModifyWriteRowRequest, ReadModifyWriteRowResponse>
      readWriteModifyRpc;

  /**
   * Constructor for BigtableDataGrpcClient.
   *
   * @param channel a {@link Channel} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param bigtableOptions a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  public BigtableDataGrpcClient(
      Channel channel,
      ScheduledExecutorService retryExecutorService,
      BigtableOptions bigtableOptions) {

    try {
      BigtableGrpc.getReadRowsMethod();
    } catch (NoSuchMethodError e) {
      throw new RuntimeException(
          "Please make sure that you are using "
              + "grpc-google-cloud-bigtable-v2 > 0.11.0 and bigtable-protos is not on your classpath");
    }

    this.clientDefaultAppProfileId = bigtableOptions.getAppProfileId();
    this.retryExecutorService = retryExecutorService;
    this.retryOptions = bigtableOptions.getRetryOptions();

    BigtableAsyncUtilities asyncUtilities = new BigtableAsyncUtilities.Default(channel);
    this.sampleRowKeysAsync =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.getSampleRowKeysMethod(), Predicates.<SampleRowKeysRequest>alwaysTrue());
    this.readRowsAsync =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.getReadRowsMethod(), Predicates.<ReadRowsRequest>alwaysTrue());
    this.mutateRowRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.getMutateRowMethod(), getMutationRetryableFunction(IS_RETRYABLE_MUTATION));
    this.mutateRowsRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.getMutateRowsMethod(),
            getMutationRetryableFunction(ARE_RETRYABLE_MUTATIONS));
    this.checkAndMutateRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.getCheckAndMutateRowMethod(),
            getMutationRetryableFunction(Predicates.<CheckAndMutateRowRequest>alwaysFalse()));
    this.readWriteModifyRpc =
        asyncUtilities.createAsyncRpc(
            BigtableGrpc.getReadModifyWriteRowMethod(),
            Predicates.<ReadModifyWriteRowRequest>alwaysFalse());
  }

  /** {@inheritDoc} */
  @Override
  public void setDeadlineGeneratorFactory(DeadlineGeneratorFactory deadlineGeneratorFactory) {
    this.deadlineGeneratorFactory = deadlineGeneratorFactory;
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
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    return createUnaryListener(request, mutateRowRpc, request.getTableName()).getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<MutateRowResponse> mutateRowAsync(MutateRowRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    return createUnaryListener(request, mutateRowRpc, request.getTableName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public List<MutateRowsResponse> mutateRows(MutateRowsRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    return createMutateRowsOperation(request).getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<MutateRowsResponse>> mutateRowsAsync(MutateRowsRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    return createMutateRowsOperation(request).getAsyncResult();
  }

  private RetryingMutateRowsOperation createMutateRowsOperation(MutateRowsRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    DeadlineGenerator deadlineGenerator =
        deadlineGeneratorFactory.getRequestDeadlineGenerator(
            request, mutateRowsRpc.isRetryable(request));
    Metadata metadata = createMetadata(request.getTableName());
    return new RetryingMutateRowsOperation(
        retryOptions,
        request,
        mutateRowsRpc,
        deadlineGenerator,
        retryExecutorService,
        metadata,
        CLOCK);
  }

  /** {@inheritDoc} */
  @Override
  public CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    return createUnaryListener(request, checkAndMutateRpc, request.getTableName())
        .getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    return createUnaryListener(request, checkAndMutateRpc, request.getTableName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ReadModifyWriteRowResponse readModifyWriteRow(ReadModifyWriteRowRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    return createUnaryListener(request, readWriteModifyRpc, request.getTableName())
        .getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<ReadModifyWriteRowResponse> readModifyWriteRowAsync(
      ReadModifyWriteRowRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    return createUnaryListener(request, readWriteModifyRpc, request.getTableName())
        .getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public List<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    return createStreamingListener(request, sampleRowKeysAsync, request.getTableName())
        .getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    return createStreamingListener(request, sampleRowKeysAsync, request.getTableName())
        .getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<Row>> readRowsAsync(ReadRowsRequest request) {
    return Futures.transform(
        readFlatRowsAsync(request),
        new Function<List<FlatRow>, List<Row>>() {
          @Override
          public List<Row> apply(List<FlatRow> input) {
            return Lists.transform(input, FLAT_ROW_TRANSFORMER);
          }
        },
        MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<FlatRow>> readFlatRowsAsync(ReadRowsRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    final StreamCollector<FlatRow> rowCollector = new StreamCollector<>();
    RetryingReadRowsOperation operation = createReadRowsRetryListener(request, rowCollector);

    return Futures.transform(
        operation.getAsyncResult(),
        new Function<String, List<FlatRow>>() {
          @Override
          public List<FlatRow> apply(String ignored) {
            return rowCollector.getResults();
          }
        },
        MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public List<FlatRow> readFlatRowsList(ReadRowsRequest request) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }
    final StreamCollector<FlatRow> rowCollector = new StreamCollector<>();
    RetryingReadRowsOperation operation = createReadRowsRetryListener(request, rowCollector);
    operation.getBlockingResult();
    return rowCollector.getResults();
  }

  private <ReqT, RespT> RetryingUnaryOperation<ReqT, RespT> createUnaryListener(
      ReqT request, BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    DeadlineGenerator deadlineGenerator =
        deadlineGeneratorFactory.getRequestDeadlineGenerator(request, rpc.isRetryable(request));
    Metadata metadata = createMetadata(tableName);
    return new RetryingUnaryOperation<>(
        retryOptions, request, rpc, deadlineGenerator, retryExecutorService, metadata, CLOCK);
  }

  private <ReqT, RespT> RetryingStreamOperation<ReqT, RespT> createStreamingListener(
      ReqT request, BigtableAsyncRpc<ReqT, RespT> rpc, String tableName) {
    DeadlineGenerator deadlineGenerator =
        deadlineGeneratorFactory.getRequestDeadlineGenerator(request, rpc.isRetryable(request));
    Metadata metadata = createMetadata(tableName);
    return new RetryingStreamOperation<>(
        retryOptions, request, rpc, deadlineGenerator, retryExecutorService, metadata, CLOCK);
  }

  /** Creates a {@link Metadata} that contains pertinent headers. */
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
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

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
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    // Delegate all resumable operations to the scanner. It will request a non-resumable scanner
    // during operation.
    final ResponseQueueReader reader = new ResponseQueueReader();
    RetryingReadRowsOperation operation = createReadRowsRetryListener(request, reader);
    operation.setResultObserver(
        new StreamObserver<ReadRowsResponse>() {
          @Override
          public void onNext(ReadRowsResponse value) {
            reader.addRequestResultMarker();
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        });

    // Start the operation.
    operation.getAsyncResult();

    return new ResumingStreamingResultScanner(reader);
  }

  /** {@inheritDoc} */
  @Override
  public ScanHandler readFlatRows(ReadRowsRequest request, StreamObserver<FlatRow> observer) {
    if (shouldOverrideAppProfile(request.getAppProfileId())) {
      request = request.toBuilder().setAppProfileId(clientDefaultAppProfileId).build();
    }

    RetryingReadRowsOperation operation = createReadRowsRetryListener(request, observer);

    // Start the operation.
    operation.getAsyncResult();

    return operation;
  }

  private RetryingReadRowsOperation createReadRowsRetryListener(
      ReadRowsRequest request, StreamObserver<FlatRow> observer) {
    return new RetryingReadRowsOperation(
        observer,
        retryOptions,
        request,
        readRowsAsync,
        deadlineGeneratorFactory.getRequestDeadlineGenerator(
            request, readRowsAsync.isRetryable(request)),
        retryExecutorService,
        createMetadata(request.getTableName()),
        CLOCK);
  }

  private boolean shouldOverrideAppProfile(String requestProfile) {
    return !this.clientDefaultAppProfileId.isEmpty() && requestProfile.isEmpty();
  }

  /**
   * Helper to buffer rows from a RetryingReadRowsOperation.
   *
   * <p>Each row will be buffered in a list.
   *
   * <p>This class assumes that the {@link StreamObserver} methods will be called by a single gRPC
   * thread at a time.
   */
  private static class StreamCollector<T> implements StreamObserver<T> {
    private enum State {
      BUFFERING,
      OK,
      ERROR
    };

    private final ImmutableList.Builder<T> results = ImmutableList.builder();
    private final AtomicReference<State> state = new AtomicReference(State.BUFFERING);

    @Override
    public void onNext(T item) {
      results.add(item);
    }

    @Override
    public void onError(Throwable ignored) {
      // note: we dont need to save the error here because it will be bubbled (possibly wrapped)
      // by the Operation's getAsyncResult. This check is mainly a sanity check to ensure that the
      // caller doesn't ignore that error and try to fetch partial results.
      state.set(State.ERROR);
    }

    @Override
    public void onCompleted() {
      state.compareAndSet(State.BUFFERING, State.OK);
    }

    public List<T> getResults() {
      Preconditions.checkState(
          state.get() == State.OK,
          "Unexpected state, stream must be complete before fetching the results");
      return results.build();
    }
  }
}
