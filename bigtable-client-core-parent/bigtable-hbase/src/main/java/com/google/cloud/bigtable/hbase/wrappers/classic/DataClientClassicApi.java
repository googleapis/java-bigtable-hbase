/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class DataClientClassicApi implements DataClientWrapper {

  private final BigtableSession session;
  private final BigtableDataClient delegate;
  private final RequestContext requestContext;

  DataClientClassicApi(BigtableSession session, RequestContext requestContext) {
    this.session = session;
    this.delegate = session.getDataClient();
    this.requestContext = requestContext;
  }

  @Override
  public BulkMutationWrapper createBulkMutation(String tableId) {
    BigtableTableName tableName =
        new BigtableTableName(
            NameUtil.formatTableName(
                requestContext.getProjectId(), requestContext.getInstanceId(), tableId));
    return new BulkMutationClassicApi(session.createBulkMutation(tableName));
  }

  @Override
  public BulkReadWrapper createBulkRead(String tableId) {
    BigtableTableName tableName =
        new BigtableTableName(
            NameUtil.formatTableName(
                requestContext.getProjectId(), requestContext.getInstanceId(), tableId));
    return new BulkReadClassicApi(session.createBulkRead(tableName));
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    ListenableFuture<MutateRowResponse> response =
        delegate.mutateRowAsync(rowMutation.toProto(requestContext));
    return ApiFutureUtil.transformAndAdapt(
        response,
        new Function<MutateRowResponse, Void>() {
          @Nullable
          @Override
          public Void apply(@Nullable MutateRowResponse mutateRowResponse) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<Result> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    ListenableFuture<ReadModifyWriteRowResponse> response =
        delegate.readModifyWriteRowAsync(readModifyWriteRow.toProto(requestContext));
    return ApiFutureUtil.transformAndAdapt(
        response,
        new Function<ReadModifyWriteRowResponse, Result>() {
          @Override
          public Result apply(ReadModifyWriteRowResponse response) {
            return transformRowToResult(response.getRow());
          }
        });
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    final CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    final ListenableFuture<CheckAndMutateRowResponse> response =
        delegate.checkAndMutateRowAsync(request);
    return ApiFutureUtil.transformAndAdapt(
        response,
        new Function<CheckAndMutateRowResponse, Boolean>() {
          @Override
          public Boolean apply(CheckAndMutateRowResponse checkAndMutateRowResponse) {
            return checkAndMutateRowResponse.getPredicateMatched();
          }
        });
  }

  @Override
  public ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId) {
    String fullTableName =
        NameUtil.formatTableName(
            requestContext.getProjectId(), requestContext.getInstanceId(), tableId);
    SampleRowKeysRequest requestProto =
        SampleRowKeysRequest.newBuilder().setTableName(fullTableName).build();
    ListenableFuture<List<SampleRowKeysResponse>> responseProto =
        delegate.sampleRowKeysAsync(requestProto);

    return ApiFutureUtil.transformAndAdapt(
        responseProto,
        new Function<List<SampleRowKeysResponse>, List<KeyOffset>>() {
          @Override
          public List<KeyOffset> apply(List<SampleRowKeysResponse> rowKeysList) {
            ImmutableList.Builder<KeyOffset> keyOffsetBuilder = ImmutableList.builder();
            for (SampleRowKeysResponse rowKeys : rowKeysList) {
              keyOffsetBuilder.add(KeyOffset.create(rowKeys.getRowKey(), rowKeys.getOffsetBytes()));
            }

            return keyOffsetBuilder.build();
          }
        });
  }

  @Override
  public ApiFuture<Result> readRowAsync(String tableId, ByteString rowKey, Filters.Filter filter) {
    Query request = Query.create(tableId).rowKey(rowKey).limit(1L);
    if (filter != null) {
      request.filter(filter);
    }

    return ApiFutureUtil.transformAndAdapt(
        delegate.readFlatRowsAsync(request.toProto(requestContext)),
        new Function<List<FlatRow>, Result>() {
          @Override
          public Result apply(List<FlatRow> flatRows) {
            if (flatRows.isEmpty()) {
              return Result.EMPTY_RESULT;
            }
            return Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRows.get(0));
          }
        });
  }

  @Override
  public org.apache.hadoop.hbase.client.ResultScanner readRows(Query request) {
    final ResultScanner<FlatRow> bigtableResultScanner =
        delegate.readFlatRows(request.toProto(requestContext));

    return new AbstractClientScanner() {
      @Override
      public Result next() throws IOException {
        FlatRow flatRow = bigtableResultScanner.next();
        if (flatRow == null) {
          // Null signals EOF.
          return null;
        }
        return Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow);
      }

      @Override
      public void close() {
        try {
          bigtableResultScanner.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      // This is copied over form BigtableResultScannerAdapter.
      /**
       * This is an HBase concept that was added in HBase 1.0.2. It's not relevant for Cloud
       * Bigtable. It will not be called from the HBase code and should not be called by the user.
       */
      // Developers Note: Do not add @Override so that this can remain backwards compatible with
      // 1.0.1.
      public boolean renewLease() {
        throw new UnsupportedOperationException("renewLease");
      }
    };
  }

  @Override
  public ApiFuture<List<Result>> readRowsAsync(Query request) {
    return ApiFutures.transform(
        ApiFutureUtil.adapt(delegate.readFlatRowsAsync(request.toProto(requestContext))),
        new ApiFunction<List<FlatRow>, List<Result>>() {
          @Override
          public List<Result> apply(List<FlatRow> rows) {
            ImmutableList.Builder<Result> resultList = ImmutableList.builder();
            for (FlatRow flatRow : rows) {
              resultList.add(Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow));
            }
            return resultList.build();
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public void readRowsAsync(Query request, final StreamObserver<Result> observer) {
    delegate.readFlatRows(
        request.toProto(requestContext),
        new StreamObserver<FlatRow>() {
          @Override
          public void onNext(FlatRow flatRow) {
            observer.onNext(Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow));
          }

          @Override
          public void onError(Throwable throwable) {
            observer.onError(throwable);
          }

          @Override
          public void onCompleted() {
            observer.onCompleted();
          }
        });
  }

  @Override
  public void close() throws Exception {
    session.close();
  }

  private static Result transformRowToResult(Row row) {
    List<org.apache.hadoop.hbase.Cell> hbaseCells = new ArrayList<>();
    byte[] rowKeyBytes = ByteStringer.extract(row.getKey());

    for (Family family : row.getFamiliesList()) {
      byte[] familyBytes = ByteStringer.extract(family.getNameBytes());

      for (Column column : family.getColumnsList()) {
        byte[] qualifierBytes = ByteStringer.extract(column.getQualifier());

        for (Cell cell : column.getCellsList()) {
          hbaseCells.add(toRowCell(rowKeyBytes, familyBytes, qualifierBytes, cell));
        }
      }
    }
    return Result.create(hbaseCells);
  }

  private static RowCell toRowCell(byte[] rowKey, byte[] family, byte[] qualifier, Cell cell) {
    return new RowCell(
        rowKey,
        family,
        qualifier,
        TimestampConverter.bigtable2hbase(cell.getTimestampMicros()),
        ByteStringer.extract(cell.getValue()),
        cell.getLabelsList());
  }
}
