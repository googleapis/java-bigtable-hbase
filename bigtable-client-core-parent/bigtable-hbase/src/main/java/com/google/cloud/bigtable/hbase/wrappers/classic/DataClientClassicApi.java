/*
 * Copyright 2020 Google LLC.
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
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.DefaultRowAdapter;
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
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;

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
            // TODO(rahulkql): should we create RowAdapter for proto Row to hbase's Result?
            return Adapters.ROW_ADAPTER.adaptResponse(
                new DefaultRowAdapter().createRowFromProto(response.getRow()));
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
            if (rowKeysList == null || rowKeysList.isEmpty()) {
              return Collections.emptyList();
            }
            ImmutableList.Builder<KeyOffset> keyOffsetBuilder = ImmutableList.builder();
            for (SampleRowKeysResponse rowKeys : rowKeysList) {
              keyOffsetBuilder.add(KeyOffset.create(rowKeys.getRowKey(), rowKeys.getOffsetBytes()));
            }

            return keyOffsetBuilder.build();
          }
        });
  }

  @Override
  public ResultScanner<Result> readRows(Query request) {
    final ResultScanner<FlatRow> rs = delegate.readFlatRows(request.toProto(requestContext));
    return new ResultScanner<Result>() {

      @Override
      public int available() {
        return rs.available();
      }

      @Override
      public Result next() throws IOException {
        return Adapters.FLAT_ROW_ADAPTER.adaptResponse(rs.next());
      }

      @Override
      public Result[] next(int count) throws IOException {
        FlatRow[] flatRows = rs.next(count);
        Result[] results = new Result[flatRows.length];
        for (int i = 0; i < flatRows.length; i++) {
          results[i] = Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRows[i]);
        }
        return results;
      }

      @Override
      public void close() throws IOException {
        rs.close();
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
}
