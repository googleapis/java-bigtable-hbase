/*
 * Copyright 2018 Google LLC.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.DefaultRowAdapter;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class implements the {@link IBigtableDataClient} interface and wraps {@link
 * BigtableDataClient} with Google-cloud-java's models.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableDataClientWrapper implements IBigtableDataClient {

  private final BigtableDataClient delegate;
  private final RequestContext requestContext;

  public BigtableDataClientWrapper(
      BigtableDataClient bigtableDataClient, RequestContext requestContext) {
    this.delegate = bigtableDataClient;
    this.requestContext = requestContext;
  }

  /** {@inheritDoc} */
  @Override
  public void mutateRow(RowMutation rowMutation) {
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);
    delegate.mutateRow(mutateRowRequest);
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    ReadModifyWriteRowResponse response =
        delegate.readModifyWriteRow(readModifyWriteRow.toProto(requestContext));
    return new DefaultRowAdapter().createRowFromProto(response.getRow());
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    ListenableFuture<ReadModifyWriteRowResponse> response =
        delegate.readModifyWriteRowAsync(readModifyWriteRow.toProto(requestContext));
    return ApiFutureUtil.transformAndAdapt(
        response,
        new Function<ReadModifyWriteRowResponse, Row>() {
          @Override
          public Row apply(ReadModifyWriteRowResponse response) {
            return new DefaultRowAdapter().createRowFromProto(response.getRow());
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public IBulkMutation createBulkMutationBatcher() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    CheckAndMutateRowResponse response =
        delegate.checkAndMutateRow(conditionalRowMutation.toProto(requestContext));
    return response.getPredicateMatched();
  }

  /** {@inheritDoc} */
  @Override
  public List<KeyOffset> sampleRowKeys(String tableId) {
    String fullTableName =
        NameUtil.formatTableName(
            requestContext.getProjectId(), requestContext.getInstanceId(), tableId);
    SampleRowKeysRequest requestProto =
        SampleRowKeysRequest.newBuilder().setTableName(fullTableName).build();
    List<SampleRowKeysResponse> responseProto = delegate.sampleRowKeys(requestProto);
    ImmutableList.Builder<KeyOffset> keyOffsetBuilder =
        ImmutableList.builderWithExpectedSize(responseProto.size());
    for (SampleRowKeysResponse rowKeys : responseProto) {
      keyOffsetBuilder.add(KeyOffset.create(rowKeys.getRowKey(), rowKeys.getOffsetBytes()));
    }

    return keyOffsetBuilder.build();
  }

  /** {@inheritDoc} */
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
          public List<KeyOffset> apply(@Nonnull List<SampleRowKeysResponse> rowKeysList) {
            if (rowKeysList == null || rowKeysList.isEmpty()) {
              return Collections.EMPTY_LIST;
            }
            ImmutableList.Builder<KeyOffset> keyOffsetBuilder =
                ImmutableList.builderWithExpectedSize(rowKeysList.size());
            for (SampleRowKeysResponse rowKeys : rowKeysList) {
              keyOffsetBuilder.add(KeyOffset.create(rowKeys.getRowKey(), rowKeys.getOffsetBytes()));
            }

            return keyOffsetBuilder.build();
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner<Row> readRows(Query request) {
    final ResultScanner<FlatRow> delegate = readFlatRows(request);
    return new ResultScanner<Row>() {

      @Override
      public Row next() throws IOException {
        return FlatRowConverter.convertToModelRow(delegate.next());
      }

      @Override
      public Row[] next(int count) throws IOException {
        FlatRow[] flatRows = delegate.next(count);
        Row[] rows = new Row[flatRows.length];
        for (int i = 0; i < flatRows.length; i++) {
          rows[i] = FlatRowConverter.convertToModelRow(flatRows[i]);
        }
        return rows;
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public int available() {
        return delegate.available();
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<List<Row>> readRowsAsync(Query request) {
    ListenableFuture<List<FlatRow>> responseProto =
        delegate.readFlatRowsAsync(request.toProto(requestContext));

    return ApiFutureUtil.transformAndAdapt(
        responseProto,
        new Function<List<FlatRow>, List<Row>>() {
          @Override
          public List<Row> apply(List<FlatRow> flatRowList) {
            ImmutableList.Builder<Row> rowBuilder =
                ImmutableList.builderWithExpectedSize(flatRowList.size());
            for (FlatRow flatRow : flatRowList) {
              rowBuilder.add(FlatRowConverter.convertToModelRow(flatRow));
            }

            return rowBuilder.build();
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public List<FlatRow> readFlatRowsList(Query request) {
    return delegate.readFlatRowsList(request.toProto(requestContext));
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner<FlatRow> readFlatRows(Query request) {
    return delegate.readFlatRows(request.toProto(requestContext));
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<List<FlatRow>> readFlatRowsAsync(Query request) {
    return ApiFutureUtil.adapt(delegate.readFlatRowsAsync(request.toProto(requestContext)));
  }

  /** {@inheritDoc} */
  @Override
  public void readFlatRowsAsync(Query request, StreamObserver<FlatRow> observer) {
    delegate.readFlatRows(request.toProto(requestContext), observer);
  }
}
