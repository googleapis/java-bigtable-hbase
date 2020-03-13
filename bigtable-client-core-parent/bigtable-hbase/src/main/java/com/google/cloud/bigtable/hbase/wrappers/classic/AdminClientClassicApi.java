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

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.List;
import javax.annotation.Nonnull;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class AdminClientClassicApi implements AdminClientWrapper {

  private final BigtableTableAdminClient delegate;
  private final BigtableInstanceName instanceName;

  AdminClientClassicApi(
      @Nonnull BigtableTableAdminClient delegate, @Nonnull BigtableInstanceName instanceName) {
    this.delegate = delegate;
    this.instanceName = instanceName;
  }

  @Override
  public ApiFuture<Table> createTableAsync(CreateTableRequest request) {
    com.google.bigtable.admin.v2.CreateTableRequest requestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return ApiFutureUtil.transformAndAdapt(
        delegate.createTableAsync(requestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        });
  }

  @Override
  public ApiFuture<Table> getTableAsync(String tableId) {
    GetTableRequest requestProto =
        GetTableRequest.newBuilder().setName(instanceName.toTableNameStr(tableId)).build();

    return ApiFutureUtil.transformAndAdapt(
        delegate.getTableAsync(requestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        });
  }

  @Override
  public ApiFuture<List<String>> listTablesAsync() {
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(instanceName.toString()).build();
    ListenableFuture<ListTablesResponse> response = delegate.listTablesAsync(request);

    return ApiFutureUtil.transformAndAdapt(
        response,
        new Function<ListTablesResponse, List<String>>() {
          @Override
          public List<String> apply(ListTablesResponse input) {
            ImmutableList.Builder<String> tableIdsBuilder = ImmutableList.builder();
            for (com.google.bigtable.admin.v2.Table tableProto : input.getTablesList()) {
              tableIdsBuilder.add(instanceName.toTableId(tableProto.getName()));
            }

            return tableIdsBuilder.build();
          }
        });
  }

  @Override
  public ApiFuture<Void> deleteTableAsync(String tableId) {
    DeleteTableRequest request =
        DeleteTableRequest.newBuilder().setName(instanceName.toTableNameStr(tableId)).build();

    return ApiFutureUtil.transformAndAdapt(
        delegate.deleteTableAsync(request),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request) {
    com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest modifyColumnRequestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return ApiFutureUtil.transformAndAdapt(
        delegate.modifyColumnFamilyAsync(modifyColumnRequestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        });
  }

  @Override
  public ApiFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix) {
    Preconditions.checkNotNull(rowKeyPrefix);
    DropRowRangeRequest protoRequest =
        DropRowRangeRequest.newBuilder()
            .setName(instanceName.toTableNameStr(tableId))
            .setDeleteAllDataFromTable(false)
            .setRowKeyPrefix(ByteString.copyFromUtf8(rowKeyPrefix))
            .build();
    return ApiFutureUtil.transformAndAdapt(
        delegate.dropRowRangeAsync(protoRequest),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<Void> dropAllRowsAsync(String tableId) {
    DropRowRangeRequest protoRequest =
        DropRowRangeRequest.newBuilder()
            .setName(instanceName.toTableNameStr(tableId))
            .setDeleteAllDataFromTable(true)
            .build();
    return ApiFutureUtil.transformAndAdapt(
        delegate.dropRowRangeAsync(protoRequest),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public void close() {}
}
