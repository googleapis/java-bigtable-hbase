/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * This class implements the {@link IBigtableTableAdminClient} interface and wraps
 * {@link BigtableTableAdminClient} with Google-cloud-java's models.
 */
public class BigtableTableAdminClientWrapper implements IBigtableTableAdminClient {

  private final BigtableTableAdminClient adminClient;
  private final BigtableInstanceName instanceName;

  public BigtableTableAdminClientWrapper(@Nonnull BigtableTableAdminClient adminClient,
      @Nonnull BigtableOptions options){
    Preconditions.checkNotNull(adminClient);
    Preconditions.checkNotNull(options);
    this.adminClient = adminClient;
    this.instanceName = options.getInstanceName();
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(CreateTableRequest request) {
    com.google.bigtable.admin.v2.CreateTableRequest requestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());
    adminClient.createTable(requestProto);

    return getTable(requestProto.getTableId());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> createTableAsync(CreateTableRequest request) {
    com.google.bigtable.admin.v2.CreateTableRequest requestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return Futures.transform(adminClient.createTableAsync(requestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(String tableId) {
    GetTableRequest requestProto = GetTableRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .build();

    return Table.fromProto(adminClient.getTable(requestProto));
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> getTableAsync(String tableId) {
    GetTableRequest requestProto = GetTableRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .build();

    return Futures.transform(adminClient.getTableAsync(requestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() {
    ListTablesRequest requestProto = ListTablesRequest.newBuilder()
        .setParent(instanceName.toString())
        .build();

    ListTablesResponse response = adminClient.listTables(requestProto);

    ImmutableList.Builder<String> tableIdsBuilder =
        ImmutableList.builderWithExpectedSize(response.getTablesList().size());
    for(com.google.bigtable.admin.v2.Table tableProto : response.getTablesList()){
      tableIdsBuilder.add(instanceName.toTableId(tableProto.getName()));
    }

    return tableIdsBuilder.build();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<List<String>> listTablesAsync() {
    ListTablesRequest request = ListTablesRequest.newBuilder()
        .setParent(instanceName.toString())
        .build();
    ListenableFuture<ListTablesResponse> response = adminClient.listTablesAsync(request);

    return Futures.transform(response, new Function<ListTablesResponse, List<String>>() {
      @Override
      public List<String> apply(ListTablesResponse input) {
        ImmutableList.Builder<String> tableIdsBuilder =
            ImmutableList.builderWithExpectedSize(input.getTablesList().size());
        for(com.google.bigtable.admin.v2.Table tableProto : input.getTablesList()){
          tableIdsBuilder.add(instanceName.toTableId(tableProto.getName()));
        }

        return tableIdsBuilder.build();
      }
    }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public void deleteTable(String tableId) {
    DeleteTableRequest request = DeleteTableRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .build();

    adminClient.deleteTable(request);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Void> deleteTableAsync(String tableId) {
    DeleteTableRequest request = DeleteTableRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .build();

    return Futures.transform(adminClient.deleteTableAsync(request), new Function<Empty, Void>() {
      @Override
      public Void apply(Empty empty) {
        return null;
      }
    }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public Table modifyFamilies(ModifyColumnFamiliesRequest request) {
    com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest modifyColumnRequestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return Table.fromProto(adminClient.modifyColumnFamily(modifyColumnRequestProto));
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request) {
    com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest modifyColumnRequestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return Futures.transform(adminClient.modifyColumnFamilyAsync(modifyColumnRequestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
      @Override
      public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
        return Table.fromProto(tableProto);
      }
    }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public void dropRowRange(String tableId, String rowKeyPrefix) {
    DropRowRangeRequest requestProto = DropRowRangeRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .setRowKeyPrefix(ByteString.copyFromUtf8(rowKeyPrefix))
        .build();

    adminClient.dropRowRange(requestProto);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix) {
    DropRowRangeRequest requestProto = DropRowRangeRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .setRowKeyPrefix(ByteString.copyFromUtf8(rowKeyPrefix))
        .build();

    return Futures.transform(adminClient.dropRowRangeAsync(requestProto), new Function<Empty, Void>() {
      @Override
      public Void apply(Empty empty) {
          return null;
      }
    }, MoreExecutors.directExecutor());
  }
}
