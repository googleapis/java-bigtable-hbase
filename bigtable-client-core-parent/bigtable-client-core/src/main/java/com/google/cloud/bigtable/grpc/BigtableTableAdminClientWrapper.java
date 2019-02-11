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

import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * This class implements the {@link IBigtableTableAdminClient} interface and wraps
 * {@link BigtableTableAdminClient} with Google-cloud-java's models.
 */
public class BigtableTableAdminClientWrapper implements IBigtableTableAdminClient {

  private final BigtableTableAdminClient delegate;
  private final BigtableInstanceName instanceName;

  public BigtableTableAdminClientWrapper(@Nonnull BigtableTableAdminClient adminClient,
      @Nonnull BigtableOptions options){
    Preconditions.checkNotNull(adminClient);
    Preconditions.checkNotNull(options);
    this.delegate = adminClient;
    this.instanceName = options.getInstanceName();
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(CreateTableRequest request) {
    com.google.bigtable.admin.v2.CreateTableRequest requestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());
    delegate.createTable(requestProto);

    return getTable(requestProto.getTableId());
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> createTableAsync(CreateTableRequest request) {
    com.google.bigtable.admin.v2.CreateTableRequest requestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return Futures.transform(delegate.createTableAsync(requestProto),
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

    return Table.fromProto(delegate.getTable(requestProto));
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> getTableAsync(String tableId) {
    GetTableRequest requestProto = GetTableRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .build();

    return Futures.transform(delegate.getTableAsync(requestProto),
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

    ListTablesResponse response = delegate.listTables(requestProto);

    ImmutableList.Builder<String> tableIdsBuilder = ImmutableList.builder();
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
    ListenableFuture<ListTablesResponse> response = delegate.listTablesAsync(request);

    return Futures.transform(response, new Function<ListTablesResponse, List<String>>() {
      @Override
      public List<String> apply(ListTablesResponse input) {
        ImmutableList.Builder<String> tableIdsBuilder = ImmutableList.builder();
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

    delegate.deleteTable(request);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Void> deleteTableAsync(String tableId) {
    DeleteTableRequest request = DeleteTableRequest.newBuilder()
        .setName(instanceName.toTableNameStr(tableId))
        .build();

    return Futures.transform(delegate.deleteTableAsync(request), new Function<Empty, Void>() {
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

    return Table.fromProto(delegate.modifyColumnFamily(modifyColumnRequestProto));
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request) {
    com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest modifyColumnRequestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return Futures.transform(delegate.modifyColumnFamilyAsync(modifyColumnRequestProto),
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

    delegate.dropRowRange(buildDropRowRangeRequest(tableId, rowKeyPrefix));
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix) {

    return Futures.transform(
        delegate.dropRowRangeAsync(buildDropRowRangeRequest(tableId, rowKeyPrefix)),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        }, MoreExecutors.directExecutor());
  }

  @Override
  public ListenableFuture<Operation> snapshotTableAsync(SnapshotTableRequest request) {
    return delegate.snapshotTableAsync(request);
  }

  @Override
  public ListenableFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request) {
    return delegate.getSnapshotAsync(request);
  }

  @Override
  public ListenableFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request) {
    return delegate.listSnapshotsAsync(request);
  }

  @Override
  public ListenableFuture<Void> deleteSnapshotAsync(DeleteSnapshotRequest request) {
    return Futures.transform(delegate.deleteSnapshotAsync(request), new Function<Empty, Void>() {
      @Override
      public Void apply(Empty input) {
        return null;
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public ListenableFuture<Operation> createTableFromSnapshotAsync(
      CreateTableFromSnapshotRequest request) {
    return delegate.createTableFromSnapshotAsync(request);
  }

  private DropRowRangeRequest buildDropRowRangeRequest(String tableId, String rowKeyPrefix) {
    DropRowRangeRequest.Builder dropRequestProtoBuiler =
        DropRowRangeRequest.newBuilder()
            .setName(instanceName.toTableNameStr(tableId));

    if (!Strings.isNullOrEmpty(rowKeyPrefix)) {
      dropRequestProtoBuiler
          .setDeleteAllDataFromTable(false)
          .setRowKeyPrefix(ByteString.copyFromUtf8(rowKeyPrefix));
    } else {
      dropRequestProtoBuiler.setDeleteAllDataFromTable(true);
    }

    return dropRequestProtoBuiler.build();
  }
}
