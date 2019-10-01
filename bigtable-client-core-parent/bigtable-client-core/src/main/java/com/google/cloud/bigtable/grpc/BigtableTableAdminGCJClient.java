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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * This class implements existing {@link IBigtableTableAdminClient} operations with
 * Google-cloud-java's {@link BigtableTableAdminClient} & {@link BaseBigtableTableAdminClient}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableTableAdminGCJClient implements IBigtableTableAdminClient, AutoCloseable {

  private final BigtableTableAdminClient delegate;
  private final BaseBigtableTableAdminClient baseAdminClient;

  public BigtableTableAdminGCJClient(
      @Nonnull BigtableTableAdminClient delegate,
      @Nonnull BaseBigtableTableAdminClient baseAdminClient) {
    this.delegate = delegate;
    this.baseAdminClient = baseAdminClient;
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(CreateTableRequest request) {
    return delegate.createTable(request);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Table> createTableAsync(CreateTableRequest request) {
    return delegate.createTableAsync(request);
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(String tableId) {
    return delegate.getTable(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Table> getTableAsync(String tableId) {
    return delegate.getTableAsync(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() {
    return delegate.listTables();
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<List<String>> listTablesAsync() {
    return delegate.listTablesAsync();
  }

  /** {@inheritDoc} */
  @Override
  public void deleteTable(String tableId) {
    delegate.deleteTable(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> deleteTableAsync(String tableId) {
    return delegate.deleteTableAsync(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public Table modifyFamilies(ModifyColumnFamiliesRequest request) {
    return delegate.modifyFamilies(request);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request) {
    return delegate.modifyFamiliesAsync(request);
  }

  /** {@inheritDoc} */
  @Override
  public void dropRowRange(String tableId, String rowKeyPrefix) {
    delegate.dropRowRange(tableId, rowKeyPrefix);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix) {
    return delegate.dropRowRangeAsync(tableId, rowKeyPrefix);
  }

  /** {@inheritDoc} */
  @Override
  public void dropAllRows(String tableId) {
    delegate.dropAllRows(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> dropAllRowsAsync(String tableId) {
    return delegate.dropAllRowsAsync(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Operation> snapshotTableAsync(SnapshotTableRequest request) {
    return baseAdminClient.snapshotTableCallable().futureCall(request);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request) {
    return baseAdminClient.getSnapshotCallable().futureCall(request);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request) {
    return baseAdminClient.listSnapshotsCallable().futureCall(request);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> deleteSnapshotAsync(DeleteSnapshotRequest request) {
    return ApiFutures.transform(
        baseAdminClient.deleteSnapshotCallable().futureCall(request),
        new ApiFunction<Empty, Void>() {
          @Override
          public Void apply(Empty input) {
            return null;
          }
        },
        directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Operation> createTableFromSnapshotAsync(CreateTableFromSnapshotRequest request) {
    return baseAdminClient.createTableFromSnapshotCallable().futureCall(request);
  }

  @Override
  public void close() throws Exception {
    delegate.close();
    baseAdminClient.close();
  }
}
