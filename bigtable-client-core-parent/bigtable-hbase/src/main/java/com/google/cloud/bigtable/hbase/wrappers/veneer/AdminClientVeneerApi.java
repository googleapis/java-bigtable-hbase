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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.Backup;
import com.google.cloud.bigtable.admin.v2.models.Cluster;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoredTableResult;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.protobuf.ByteString;
import java.util.List;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class AdminClientVeneerApi implements AdminClientWrapper {

  private final BigtableTableAdminClient delegate;
  private final BigtableInstanceAdminClient instanceDelegate;

  AdminClientVeneerApi(
      BigtableTableAdminClient delegate, BigtableInstanceAdminClient instanceDelegate) {
    this.delegate = delegate;
    this.instanceDelegate = instanceDelegate;
  }

  @Override
  public ApiFuture<Table> createTableAsync(CreateTableRequest request) {
    return delegate.createTableAsync(request);
  }

  @Override
  public ApiFuture<Table> getTableAsync(String tableId) {
    return delegate.getTableAsync(tableId);
  }

  @Override
  public ApiFuture<List<String>> listTablesAsync() {
    return delegate.listTablesAsync();
  }

  @Override
  public ApiFuture<Void> deleteTableAsync(String tableId) {
    return delegate.deleteTableAsync(tableId);
  }

  @Override
  public ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request) {
    return delegate.modifyFamiliesAsync(request);
  }

  @Override
  public ApiFuture<Void> dropRowRangeAsync(String tableId, ByteString rowKeyPrefix) {
    return delegate.dropRowRangeAsync(tableId, rowKeyPrefix);
  }

  @Override
  public ApiFuture<Void> dropAllRowsAsync(String tableId) {
    return delegate.dropAllRowsAsync(tableId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Backup> createBackupAsync(CreateBackupRequest request) {
    return delegate.createBackupAsync(request);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<List<String>> listBackupsAsync(String clusterId) {
    return delegate.listBackupsAsync(clusterId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> deleteBackupAsync(String clusterId, String backupId) {
    return delegate.deleteBackupAsync(clusterId, backupId);
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<RestoredTableResult> restoreTableAsync(RestoreTableRequest request) {
    return delegate.restoreTableAsync(request);
  }

  public List<Cluster> listClusters(String instanceId) {
    return instanceDelegate.listClusters(instanceId);
  }

  @Override
  public void close() {
    delegate.close();
    if (instanceDelegate != null) {
      instanceDelegate.close();
    }
  }
}
