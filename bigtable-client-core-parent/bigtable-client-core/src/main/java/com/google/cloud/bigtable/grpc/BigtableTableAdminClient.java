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

import com.google.api.core.InternalExtensionOnly;
import com.google.bigtable.admin.v2.Backup;
import com.google.bigtable.admin.v2.CreateBackupRequest;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteBackupRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetBackupRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListBackupsRequest;
import com.google.bigtable.admin.v2.ListBackupsResponse;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.RestoreTableRequest;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.UpdateBackupRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A client for the Cloud Bigtable Table Admin API. */
@InternalExtensionOnly
public interface BigtableTableAdminClient {

  /**
   * Creates a new table. The table can be created with a full set of initial column families,
   * specified in the request.
   *
   * @param request a {@link CreateTableRequest} object.
   */
  Table createTable(CreateTableRequest request);

  /**
   * Creates a new table asynchronously. The table can be created with a full set of initial column
   * families, specified in the request.
   *
   * @param request a {@link CreateTableRequest} object.
   */
  ListenableFuture<Table> createTableAsync(CreateTableRequest request);

  /**
   * Gets the details of a table.
   *
   * @param request a {@link GetTableRequest} object.
   * @return a {@link Table} object.
   */
  Table getTable(GetTableRequest request);

  /**
   * Gets the details of a table asynchronously.
   *
   * @param request a {@link com.google.bigtable.admin.v2.GetTableRequest} object.
   * @return a {@link ListenableFuture} that returns a {@link Table} object.
   */
  ListenableFuture<Table> getTableAsync(GetTableRequest request);

  /**
   * Lists the names of all tables in an instance.
   *
   * @param request a {@link ListTablesRequest} object.
   * @return a {@link ListTablesResponse} object.
   */
  ListTablesResponse listTables(ListTablesRequest request);

  /**
   * Lists the names of all tables in an instance asynchronously.
   *
   * @param request a {@link ListTablesRequest} object.
   * @return a {@link ListenableFuture} that returns a {@link ListTablesResponse} object.
   */
  ListenableFuture<ListTablesResponse> listTablesAsync(ListTablesRequest request);

  /**
   * Permanently deletes a specified table and all of its data.
   *
   * @param request a {@link com.google.bigtable.admin.v2.DeleteTableRequest} object.
   */
  void deleteTable(DeleteTableRequest request);

  /**
   * Permanently deletes a specified table and all of its data.
   *
   * @param request a {@link DeleteTableRequest} object.
   * @return a {@link ListenableFuture} that returns {@link Empty} object.
   */
  ListenableFuture<Empty> deleteTableAsync(DeleteTableRequest request);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return return {@link Table} object that contains the updated table structure.
   */
  Table modifyColumnFamily(ModifyColumnFamiliesRequest request);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return a {@link ListenableFuture} that returns {@link Table} object that contains the updated
   *     table structure.
   */
  ListenableFuture<Table> modifyColumnFamilyAsync(ModifyColumnFamiliesRequest request);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param request a {@link DropRowRangeRequest} object.
   */
  void dropRowRange(DropRowRangeRequest request);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param request a {@link com.google.bigtable.admin.v2.DropRowRangeRequest} object.
   * @return a {@link ListenableFuture} that returns {@link Empty} object.
   */
  ListenableFuture<Empty> dropRowRangeAsync(DropRowRangeRequest request);

  /**
   * Blocks until replication has caught up to the point this method was called or timeout is
   * reached.
   *
   * @param tableName the name of the table to wait for replication.
   * @param timeout the maximum time to wait in seconds.
   * @throws InterruptedException if call is interrupted while waiting to recheck if replication has
   *     caught up.
   * @throws TimeoutException if timeout is reached.
   */
  void waitForReplication(BigtableTableName tableName, long timeout)
      throws InterruptedException, TimeoutException;

  /**
   * Get an IAM policy.
   *
   * @see <a href="https://cloud.google.com/bigtable/docs/access-control">Cloud Bigtable access
   *     control</a>
   * @param request a {@link com.google.iam.v1.GetIamPolicyRequest} object.
   * @return a {@link com.google.iam.v1.Policy} object.
   */
  Policy getIamPolicy(GetIamPolicyRequest request);

  /**
   * Set an IAM policy.
   *
   * @see <a href="https://cloud.google.com/bigtable/docs/access-control">Cloud Bigtable access
   *     control</a>
   * @param request a {@link com.google.iam.v1.SetIamPolicyRequest} object.
   * @return a {@link com.google.iam.v1.Policy} object.
   */
  Policy setIamPolicy(SetIamPolicyRequest request);

  /**
   * Tests an IAM policy.
   *
   * @see <a href="https://cloud.google.com/bigtable/docs/access-control">Cloud Bigtable access
   *     control</a>
   * @param request a {@link com.google.iam.v1.TestIamPermissionsRequest} object.
   * @return a {@link com.google.iam.v1.TestIamPermissionsResponse} object.
   */
  TestIamPermissionsResponse testIamPermissions(TestIamPermissionsRequest request);

  // ////////////// SNAPSHOT methods /////////////
  /** @deprecated Snapshots will be removed in the future */
  @Deprecated
  ListenableFuture<Operation> snapshotTableAsync(SnapshotTableRequest request);

  /** @deprecated Snapshots will be removed in the future */
  @Deprecated
  ListenableFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request);

  /** @deprecated Snapshots will be removed in the future */
  @Deprecated
  ListenableFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request);

  /** @deprecated Snapshots will be removed in the future */
  @Deprecated
  ListenableFuture<Empty> deleteSnapshotAsync(DeleteSnapshotRequest request);

  /** @deprecated Snapshots will be removed in the future */
  @Deprecated
  ListenableFuture<Operation> createTableFromSnapshotAsync(CreateTableFromSnapshotRequest request);

  /**
   * Creates a new backup from a table in a specific cluster.
   *
   * @param request a {@link CreateBackupRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  ListenableFuture<Operation> createBackupAsync(CreateBackupRequest request);

  /**
   * Gets metadata information about the specified backup.
   *
   * @param request a {@link GetBackupRequest} object.
   * @return The {@link Backup} defined by the request.
   */
  ListenableFuture<Backup> getBackupAsync(GetBackupRequest request);

  /**
   * Updates the specified backup.
   *
   * @param request the request to update the Backup.
   */
  ListenableFuture<Backup> updateBackupAsync(UpdateBackupRequest request);

  /**
   * Lists all backups associated with the specified cluster.
   *
   * @param request a {@link GetBackupRequest} object.
   * @return The {@link ListBackupsResponse} which has the list of the backups in the cluster.
   */
  ListenableFuture<ListBackupsResponse> listBackupsAsync(ListBackupsRequest request);

  /**
   * Permanently deletes the specified backup.
   *
   * @param request a {@link DeleteBackupRequest} object.
   */
  ListenableFuture<Empty> deleteBackupAsync(DeleteBackupRequest request);

  /**
   * Creates a new table from a backup.
   *
   * @param request a {@link RestoreTableRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  ListenableFuture<Operation> restoreTableAsync(RestoreTableRequest request);

  /**
   * Gets the latest state of a long-running operation. Clients may use this method to poll the
   * operation result at intervals as recommended by the API service.
   *
   * <p>{@link #createBackupAsync(CreateBackupRequest)} and {@link
   * #restoreTableAsync(RestoreTableRequest)} will return a {@link
   * com.google.longrunning.Operation}. Use this method and pass in the {@link
   * com.google.longrunning.Operation}'s name in the request to see if the Operation is done via
   * {@link com.google.longrunning.Operation#getDone()}. The backup will not be available until that
   * happens.
   *
   * @param request a {@link com.google.longrunning.GetOperationRequest} object.
   * @return a {@link com.google.longrunning.Operation} object.
   */
  Operation getOperation(GetOperationRequest request);

  /**
   * Waits for the long running operation to complete by polling with exponential backoff. A default
   * timeout of 10 minutes is used.
   *
   * @param operation
   * @throws IOException
   * @throws TimeoutException If the timeout is exceeded.
   */
  Operation waitForOperation(Operation operation) throws TimeoutException, IOException;

  /**
   * Waits for the long running operation to complete by polling with exponential backoff.
   *
   * @param operation
   * @param timeout
   * @param timeUnit
   * @throws IOException
   * @throws TimeoutException If the timeout is exceeded.
   */
  Operation waitForOperation(Operation operation, long timeout, TimeUnit timeUnit)
      throws IOException, TimeoutException;
}
