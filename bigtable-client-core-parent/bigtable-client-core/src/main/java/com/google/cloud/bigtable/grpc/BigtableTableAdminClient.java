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

import java.util.concurrent.TimeoutException;

import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;

/**
 * A client for the Cloud Bigtable Table Admin API.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface BigtableTableAdminClient {

  /**
   * Creates a new table. The table can be created with a full set of initial column families,
   * specified in the request.
   * @param request a {@link CreateTableRequest} object.
   */
  void createTable(CreateTableRequest request);

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
   * @return return {@link Table} object  that contains the updated table structure.
   */
  Table modifyColumnFamily(ModifyColumnFamiliesRequest request);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return a {@link ListenableFuture} that returns {@link Table} object that contains the updated
   *         table structure.
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
   * caught up.
   * @throws TimeoutException if timeout is reached.
   */
  void waitForReplication(BigtableTableName tableName, long timeout) throws InterruptedException, TimeoutException;


  // ////////////// SNAPSHOT methods /////////////
  /**
   * Creates a new snapshot from a table in a specific cluster.
   * @param request a {@link SnapshotTableRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  ListenableFuture<Operation> snapshotTableAsync(SnapshotTableRequest request);

  /**
   * Gets metadata information about the specified snapshot.
   * @param request a {@link GetSnapshotRequest} object.
   * @return The {@link Snapshot} definied by the request.
   */
  ListenableFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request);

  /**
   * Lists all snapshots associated with the specified cluster.
   * @param request a {@link ListSnapshotsRequest} object.
   * @return The {@link ListSnapshotsResponse} which has the list of the snapshots in the cluster.
   */
  ListenableFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request);

  /**
   * Permanently deletes the specified snapshot.
   * @param request a {@link DeleteSnapshotRequest} object.
   */
  ListenableFuture<Empty> deleteSnapshotAsync(DeleteSnapshotRequest request);

  /**
   * Creates a new table from a snapshot.
   * @param request a {@link CreateTableFromSnapshotRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  ListenableFuture<Operation> createTableFromSnapshotAsync(CreateTableFromSnapshotRequest request);
}
