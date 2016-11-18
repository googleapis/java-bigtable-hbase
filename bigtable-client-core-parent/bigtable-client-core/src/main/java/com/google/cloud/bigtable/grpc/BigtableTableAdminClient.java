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
import com.google.longrunning.Operation;

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
   * @param request a {@link com.google.bigtable.admin.v2.CreateTableRequest} object.
   */
  void createTable(CreateTableRequest request);

  /**
   * Gets the details of a table.
   *
   * @param request a {@link com.google.bigtable.admin.v2.GetTableRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.Table} object.
   */
  Table getTable(GetTableRequest request);

  /**
   * Lists the names of all tables in an instance.
   *
   * @param request a {@link com.google.bigtable.admin.v2.ListTablesRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.ListTablesResponse} object.
   */
  ListTablesResponse listTables(ListTablesRequest request);

  /**
   * Permanently deletes a specified table and all of its data.
   *
   * @param request a {@link com.google.bigtable.admin.v2.DeleteTableRequest} object.
   */
  void deleteTable(DeleteTableRequest request);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest} object.
   */
  void modifyColumnFamily(ModifyColumnFamiliesRequest request);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param request a {@link com.google.bigtable.admin.v2.DropRowRangeRequest} object.
   */
  void dropRowRange(DropRowRangeRequest request);

  // ////////////// SNAPSHOT methods /////////////
  /**
   * Creates a new snapshot from a table in a specific cluster.
   * @param request a {@link SnapshotTableRequest} object.
   * 
   * @return The long running {@link Operation} for the request.
   */
  Operation snapshotTable(SnapshotTableRequest request);

  /**
   * Get metadata information about the specified snapshot.
   * @param request a {@link GetSnapshotRequest} object.
   */
  Snapshot getSnapshot(GetSnapshotRequest request);

  /**
   * Lists all snapshots associated with the specified cluster.
   * @param request a {@link ListSnapshotsRequest} object.
   */
  ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request);

  /**
   * Permanently deletes the specified snapshot.
   * @param request a {@link DeleteSnapshotRequest} object.
   */
  void deleteSnapshot(DeleteSnapshotRequest request);

  /**
   * Creates a new table from a snapshot.
   * @param request a {@link CreateTableFromSnapshotRequest} object.
   */
  Operation createTableFromSnapshot(CreateTableFromSnapshotRequest request);

}
