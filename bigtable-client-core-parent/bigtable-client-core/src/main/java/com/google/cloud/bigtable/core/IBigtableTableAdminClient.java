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
package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.longrunning.Operation;
import java.util.List;

/**
 * Interface to wrap {@link com.google.cloud.bigtable.grpc.BigtableTableAdminClient} with
 * Google-Cloud-java's models.
 */
public interface IBigtableTableAdminClient {

  /**
   * Creates a new table. The table can be created with a full set of initial column families,
   * specified in the request.
   * @param request a {@link CreateTableRequest} object.
   */
  Table createTable(CreateTableRequest request);

  /**
   * Creates a new table asynchronously. The table can be created with a full set of initial column
   * families, specified in the request.
   *
   * @param request a {@link CreateTableRequest} object.
   */
  ApiFuture<Table> createTableAsync(CreateTableRequest request);

  /**
   * Gets the details of a table.
   *
   * @param tableId a String object.
   * @return a {@link Table} object.
   */
  Table getTable(String tableId);

  /**
   * Gets the details of a table asynchronously.
   *
   * @return a {@link ApiFuture} that returns a {@link Table} object.
   */
  ApiFuture<Table> getTableAsync(String tableId);

  /**
   * Lists the names of all tables in an instance.
   *
   * @return an immutable {@link List} object containing tableId.
   */
  List<String> listTables();

  /**
   * Lists the names of all tables in an instance asynchronously.
   *
   * @return a {@link ApiFuture} of type {@link Void} will be set when request is
   *   successful otherwise exception will be thrown.
   */
  ApiFuture<List<String>> listTablesAsync();

  /**
   * Permanently deletes a specified table and all of its data.
   *
   */
  void deleteTable(String tableId);

  /**
   * Permanently deletes a specified table and all of its data.
   *
   * @return a {@link ApiFuture} of type {@link Void} will be set when request is
   *  successful otherwise exception will be thrown.
   */
  ApiFuture<Void> deleteTableAsync(String tableId);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return return {@link Table} object  that contains the updated table structure.
   */
  Table modifyFamilies(ModifyColumnFamiliesRequest request);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return a {@link ApiFuture} that returns {@link Table} object that contains
   *  the updated table structure.
   */
  ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param tableId
   * @param rowKeyPrefix
   */
  void dropRowRange(String tableId, String rowKeyPrefix);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param tableId
   * @param rowKeyPrefix
   * @return a {@link ApiFuture} that returns {@link Void} object.
   */
  ApiFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix);

  // ////////////// SNAPSHOT methods /////////////
  /**
   * Creates a new snapshot from a table in a specific cluster.
   * @param request a {@link SnapshotTableRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  ApiFuture<Operation> snapshotTableAsync(SnapshotTableRequest request);

  /**
   * Gets metadata information about the specified snapshot.
   * @param request a {@link GetSnapshotRequest} object.
   * @return The {@link Snapshot} definied by the request.
   */
  ApiFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request);

  /**
   * Lists all snapshots associated with the specified cluster.
   * @param request a {@link ListSnapshotsRequest} object.
   * @return The {@link ListSnapshotsResponse} which has the list of the snapshots in the cluster.
   */
  ApiFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request);

  /**
   * Permanently deletes the specified snapshot.
   * @param request a {@link DeleteSnapshotRequest} object.
   */
  ApiFuture<Void> deleteSnapshotAsync(DeleteSnapshotRequest request);

  /**
   * Creates a new table from a snapshot.
   * @param request a {@link CreateTableFromSnapshotRequest} object.
   * @return The long running {@link Operation} for the request.
   */
  ApiFuture<Operation> createTableFromSnapshotAsync(CreateTableFromSnapshotRequest request);
}
