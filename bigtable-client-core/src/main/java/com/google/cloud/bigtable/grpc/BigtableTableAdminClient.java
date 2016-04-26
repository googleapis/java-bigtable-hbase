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

import com.google.bigtable.admin.table.v1.BulkDeleteRowsRequest;
import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest;
import com.google.bigtable.admin.table.v1.GetTableRequest;
import com.google.bigtable.admin.table.v1.ListTablesRequest;
import com.google.bigtable.admin.table.v1.ListTablesResponse;
import com.google.bigtable.admin.table.v1.RenameTableRequest;
import com.google.bigtable.admin.table.v1.Table;

/**
 * A client for the Cloud Bigtable Table Admin API.
 */
public interface BigtableTableAdminClient {

  /**
   * Lists the names of all tables served from a specified cluster.
   */
  ListTablesResponse listTables(ListTablesRequest request);

  /**
   * Gets the details of a table.
   */
  Table getTable(GetTableRequest request);

  /**
   * Creates a new table, to be served from a specified cluster.
   * The table can be created with a full set of initial column families,
   * specified in the request.
   */
  void createTable(CreateTableRequest request);

  /**
   * Creates a new column family within a specified table.
   */
  void createColumnFamily(CreateColumnFamilyRequest request);

  /**
   * Updates a column family within a specified table.
   */
  void updateColumnFamily(ColumnFamily request);

  /**
   * Permanently deletes a specified table and all of its data.
   */
  void deleteTable(DeleteTableRequest request);

  /**
   * Permanently deletes a specified column family and all of its data.
   */
  void deleteColumnFamily(DeleteColumnFamilyRequest request);

  /**
   * Permanently deletes all rows in a range.
   */
  void bulkDeleteRows(BulkDeleteRowsRequest request);

  /**
   * Changes the name of a specified table.
   * Cannot be used to move tables between clusters, zones, or projects.
   */
  void renameTable(RenameTableRequest request);
}
