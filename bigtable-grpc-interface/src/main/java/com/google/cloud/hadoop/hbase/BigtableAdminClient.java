package com.google.cloud.hadoop.hbase;

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
public interface BigtableAdminClient extends AutoCloseable {

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
   * Permanently deletes a specified table and all of its data.
   */
  void deleteTable(DeleteTableRequest request);

  /**
   * Permanently deletes a specified column family and all of its data.
   */
  void deleteColumnFamily(DeleteColumnFamilyRequest request);

  /**
   * Changes the name of a specified table.
   * Cannot be used to move tables between clusters, zones, or projects.
   */
  void renameTable(RenameTableRequest request);
}
