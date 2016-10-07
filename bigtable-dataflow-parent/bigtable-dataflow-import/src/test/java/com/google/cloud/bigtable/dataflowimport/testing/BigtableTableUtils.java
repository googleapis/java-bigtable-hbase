/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflowimport.testing;

import com.google.bigtable.repackaged.com.google.cloud.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.dataflowimport.HBaseImportOptions;
import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * Helper for creating, deleting and reading a Cloud Bigtable during tests. Each instance manages
 * a specific table.
 */
public class BigtableTableUtils implements AutoCloseable {
  private static final int MAX_VERSISON = 50;

  private final Connection connection;
  private final Admin admin;
  private final TableName tableName;
  private final String[] columnFamilyNames;

  private BigtableTableUtils(
      Connection connection, Admin admin, String tableName, String ...columnFamilyNames)
      throws IOException {
    this.connection = connection;
    this.admin = admin;
    this.tableName = TableName.valueOf(tableName);
    this.columnFamilyNames = Arrays.copyOf(columnFamilyNames, columnFamilyNames.length);
  }

  /**
   * Creates an empty table with column families specified by {@code columnFamilyNames}.
   * If table already exists, it is removed and recreated.
   */
  public void createEmptyTable() throws IOException {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    for (String columnFamilyName : columnFamilyNames) {
      tableDescriptor.addFamily(
          new HColumnDescriptor(columnFamilyName).setMaxVersions(MAX_VERSISON));
    }
    admin.createTable(tableDescriptor);
  }

  /**
   * Returns true if table already exists.
   */
  public boolean isTableExists() throws IOException {
    return admin.tableExists(tableName);
  }

  /**
   * Drops the table.
   */
  @Override
  public void close() throws IOException {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  /**
   * Returns the content of the table as a {@link Set} of {@link Cell}s. This is only suitable
   * for small tables.
   */
  public Set<? extends Cell> readAllCellsFromTable() throws IOException {
    Table table = connection.getTable(tableName);
    Scan scan = new Scan().setMaxVersions().setCacheBlocks(false);
    ResultScanner resultScanner = table.getScanner(scan);
    Set<Cell> cells = Sets.newHashSet();
    for (Result result : resultScanner) {
      cells.addAll(result.listCells());
    }
    return cells;
  }

  public static class BigtableTableUtilsFactory {
    private final Connection connection;
    private final Admin admin;

    private BigtableTableUtilsFactory(Connection connection) throws IOException {
      this.connection = connection;
      this.admin = connection.getAdmin();
    }

    /**
     * Creates a {@link BigtableTableUtils} instance that manages a table named {@code tableName}.
     * The {@code columnFamilies} parameter defines the column families in this table.
     */
    public BigtableTableUtils createBigtableTableUtils(String tableName, String ...columnFamilies)
        throws IOException {
      return new BigtableTableUtils(connection, admin, tableName, columnFamilies);
    }

    public void close() throws IOException {
      this.connection.close();
    }

    public static BigtableTableUtilsFactory from(HBaseImportOptions options) throws IOException {
      return new BigtableTableUtilsFactory(BigtableConfiguration.connect(
          options.getBigtableProjectId(),
          options.getBigtableInstanceId()));
    }
  }
}
