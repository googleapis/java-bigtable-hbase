package com.google.cloud.bigtable.dataflowimport.testing;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
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
import java.util.Set;

/**
 * Helper for creating, deleting and reading a Cloud Bigtable during tests. Each instance manages
 * a specific table that has one column family.
 */
public class BigtableTableUtils implements AutoCloseable {
  private static final int MAX_VERSISON = 5;

  private final Connection connection;
  private final Admin admin;
  private final TableName tableName;
  private final String columnFamilyName;

  private BigtableTableUtils(
      Connection connection, Admin admin, String tableName, String columnFamilyName)
      throws IOException {
    this.connection = connection;
    this.admin = admin;
    this.tableName = TableName.valueOf(tableName);
    this.columnFamilyName = columnFamilyName;
  }

  /**
   * Creates an empty table with a single column family as specified by {@code columnFamily}.
   * If table already exists, it is removed and recreated.
   */
  public void createEmptyTable() throws IOException {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(new HColumnDescriptor(columnFamilyName).setMaxVersions(MAX_VERSISON));
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
     * This table will have a single column family named {@code columnFamilyName}.
     */
    public BigtableTableUtils createBigtableTableUtils(String tableName, String columnFamilyName)
        throws IOException {
      return new BigtableTableUtils(connection, admin, tableName, columnFamilyName);
    }

    public void close() throws IOException {
      this.connection.close();
    }

    public static BigtableTableUtilsFactory from(HBaseImportOptions options) throws IOException {
      return new BigtableTableUtilsFactory(BigtableConfiguration.connect(
          options.getBigtableProjectId(),
          options.getBigtableZoneId(),
          options.getBigtableClusterId()));
    }
  }
}
