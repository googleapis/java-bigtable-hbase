/*
 * Copyright 2019 Google LLC.
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
package com.google.cloud.bigtable.hbase1_x;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BATCH;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT;
import static com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.COL_FAMILY;
import static com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.READ_ROW_PREFIX;
import static com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.SAMPLE_TIMESTAMP;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

class BenchmarkSetupUtils {

  private static final Logger LOG = Logger.getLogger(BenchmarkSetupUtils.class.getName());
  private static final long TOTAL_DATA_IN_BYTES = 1024 * 1024 * 1024;
  private static final Pattern CELL_PATTERN = Pattern.compile("cellsPerRow/(\\d+)/cellSize/(\\d+)");

  static Connection createConnection(
      String projectId, String instanceId, boolean useBatch, boolean useGcj) {
    Configuration config = BigtableConfiguration.configure(projectId, instanceId);
    config.set(BIGTABLE_USE_BATCH, String.valueOf(useBatch));
    config.set(BIGTABLE_BULK_AUTOFLUSH_MS_KEY, String.valueOf(100));
    config.set(BIGTABLE_BULK_MAX_ROW_KEY_COUNT, String.valueOf(3000));
    config.set(BIGTABLE_USE_GCJ_CLIENT, String.valueOf(useGcj));
    return BigtableConfiguration.connect(config);
  }

  static void createAndPopulateTable(Connection connection, RowShapeParams rowShapeParams)
      throws IOException {

    try (Admin admin = connection.getAdmin()) {
      if (admin.tableExists(rowShapeParams.tableName)) {
        LOG.info("Using existing table");
        // Assuming the existing table would have all required data
        return;
      }
      LOG.info("Did not found the the table with tableName as: " + rowShapeParams.tableName);

      admin.createTable(
          new HTableDescriptor(rowShapeParams.tableName)
              .addFamily(new HColumnDescriptor(COL_FAMILY).setMaxVersions(1)));
    }

    try (BufferedMutator bufferedMutator =
        connection.getBufferedMutator(rowShapeParams.tableName)) {

      for (int rowInd = 0; rowInd < rowShapeParams.totalRows; rowInd++) {
        // zero padded row-key
        Put put = new Put(Bytes.toBytes(READ_ROW_PREFIX + String.format("%010d", rowInd)));

        for (int cellInd = 0; cellInd < rowShapeParams.cellsPerRow; cellInd++) {
          put.addColumn(
              COL_FAMILY,
              Bytes.toBytes("qualifier-" + String.format("%06d", cellInd)),
              SAMPLE_TIMESTAMP,
              getRandomBytes(rowShapeParams.cellSize));
        }
        bufferedMutator.mutate(put);
      }
    }
    LOG.info(String.format("Mutate table with %d rows", rowShapeParams.totalRows));
  }

  private static String getTableId(int cellsPerRow, int cellSize) {
    return String.format("benchmark_table_%d-cells_%d-bytes", cellsPerRow, cellSize);
  }

  static byte[] getRandomBytes(int size) {
    byte[] data = new byte[size];
    new Random().nextBytes(data);
    return data;
  }

  static void createTableToWrite(Connection connection, TableName writeToTableName)
      throws IOException {
    try (Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(writeToTableName)) {
        admin.createTable(
            new HTableDescriptor(writeToTableName)
                .addFamily(new HColumnDescriptor(COL_FAMILY).setMaxVersions(1)));
      }
    }
  }

  static class RowShapeParams {

    final int cellsPerRow;
    final int cellSize;
    final TableName tableName;
    final long totalRows;

    RowShapeParams(String dataShape) {
      Matcher matcher = CELL_PATTERN.matcher(dataShape);
      Preconditions.checkArgument(matcher.matches(), "Benchmark job configuration did not match");
      cellsPerRow = Integer.valueOf(matcher.group(1));
      cellSize = Integer.valueOf(matcher.group(2));
      Preconditions.checkArgument(
          cellsPerRow != 0 && cellSize != 0, "CellsPerSize or CellSize cannot be zero");

      tableName = TableName.valueOf(getTableId(cellsPerRow, cellSize));

      // Total rows is ~1GB, 15 is the additional size of qualifier, taking this into account is
      // necessary, especially when cellSize is provided 1.
      totalRows = TOTAL_DATA_IN_BYTES / (cellsPerRow * (cellSize + 15));
    }
  }
}
