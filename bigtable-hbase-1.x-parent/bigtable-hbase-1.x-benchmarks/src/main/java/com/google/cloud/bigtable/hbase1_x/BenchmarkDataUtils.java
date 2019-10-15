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

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BATCH;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT;
import static com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.COL_FAMILY;
import static com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.ROWS_PER_BATCH;
import static com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.SUFFIX_FMT;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase1_x.BigtableBenchmark.BenchmarkJobConfig;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class BenchmarkDataUtils {

  private static final Logger LOG = Logger.getLogger(BenchmarkDataUtils.class.getName());

  static Connection createConnection(
      String projectId, String instanceId, boolean useBatch, boolean useGcj) {
    Configuration config = BigtableConfiguration.configure(projectId, instanceId);
    config.set(BIGTABLE_USE_BATCH, String.valueOf(useBatch));
    config.set(BIGTABLE_USE_GCJ_CLIENT, String.valueOf(useGcj));
    return BigtableConfiguration.connect(config);
  }

  static void createAndFillTable(Connection connection, BenchmarkJobConfig jobConfig)
      throws IOException {

    try (Admin admin = connection.getAdmin()) {
      if (admin.tableExists(jobConfig.tableName)) {
        LOG.info("Using existing table");
        // Assuming the existing table would have all required data
        return;
      }
      LOG.info("Did not found the the table with tableName as: " + jobConfig.tableName);

      admin.createTable(
          new HTableDescriptor(jobConfig.tableName)
              .addFamily(new HColumnDescriptor(COL_FAMILY).setMaxVersions(1)));
    }

    try (BufferedMutator bufferedMutator = connection.getBufferedMutator(jobConfig.tableName)) {

      String rowPrefix = getRowPrefix(jobConfig.cellsPerRow, jobConfig.cellSize);
      ImmutableList.Builder<Put> putBuilder = ImmutableList.builderWithExpectedSize(ROWS_PER_BATCH);
      final long currentTimestamp = System.currentTimeMillis();

      for (int rowInd = 0, sendBatchInd = 0;
          rowInd < jobConfig.totalRows;
          rowInd++, sendBatchInd++) {

        // zero padded row-key
        Put put = new Put(Bytes.toBytes(rowPrefix + String.format(SUFFIX_FMT, rowInd)));

        for (int cellInd = 0; cellInd < jobConfig.cellsPerRow; cellInd++) {
          put.addColumn(
              COL_FAMILY,
              Bytes.toBytes("qualifier-" + String.format(SUFFIX_FMT, cellInd)),
              currentTimestamp,
              getRandomBytes(jobConfig.cellSize));
        }
        putBuilder.add(put);

        if (sendBatchInd % ROWS_PER_BATCH == 0 || rowInd == (jobConfig.totalRows - 1)) {
          bufferedMutator.mutate(putBuilder.build());

          // Does this makes sense?
          bufferedMutator.flush();

          putBuilder = ImmutableList.builderWithExpectedSize(ROWS_PER_BATCH);
        }
      }
    }
    LOG.info(String.format("Mutate table with %d rows", jobConfig.totalRows));
  }

  static String getTableId(int cellsPerRow, int cellSize) {
    return String.format("benchmark_table_%d-cells_%d-bytes", cellsPerRow, cellSize);
  }

  static String getRowPrefix(int cellsPerRow, int cellSize) {
    return String.format("key_%d-cell_%d-bytes-", cellsPerRow, cellSize);
  }

  static byte[] getRandomBytes(int size) {
    byte[] data = new byte[size];
    new Random().nextBytes(data);
    return data;
  }

  static void createTableToWrite(Connection connection, TableName writeToTableName)
      throws IOException {
    try (Admin admin = connection.getAdmin()) {

      if (admin.tableExists(writeToTableName)) {
        // Deleting the existing table to create a fresh table to write to.
        admin.deleteTable(writeToTableName);
      }
      admin.createTable(
          new HTableDescriptor(writeToTableName)
              .addFamily(new HColumnDescriptor(COL_FAMILY).setMaxVersions(1)));
    }
  }
}
