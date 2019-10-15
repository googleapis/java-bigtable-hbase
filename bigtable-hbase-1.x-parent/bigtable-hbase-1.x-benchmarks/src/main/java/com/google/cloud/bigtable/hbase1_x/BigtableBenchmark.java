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

import static com.google.cloud.bigtable.hbase1_x.BenchmarkDataUtils.createAndFillTable;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkDataUtils.createTableToWrite;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkDataUtils.getRandomBytes;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkDataUtils.getRowPrefix;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkDataUtils.getTableId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
public class BigtableBenchmark {

  static final byte[] COL_FAMILY = "bm-test-cf".getBytes();
  static final String SUFFIX_FMT = "%010d";
  private static final long TOTAL_DATA_IN_BYTES = 1024 * 1024 * 1024;
  private static final String BENCHMARK_WRITE_TABLE = "benchmark_writes";
  private static final Pattern CELL_PATTERN = Pattern.compile("cellsPerRow/(\\d+)/cellSize/(\\d+)");

  // TODO(rahulkql): Discuss and finalize If this actually needed.
  static final int ROWS_PER_BATCH = 10000;

  @Param("")
  private String projectId;

  @Param("")
  private String instanceId;

  @Param({
    "cellsPerRow/10/cellSize/100",
    "cellsPerRow/1/cellSize/1024",
    "cellsPerRow/100/cellSize/1"
  })
  private String benchmarkConfig;

  @Param({"true", "false"})
  private boolean useGcj;

  @Param("false")
  private boolean useBatch;

  private Connection connection;

  private BenchmarkJobConfig jobConfig;

  @Setup
  public void setUp() throws IOException {
    connection = BenchmarkDataUtils.createConnection(projectId, instanceId, useBatch, useGcj);
    jobConfig = new BenchmarkJobConfig(benchmarkConfig);

    createAndFillTable(connection, jobConfig);
    createTableToWrite(connection, TableName.valueOf(BENCHMARK_WRITE_TABLE));
  }

  @Benchmark
  public void pointRead(Blackhole blackhole) throws IOException {
    Table table = connection.getTable(jobConfig.tableName);
    String rowPrefix = getRowPrefix(jobConfig.cellsPerRow, jobConfig.cellSize);
    Result result = table.get(new Get(Bytes.toBytes(rowPrefix + String.format(SUFFIX_FMT, 1))));

    for (Cell cell : result.listCells()) {
      blackhole.consume(cell);
    }
  }

  @Benchmark
  public void bulkScans(Blackhole blackhole) throws IOException {
    String rowPrefix = getRowPrefix(jobConfig.cellsPerRow, jobConfig.cellSize);

    Scan scan = new Scan().setMaxVersions(1).setRowPrefixFilter(Bytes.toBytes(rowPrefix));
    ResultScanner resScanner = connection.getTable(jobConfig.tableName).getScanner(scan);

    for (Result result = resScanner.next(); result != null; result = resScanner.next()) {

      for (Cell cell : result.rawCells()) {
        blackhole.consume(cell);
      }
    }
  }

  // TODO(rahulkql): yet to be fully tested.
  @Benchmark
  public void pointWrite() throws IOException {
    Table table = connection.getTable(TableName.valueOf(BENCHMARK_WRITE_TABLE));

    String rowPrefix = getRowPrefix(jobConfig.cellsPerRow, jobConfig.cellSize);
    Put put = new Put(Bytes.toBytes(rowPrefix + String.format(SUFFIX_FMT, 100)));
    long currentTimestamp = System.currentTimeMillis();

    for (int cellInd = 0; cellInd < jobConfig.cellsPerRow; cellInd++) {
      put.addColumn(
          COL_FAMILY,
          Bytes.toBytes("qualifier-" + String.format(SUFFIX_FMT, cellInd)),
          currentTimestamp,
          getRandomBytes(jobConfig.cellSize));
    }

    table.put(put);
  }

  // TODO(rahulkql): yet to be fully tested.
  @Benchmark
  public void bulkWrite() throws IOException {
    try (BufferedMutator bufferedMutator =
        connection.getBufferedMutator(TableName.valueOf(BENCHMARK_WRITE_TABLE))) {

      String rowPrefix = getRowPrefix(jobConfig.cellsPerRow, jobConfig.cellSize);
      byte[] cellValue = getRandomBytes(jobConfig.cellSize);
      ImmutableList.Builder<Put> putBuilder = ImmutableList.builderWithExpectedSize(ROWS_PER_BATCH);
      long currentTimestamp = System.currentTimeMillis();

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
              cellValue);
        }
        putBuilder.add(put);

        // Sending a lot of rows in a single batch randomly OOM exception
        if (sendBatchInd % ROWS_PER_BATCH == 0 || rowInd == (jobConfig.totalRows - 1)) {
          bufferedMutator.mutate(putBuilder.build());

          // Does this makes sense?
          bufferedMutator.flush();

          putBuilder = ImmutableList.builderWithExpectedSize(ROWS_PER_BATCH);
        }
      }
    }
  }

  public static class BenchmarkJobConfig {

    final int cellsPerRow;
    final int cellSize;
    final TableName tableName;
    final long totalRows;

    BenchmarkJobConfig(String dataShape) {
      Matcher matcher = CELL_PATTERN.matcher(dataShape);
      Preconditions.checkArgument(matcher.matches(), "Benchmark job configuration did not match");
      cellsPerRow = Integer.valueOf(matcher.group(1));
      cellSize = Integer.valueOf(matcher.group(2));
      Preconditions.checkArgument(
          cellsPerRow != 0 && cellSize != 0, "CellsPerSize or CellSize cannot be zero");

      tableName = TableName.valueOf(getTableId(cellsPerRow, cellSize));

      // Total rows is 1GB, 15 is the additional size of qualifier, taking this into account is
      // necessary especially when cellSize provided 1.
      totalRows = TOTAL_DATA_IN_BYTES / (cellsPerRow * (cellSize + 15));
    }
  }
}
