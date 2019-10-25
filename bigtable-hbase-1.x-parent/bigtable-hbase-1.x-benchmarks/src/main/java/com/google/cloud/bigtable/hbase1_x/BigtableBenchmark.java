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

import static com.google.cloud.bigtable.hbase1_x.BenchmarkSetupUtils.createAndPopulateTable;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkSetupUtils.createTableToWrite;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkSetupUtils.getRandomBytes;
import static com.google.cloud.bigtable.hbase1_x.BenchmarkSetupUtils.getRandomInt;

import com.google.cloud.bigtable.hbase1_x.BenchmarkSetupUtils.RowShapeParams;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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

/**
 * Performs extensive benchmark tests on bigtable-core and google-cloud-bigtable client. This class
 * is configurable using runtime variables.
 *
 * <p>The {@link #pointRead(Blackhole)} and {@link #bulkScans(Blackhole)} depends on table name with
 * the format of "benchmark_table_[CELLS_PER_ROW]-cells_[CELL_SIZE]-bytes". By default it looks up
 * for: 1. benchmark_table_10-cells_100-bytes, 2. benchmark_table_1-cells_1024-bytes, 3.
 * benchmark_table_100-cells_1-bytes. Each of these table are initialized with 1GB of data to read.
 *
 * <p>Note: If read tables found while running the test, then it assumes the tables would have
 * sufficient data with row prefixed with {@link #READ_ROW_PREFIX}. In case tables not found then
 * only it creates and populate ~1 gig random data.
 *
 * <p>Both {@link #pointWrite()} and {@link #bulkWrite()} write onto "benchmark_writes" table. Here
 * `pointWrite` randomly generates rowKey, while `bulkWrite` rowKeys are prefixed with
 * "key_[CELLS_PER_ROW]-cell_[CELL_SIZE]-bytes-" with up to 6 digit left padded zero.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
public class BigtableBenchmark {

  private static final String BENCHMARK_WRITE_TABLE = "benchmark_writes";
  static final byte[] COL_FAMILY = "bm-test-cf".getBytes();
  static final String READ_ROW_PREFIX = "bm_row_key-";
  private static final String MUTATE_ROW_PREFIX = "bm_mutate_row_key-";
  static final long SAMPLE_TIMESTAMP = 1571940889000L;

  @Param("")
  private String projectId;

  @Param("")
  private String instanceId;

  @Param({
    "cellsPerRow/10/cellSize/100",
    "cellsPerRow/1/cellSize/1024",
    "cellsPerRow/100/cellSize/1"
  })
  private String rowShape;

  @Param({"true", "false"})
  private boolean useGcj;

  @Param("true")
  private boolean useBatch;

  private Connection connection;
  private Table table;
  private RowShapeParams rowShapeParams;

  @Setup
  public void setUp() throws IOException {
    connection = BenchmarkSetupUtils.createConnection(projectId, instanceId, useBatch, useGcj);
    rowShapeParams = new RowShapeParams(rowShape);
    table = connection.getTable(rowShapeParams.tableName);

    createAndPopulateTable(connection, rowShapeParams);
    createTableToWrite(connection, TableName.valueOf(BENCHMARK_WRITE_TABLE));
  }

  /** Reads randomly one row and iterator over the cells. */
  @Benchmark
  public void pointRead(Blackhole blackhole) throws IOException {
    byte[] rowKey = Bytes.toBytes(READ_ROW_PREFIX + String.format("%010d", getRandomInt(10_000)));

    Result result = table.get(new Get(rowKey));

    for (Cell cell : result.listCells()) {
      blackhole.consume(cell);
    }
  }

  /** Scans complete table and receive stream of {@link Result} containing data ~1 gig. */
  @Benchmark
  public void bulkScans(Blackhole blackhole) throws IOException {
    Scan scan = new Scan().setMaxVersions(1).setRowPrefixFilter(Bytes.toBytes(READ_ROW_PREFIX));
    ResultScanner resScanner = table.getScanner(scan);

    for (Result result = resScanner.next(); result != null; result = resScanner.next()) {

      for (Cell cell : result.rawCells()) {
        blackhole.consume(cell);
      }
    }
  }

  /** Write a single row with randomly generated rowKey and variably provided cells. */
  @Benchmark
  public void pointWrite() throws IOException {
    String rowKey = MUTATE_ROW_PREFIX + String.format("%06d", getRandomInt(1_000_000));
    Put put = new Put(Bytes.toBytes(rowKey));
    byte[] cellValue = getRandomBytes(rowShapeParams.cellSize);

    for (int cellInd = 0; cellInd < rowShapeParams.cellsPerRow; cellInd++) {
      put.addColumn(
          COL_FAMILY,
          Bytes.toBytes("qualifier-" + String.format("%06d", cellInd)),
          SAMPLE_TIMESTAMP,
          cellValue);
    }

    table.put(put);
  }

  /**
   * Sent a single batch of 100 rows for mutation. The rows are prefixed with
   * "key_[CELLS_PER_ROW]-cell_[CELL_SIZE]-bytes-" and with randomly generated up to 6 digit left
   * padded zeros.
   */
  @Benchmark
  public void bulkWrite() throws IOException {
    try (BufferedMutator bufferedMutator =
        connection.getBufferedMutator(TableName.valueOf(BENCHMARK_WRITE_TABLE))) {

      for (int rowInd = 0; rowInd < 100; rowInd++) {
        // zero padded row-key
        Put put =
            new Put(
                Bytes.toBytes(MUTATE_ROW_PREFIX + String.format("%06d", getRandomInt(1_000_000))));

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
  }
}
