package com.google.cloud.bigtable.hbase1_x;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 6)
@State(Scope.Benchmark)
public class Benchmarks {
  private static final String COL_FAMILY = "bm-test-cf";

  @Param("")
  private String projectId;

  @Param("")
  private String instanceId;

  @Param("100")
  private int cellSize;

  @Param("10")
  private int cellsPerRow;

  @Param("1000")
  private int bulkReadCellsLen;

  @Param("1000")
  private int batchMutationRows;

  @Param("100")
  private int numRows;

  @Param("20")
  private int numKeys;

  @Param({"true", "false"})
  private boolean useGcj;

  @Param("false")
  private boolean useBatch;

  private byte[][] rowKeys;

  private Connection connection;
  private TableName tableName;
  private Table table;
  private BufferedMutator bufferedMutator;

  @Setup
  public void setUp() throws IOException {
    rowKeys = new byte[numRows][30];
    connection = createConnection();
    tableName = TableName.valueOf(getTableId());
    try (Admin admin = connection.getAdmin()) {
      if (admin.tableExists(tableName)) {
        admin.deleteTable(tableName);
      }
      admin.createTable(
          new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COL_FAMILY)));
    }
    table = connection.getTable(tableName);
    bufferedMutator = connection.getBufferedMutator(tableName);

    // Populate rowKeys with a prefix
    for (int i = 0; i < numRows; i++) {
      // These are rows which are being populated when table is created.
      byte[] ranRow = Bytes.toBytes("bm-test-row" + RandomStringUtils.randomAlphanumeric(5));
      rowKeys[i] = ranRow;
    }

    // Populates table for
    ImmutableList.Builder<Put> putBuilder = ImmutableList.builder();
    byte[] value = getRandomByte(cellSize);
    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
      Put put = new Put(rowKeys[rowIndex]);
      for (int cellIndex = 0; cellIndex < cellsPerRow; cellIndex++) {
        put.addColumn(Bytes.toBytes(COL_FAMILY), getRandomByte(10), value);
      }
      putBuilder.add(put);
    }

    table.put(putBuilder.build());

    // Populating table for bulkRead
    Put put = new Put(getRandomByte(20));
    for (int cellIndex = 0; cellIndex < bulkReadCellsLen; cellIndex++) {
      put.addColumn(Bytes.toBytes(COL_FAMILY), getRandomByte(10), value);
    }
    table.put(put);
  }

  @TearDown
  public void tearDown() throws IOException {
    try (Admin admin = connection.getAdmin()) {
      // TODO(rahulkql): Should we use deleteTables(Pattern) instead of this?
      admin.deleteTable(tableName);
    }
    bufferedMutator.close();
    table.close();
    connection.close();
  }

  private Connection createConnection() {
    Configuration config = BigtableConfiguration.configure(projectId, instanceId);
    config.set(BIGTABLE_USE_GCJ_CLIENT, String.valueOf(useGcj));
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BATCH, String.valueOf(useBatch));
    config.set(
        BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, String.valueOf(useBatch));
    return BigtableConfiguration.connect(config);
  }

  private String getTableId() {
    return String.format("benchmark_table_%d-cells_%d-bytes", cellsPerRow, cellSize);
  }

  @Benchmark
  public void pointWrite(Blackhole blackhole) throws IOException {
    byte[] rowKey = getRandomByte(20);
    byte[] cellValue = getRandomByte(cellSize);
    long timestamp = 10_000L;

    Put put = new Put(rowKey);
    for (int i = 0; i < cellsPerRow; i++) {
      put.addColumn(COL_FAMILY.getBytes(), getRandomByte(10), timestamp, cellValue);
    }

    table.put(put);
    Result result = table.get(new Get(rowKey));

    assertEquals(result.listCells().size(), cellsPerRow);
    blackhole.consume(result);
  }

  @Benchmark
  public void batchWrite(Blackhole blackhole) throws IOException {
    String batchRowPrefix = "bm-batch-row-";

    ImmutableList.Builder<Put> putBuilder = ImmutableList.builder();
    byte[] value = getRandomByte(cellSize);
    for (int rowIndex = 0; rowIndex < batchMutationRows; rowIndex++) {
      Put put = new Put(Bytes.toBytes(batchRowPrefix + rowIndex));
      for (int cellIndex = 0; cellIndex < cellsPerRow; cellIndex++) {
        put.addColumn(Bytes.toBytes(COL_FAMILY), getRandomByte(10), value);
      }
      putBuilder.add(put);
    }

    table.put(putBuilder.build());

    Result result = table.get(new Get(Bytes.toBytes(batchRowPrefix + (batchMutationRows - 1))));
    assertNotNull(result);
    blackhole.consume(result);
  }

  @Benchmark
  public void pointRead(Blackhole blackhole) throws IOException {
    Result result = table.get(new Get(rowKeys[0]));

    assertArrayEquals(result.getRow(), rowKeys[0]);
    assertEquals(result.listCells().size(), cellsPerRow);
    for (Cell cell : result.listCells()) {
      assertEquals(cell.getValueArray().length, cellSize);
      blackhole.consume(cell);
    }
  }

  @Benchmark
  public void scans(Blackhole blackhole) throws IOException {
    Scan scan = new Scan();
    scan.withStartRow(rowKeys[0]).withStopRow(rowKeys[numRows - 1]);
    ResultScanner resScanner = table.getScanner(scan);

    Result[] results = resScanner.next(numRows + 1);
    for (int ind = 0; ind < results.length; ind++) {
      Result res = results[ind];
      assertArrayEquals(res.getRow(), rowKeys[ind]);
      assertEquals(cellsPerRow, res.listCells().size());

      for (Cell cell : res.listCells()) {
        assertEquals(cell.getValueArray().length, cellSize);
        blackhole.consume(cell);
      }
    }
  }

  @Benchmark
  public void batchRead(Blackhole blackhole) throws IOException, InterruptedException {
    ImmutableList.Builder<Get> getBuilder = ImmutableList.builder();
    for (byte[] rowKey : rowKeys) {
      getBuilder.add(new Get(rowKey));
    }
    Object[] results = new Object[rowKeys.length];
    table.batch(getBuilder.build(), results);

    // Verifies to results
    for (int i = 0; i < results.length; i++) {
      Result res = (Result) results[i];
      assertArrayEquals(res.getRow(), rowKeys[i]);
      blackhole.consume(res);
    }
  }

  private static byte[] getRandomByte(int size) {
    byte[] randomData = new byte[size];
    new Random().nextBytes(randomData);
    return randomData;
  }
}
