/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.mapreduce.hbasesnapshots;

import com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportHBaseSnapshotJob.ScanCounter;
import com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportHBaseSnapshotJob.SnapshotMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** test mapper for snapshot import */
public class TestSnapshotMapper {

  private MapDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> mapDriver;

  @Before
  public void setup() {
    SnapshotMapper snapshotMapper = new SnapshotMapper();
    mapDriver = MapDriver.newMapDriver(snapshotMapper);
    Configuration conf = new Configuration();
    mapDriver
        .getConfiguration()
        .setStrings(
            "io.serializations",
            conf.get("io.serializations"),
            MutationSerialization.class.getName(),
            ResultSerialization.class.getName());
  }

  @Test
  public void testSnapshotMapper() throws IOException {
    int rowCount = 20;
    int cellCount = 10;
    byte[] row = Bytes.toBytes("row");
    byte[] columnFamily = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    byte[] value = Bytes.toBytes("v");
    long ts = 123L;

    ImmutableBytesWritable key = new ImmutableBytesWritable(row);

    for (int h = 0; h < rowCount; h++) {
      List<Cell> cellList = new ArrayList<>();
      for (int i = 0; i < cellCount; i++) {
        KeyValue kv = new KeyValue(row, columnFamily, qualifier, ts, value);
        cellList.add(kv);
      }

      Result res = Result.create(cellList);
      mapDriver.addInput(new Pair<>(key, res));
    }

    List<Pair<ImmutableBytesWritable, Put>> resultList = mapDriver.run();
    Assert.assertEquals(rowCount, resultList.size());

    int cellResCount = 0;
    for (Pair<ImmutableBytesWritable, Put> r : resultList) {
      CellScanner s = r.getSecond().cellScanner();
      while (s.advance()) {
        Cell resultCell = s.current();
        Assert.assertTrue(CellUtil.matchingRow(resultCell, row));
        Assert.assertTrue(CellUtil.matchingFamily(resultCell, columnFamily));
        Assert.assertTrue(CellUtil.matchingQualifier(resultCell, qualifier));
        Assert.assertTrue(CellUtil.matchingValue(resultCell, value));
        Assert.assertEquals(ts, resultCell.getTimestamp());

        cellResCount++;
      }
    }
    Assert.assertEquals((rowCount * cellCount), cellResCount);

    // verify counters
    Counters counters = mapDriver.getCounters();
    long numRows = counters.findCounter(ScanCounter.NUM_ROWS).getValue();
    long numCells = counters.findCounter(ScanCounter.NUM_CELLS).getValue();
    Assert.assertEquals(rowCount, numRows);
    Assert.assertEquals((rowCount * cellCount), numCells);
  }

  @Test
  public void testRowExceedingMaxCells() throws IOException {
    int cellCount = SnapshotMapper.MAX_CELLS + 100;
    byte[] row = Bytes.toBytes("row");
    ImmutableBytesWritable key = new ImmutableBytesWritable(row);
    long ts = 123L;
    List<Cell> cellList = new ArrayList<>();
    for (int i = 0; i < cellCount; i++) {
      KeyValue kv =
          new KeyValue(row, Bytes.toBytes("f"), Bytes.toBytes("q"), ts, Bytes.toBytes("v"));
      cellList.add(kv);
    }

    Result res = Result.create(cellList);
    Pair<ImmutableBytesWritable, Result> input = new Pair<>(key, res);
    mapDriver.addInput(input);

    List<Pair<ImmutableBytesWritable, Put>> resultList = mapDriver.run();
    Assert.assertEquals(2, resultList.size());

    int cellResCount = 0;
    for (Pair<ImmutableBytesWritable, Put> r : resultList) {
      CellScanner s = r.getSecond().cellScanner();
      while (s.advance()) {
        cellResCount++;
      }
    }
    Assert.assertEquals(cellCount, cellResCount);

    // verify counters
    Counters counters = mapDriver.getCounters();
    long numRows = counters.findCounter(ScanCounter.NUM_ROWS).getValue();
    long numCells = counters.findCounter(ScanCounter.NUM_CELLS).getValue();
    Assert.assertEquals(1, numRows);
    Assert.assertEquals(cellCount, numCells);
  }
}
