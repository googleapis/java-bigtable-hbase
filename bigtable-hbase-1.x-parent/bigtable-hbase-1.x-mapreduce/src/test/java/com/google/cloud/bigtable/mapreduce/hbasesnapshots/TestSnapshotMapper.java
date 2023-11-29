/*
 * Copyright 2021 Google LLC
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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportHBaseSnapshotJob.ScanCounter;
import com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportHBaseSnapshotJob.SnapshotMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** test mapper for snapshot import */
public class TestSnapshotMapper {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private SnapshotMapper mappUnderTest;

  @Mock private Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context mockCtx;
  @Mock private Counter rowCounter;
  @Mock private Counter cellCounter;
  private List<Pair<ImmutableBytesWritable, Put>> resultList;

  @Before
  public void setup() throws Exception {
    mappUnderTest = new SnapshotMapper();

    when(mockCtx.getCounter(Mockito.eq(ScanCounter.NUM_ROWS))).thenReturn(rowCounter);
    when(mockCtx.getCounter(Mockito.eq(ScanCounter.NUM_CELLS))).thenReturn(cellCounter);

    resultList = new ArrayList<>();
    doAnswer(
            invocationOnMock -> {
              resultList.add(
                  Pair.newPair(
                      (ImmutableBytesWritable) invocationOnMock.getArguments()[0],
                      (Put) invocationOnMock.getArguments()[1]));
              return null;
            })
        .when(mockCtx)
        .write(Mockito.any(), Mockito.any());
  }

  @Test
  public void testSnapshotMapper() throws Exception {
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
      mappUnderTest.map(key, res, mockCtx);
    }

    Mockito.verify(mockCtx, Mockito.times(rowCount)).write(Mockito.any(), Mockito.any());

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
    Mockito.verify(rowCounter, Mockito.times(rowCount)).increment(Mockito.eq(1L));
    Mockito.verify(cellCounter, Mockito.times(rowCount)).increment(Mockito.eq((long) cellCount));
  }

  @Test
  public void testRowExceedingMaxCells() throws Exception {
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

    mappUnderTest.map(key, res, mockCtx);

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
    Mockito.verify(rowCounter, Mockito.times(1)).increment(Mockito.eq(1L));
    Mockito.verify(cellCounter, Mockito.times(1)).increment(Mockito.eq((long) cellCount));
  }
}
