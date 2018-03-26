/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.wrappers.Filters;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.hbase.filter.BigtableFilter;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;

public class TestFilters extends AbstractTestFilters {
  
  @Test
  public void testTimestampRangeFilter() throws IOException {
    // Initialize
    int numCols = 10;
    String goodValue = "includeThisValue";
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testRow-TimestampRange-");
    Put put = new Put(rowKey);
    for (int i = 0; i < numCols; ++i) {
      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), i, Bytes.toBytes(goodValue));
    }
    table.put(put);

    // Filter for results
    Filter filter = new TimestampRangeFilter(4, 6);

    Get get = new Get(rowKey).setFilter(filter);
    Result result = table.get(get);
    Cell[] cells = result.rawCells();
    Assert.assertEquals("Should have three cells, timestamps 4 and 5.", 2, cells.length);

    // Since the qualifiers are random, ignore the order of the returned cells.
    long[] timestamps =
        new long[] { cells[0].getTimestamp(), cells[1].getTimestamp() };
    Arrays.sort(timestamps);
    Assert.assertArrayEquals(new long[] { 4L, 5L }, timestamps);

    table.close();
  }
  
  @Test
  public void testBigtableFilter() throws IOException {
    if (!sharedTestEnv.isBigtable()) {
      return;
    }

    byte[] rowKey = dataHelper.randomData("cbt-filter-");
    byte[] qualA = Bytes.toBytes("a");
    byte[] qualB = Bytes.toBytes("b");
    byte[] valA = dataHelper.randomData("a");
    byte[] valB = dataHelper.randomData("b");

    try(Table table = getDefaultTable()){
      table.put(new Put(rowKey)
        .addColumn(COLUMN_FAMILY, qualA, valA)
        .addColumn(COLUMN_FAMILY, qualB, valB));

      Filters.Filter qualAFilter =
          Filters.FILTERS.qualifier().exactMatch(ByteString.copyFrom(qualA));
      BigtableFilter bigtableFilter = new BigtableFilter(qualAFilter);
      Result result = table.get(new Get(rowKey).setFilter(bigtableFilter));

      Assert.assertEquals(1, result.size());
      Assert.assertTrue(CellUtil.matchingValue(result.rawCells()[0], valA));
    }
  }
  
  /**
   * This test case is used to validate TimestampRangeFilter with Integer.MAX_VALUE #1552
   * 
   * @throws IOException
   */
  @Test
  public void testTimestampRangeFilterWithMaxVal() throws IOException {
	    // Initialize
	    long numCols = Integer.MAX_VALUE;
	    long start = Integer.MAX_VALUE - 2;
	    String goodValue = "includeThisValue";
	    Table table = getDefaultTable();
	    byte[] rowKey = dataHelper.randomData("testRow-TimestampRange-");
	    Put put = new Put(rowKey);
	    for (long i = start; i < numCols; ++i) {
	      put.addColumn(COLUMN_FAMILY, dataHelper.randomData(""), i, Bytes.toBytes(goodValue));
	    }
	    table.put(put);

	    Filter filter = new TimestampRangeFilter(start, Integer.MAX_VALUE);
	    
	    Get get = new Get(rowKey).setFilter(filter);
	    Result result = table.get(get);
	    Cell[] cells = result.rawCells();
	    Assert.assertEquals("Should have all cells.", 2, cells.length);

	    long[] timestamps =
	        new long[] { cells[0].getTimestamp(), cells[1].getTimestamp() };
	    Arrays.sort(timestamps);
	    Assert.assertArrayEquals(new long[] { start, Integer.MAX_VALUE-1 }, timestamps);

	    table.close();
  }

  @Override
  protected void getGetAddVersion(Get get, int version) throws IOException {
    get.setMaxVersions(version);
  }

  @Override
  protected void scanAddVersion(Scan scan, int version){
    scan.setMaxVersions(version);
  }
}
