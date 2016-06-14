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
package com.google.cloud.bigtable.hbase.adapters.filters;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.RowFilter;

@RunWith(JUnit4.class)
public class TestColumnRangeFilterAdapter {

  ColumnRangeFilterAdapter filterAdapter = new ColumnRangeFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan, null);

  @Test
  public void testColumnRangeFilterThrowsWithNoFamilies() {
    ColumnRangeFilter filter = new ColumnRangeFilter(
        Bytes.toBytes("a"), true, Bytes.toBytes("b"), true);
    Assert.assertFalse(filterAdapter.isFilterSupported(emptyScanContext, filter).isSupported());
  }

  @Test
  public void testColumnRangeFilterWithASingleFamily() throws IOException {
    ColumnRangeFilter filter = new ColumnRangeFilter(
        Bytes.toBytes("a"), true, Bytes.toBytes("b"), false);
    Scan familyScan = new Scan().addFamily(Bytes.toBytes("foo"));
    RowFilter rowFilter = filterAdapter.adapt(
        new FilterAdapterContext(familyScan, null), filter);

    Assert.assertEquals(
        "a",
        Bytes.toString(
            rowFilter
                .getColumnRangeFilter()
                .getStartQualifierClosed()
                .toByteArray()));

    Assert.assertEquals(
        "b",
        Bytes.toString(
            rowFilter
                .getColumnRangeFilter()
                .getEndQualifierOpen()
                .toByteArray()));
  }
}
