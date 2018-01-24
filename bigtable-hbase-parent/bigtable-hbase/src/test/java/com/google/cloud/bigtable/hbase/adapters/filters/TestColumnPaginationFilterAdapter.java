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

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.F;

import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestColumnPaginationFilterAdapter {

  ColumnPaginationFilterAdapter adapter = new ColumnPaginationFilterAdapter();

  @Test
  public void integerLimitsAreApplied() throws IOException {
    ColumnPaginationFilter filter = new ColumnPaginationFilter(10, 20);
    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan(), null), filter);
    RowFilter expected = F.chain()
          .filter(F.limit().cellsPerColumn(1))
          .filter(F.offset().cellsPerRow(20))
          .filter(F.limit().cellsPerRow(10))
          .toProto();
    Assert.assertEquals(expected, adaptedFilter);
  }

  @Test
  public void zeroOffsetLimitIsSupported() throws IOException {
    ColumnPaginationFilter filter = new ColumnPaginationFilter(10, 0);
    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan(), null), filter);
    RowFilter expected = F.chain()
        .filter(F.limit().cellsPerColumn(1))
        .filter(F.limit().cellsPerRow(10))
        .toProto();
    Assert.assertEquals(expected, adaptedFilter);
  }

  @Test
  public void qualifierOffsetIsPartiallySupported() throws IOException {
    Scan scan = new Scan().addFamily(Bytes.toBytes("f1"));
    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(scan, null),
        new ColumnPaginationFilter(10, Bytes.toBytes("q1")));
    RowFilter expected = F.chain()
        .filter(F.limit().cellsPerColumn(1))
        .filter(F.qualifier().rangeWithinFamily("f1")
            .startClosed(ByteString.copyFromUtf8("q1")))
        .filter(F.limit().cellsPerRow(10))
        .toProto();
    Assert.assertEquals(expected, adaptedFilter);
  }
}
