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

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
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
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setChain(
                Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerColumnLimitFilter(1))
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerRowOffsetFilter(20))
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerRowLimitFilter(10)))
            .build(),
        adaptedFilter);
  }

  @Test
  public void zeroOffsetLimitIsSupported() throws IOException {
    ColumnPaginationFilter filter = new ColumnPaginationFilter(10, 0);
    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan(), null), filter);
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setChain(
                Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerColumnLimitFilter(1))
                    .addFilters(RowFilter.newBuilder()
                        .setCellsPerRowLimitFilter(10)))
            .build(),
        adaptedFilter);
  }

  @Test
  public void qualifierOffsetIsPartiallySupported() throws IOException {
    ColumnPaginationFilter filter =
        new ColumnPaginationFilter(10, Bytes.toBytes("q1"));
    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(
            new Scan().addFamily(Bytes.toBytes("f1")), null),
        filter);
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setChain(
                Chain.newBuilder()
                    .addFilters(
                        RowFilter.newBuilder()
                            .setCellsPerColumnLimitFilter(1))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setColumnRangeFilter(
                                ColumnRange.newBuilder()
                                    .setFamilyName("f1")
                                    .setStartQualifierClosed(
                                        ByteString.copyFromUtf8("q1"))))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setCellsPerRowLimitFilter(10)))
            .build(),
        adaptedFilter);
  }
}
