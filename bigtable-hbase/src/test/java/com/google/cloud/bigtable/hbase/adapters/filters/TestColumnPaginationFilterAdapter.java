package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
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
        new FilterAdapterContext(new Scan()), filter);
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
        new FilterAdapterContext(new Scan()), filter);
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
            new Scan().addFamily(Bytes.toBytes("f1"))),
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
                                    .setStartQualifierInclusive(
                                        ByteString.copyFromUtf8("q1"))))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setCellsPerRowLimitFilter(10)))
            .build(),
        adaptedFilter);
  }
}
