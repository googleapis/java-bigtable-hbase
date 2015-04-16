package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestColumnCountGetFilterAdapter {

  ColumnCountGetFilterAdapter adapter = new ColumnCountGetFilterAdapter();

  @Test
  public void testSimpleColumnCount() throws IOException {
    RowFilter adaptedFilter =
        adapter.adapt(
            new FilterAdapterContext(new Scan()),
            new ColumnCountGetFilter(2));
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setChain(Chain.newBuilder()
                .addFilters(
                    RowFilter.newBuilder()
                        .setCellsPerColumnLimitFilter(1))
                .addFilters(
                    RowFilter.newBuilder()
                        .setCellsPerRowLimitFilter(2)))
        .build(),
        adaptedFilter);
  }
}
