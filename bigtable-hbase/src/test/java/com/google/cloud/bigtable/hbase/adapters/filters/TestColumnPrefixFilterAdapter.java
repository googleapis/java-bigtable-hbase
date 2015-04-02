package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestColumnPrefixFilterAdapter {

  ColumnPrefixFilterAdapter adapter = new ColumnPrefixFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan);

  @Test
  public void columnPrefixFiltersAreAdapted() throws IOException {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("prefix"));
    RowFilter rowFilter = adapter.adapt(emptyScanContext, filter);
    Assert.assertEquals(
        "prefix\\C*",
        Bytes.toString(rowFilter.getColumnQualifierRegexFilter().toByteArray()));
  }
}
