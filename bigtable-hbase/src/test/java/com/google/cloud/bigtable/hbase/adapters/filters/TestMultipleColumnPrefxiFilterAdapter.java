package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Interleave;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestMultipleColumnPrefxiFilterAdapter {

  MultipleColumnPrefixFilterAdapter filterAdapter = new MultipleColumnPrefixFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan);

  @Test
  public void multiplePrefixesAreAdapted() throws IOException {
    // Return all columns in all families that are prefixed by "prefix" or prefix2.
    MultipleColumnPrefixFilter filter =
        new MultipleColumnPrefixFilter(
            new byte[][]{Bytes.toBytes("prefix"), Bytes.toBytes("prefix2")});

    RowFilter rowFilter = filterAdapter.adapt(emptyScanContext, filter);
    Interleave interleave = rowFilter.getInterleave();
    Assert.assertEquals(2, interleave.getFiltersCount());
    Assert.assertEquals(
        "prefix\\C*",
        interleave.getFilters(0).getColumnQualifierRegexFilter().toStringUtf8());
    Assert.assertEquals(
        "prefix2\\C*",
        interleave.getFilters(1).getColumnQualifierRegexFilter().toStringUtf8());
  }
}
