package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestValueFilterAdapter {
  ValueFilterAdapter adapter = new ValueFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan);

  @Test
  public void testValueFilterFiltersOnValue() throws IOException {
    byte[] filterValue = Bytes.toBytes("foobar");

    ValueFilter filter = new ValueFilter(
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    RowFilter rowFilter = adapter.adapt(emptyScanContext, filter);
    Assert.assertEquals("foobar",
        Bytes.toString(rowFilter.getValueRegexFilter().toByteArray()));
  }
}
