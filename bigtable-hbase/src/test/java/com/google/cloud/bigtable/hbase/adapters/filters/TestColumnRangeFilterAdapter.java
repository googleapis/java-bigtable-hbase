package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@RunWith(JUnit4.class)
public class TestColumnRangeFilterAdapter {

  ColumnRangeFilterAdapter filterAdapter = new ColumnRangeFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan);

  @Test
  public void testColumnRangeFilterThrowsWithNoFamilies() throws IOException {
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
        new FilterAdapterContext(familyScan), filter);

    Assert.assertEquals(
        "a",
        Bytes.toString(
            rowFilter
                .getColumnRangeFilter()
                .getStartQualifierInclusive()
                .toByteArray()));

    Assert.assertEquals(
        "b",
        Bytes.toString(
            rowFilter
                .getColumnRangeFilter()
                .getEndQualifierExclusive()
                .toByteArray()));
  }
}
