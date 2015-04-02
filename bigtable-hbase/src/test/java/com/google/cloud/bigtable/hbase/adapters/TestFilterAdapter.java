package com.google.cloud.bigtable.hbase.adapters;

import com.google.cloud.bigtable.hbase.adapters.FilterAdapter.ColumnRangeFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.UnsupportedFilterException;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
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
public class TestFilterAdapter {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  FilterAdapter filterAdapter = new FilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapter.FilterContext emptyScanContext =
      new FilterAdapter.FilterContext(emptyScan);

  @Test
  public void testValueFilterFiltersOnValue() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] filterValue = Bytes.toBytes("foobar");

    ValueFilter filter = new ValueFilter(
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);

    Assert.assertArrayEquals(Bytes.toBytes("value_match({foobar})"), outputStream.toByteArray());
  }

  @Test
  public void testSingleColumnValueFilterFilters() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] filterValue = Bytes.toBytes("foobar");
    byte[] qualifier = Bytes.toBytes("someColumn");
    byte[] family = Bytes.toBytes("f");

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        family,
        qualifier,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    filter.setFilterIfMissing(false);
    filter.setLatestVersionOnly(false);
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);

    Assert.assertEquals(
            "(row_has((col({f:someColumn}, all))) ? "
                + "((row_has(((col({f:someColumn}, all)) | "
                + "(value_match({foobar})))) ? ((col({.*:\\C*}, all))))) "
                + ": ((col({.*:\\C*}, all))))",
        Bytes.toString(outputStream.toByteArray()));

    outputStream.reset();

    filter.setFilterIfMissing(false);
    filter.setLatestVersionOnly(true);
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);

    Assert.assertEquals(
        "(row_has((col({f:someColumn}, latest))) ? "
            + "((row_has(((col({f:someColumn}, latest)) | "
            + "(value_match({foobar})))) ? ((col({.*:\\C*}, all))))) "
            + ": ((col({.*:\\C*}, all))))",
        Bytes.toString(outputStream.toByteArray()));

    outputStream.reset();

    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(false);
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);

    Assert.assertEquals(
        "(row_has(((col({f:someColumn}, all)) | "
            + "(value_match({foobar})))) ? ((col({.*:\\C*}, all))))",
        Bytes.toString(outputStream.toByteArray()));

    outputStream.reset();

    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(true);
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);

    Assert.assertEquals(
        "(row_has(((col({f:someColumn}, latest)) | "
            + "(value_match({foobar})))) ? ((col({.*:\\C*}, all))))",
        Bytes.toString(outputStream.toByteArray()));

    outputStream.reset();
  }

  @Test
  public void testColumnCountGetFilter() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ColumnCountGetFilter filter = new ColumnCountGetFilter(10);
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);
    Assert.assertArrayEquals(
        Bytes.toBytes("((col({.*:\\C*}, latest)) | itemlimit(10))"), outputStream.toByteArray());
  }

  @Test
  public void testColumnPaginationFilter() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // Return 10 items after skipping 20 items.
    ColumnPaginationFilter filter = new ColumnPaginationFilter(10, 20);
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);
    Assert.assertArrayEquals(
        Bytes.toBytes("((col({.*:\\C*}, latest)) | skip_items(20) | itemlimit(10))"),
        outputStream.toByteArray());
  }

  @Test
  public void testColumnPrefixFilter() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // Return all columns in all families that are prefixed by "prefix".
    ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("prefix"));
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);
    Assert.assertArrayEquals(
        Bytes.toBytes("(col({.*:prefix.*}, all))"), outputStream.toByteArray());
  }

  @Test
  public void testMultipleColumnPrefixFilter() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // Return all columns in all families that are prefixed by "prefix" or prefix2.
    MultipleColumnPrefixFilter filter =
        new MultipleColumnPrefixFilter(
            new byte[][]{Bytes.toBytes("prefix"), Bytes.toBytes("prefix2")});
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);
    Assert.assertArrayEquals(
        Bytes.toBytes("((col({.*:prefix.*}, all)) + (col({.*:prefix2.*}, all)))"),
        outputStream.toByteArray());
  }

  @Test
  public void testTimestampsFilter() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // Return all columns in all families that are prefixed by "prefix".

    TimestampsFilter filter = new TimestampsFilter(ImmutableList.<Long>of(1L, 2L, 3L));
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);
    Assert.assertArrayEquals(
        Bytes.toBytes("((ts(1000,1000)) + (ts(2000,2000)) + (ts(3000,3000)))"),
        outputStream.toByteArray());
  }

  @Test
  public void testKeyOnlyFilter() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // Return all columns in all families that are prefixed by "prefix".

    KeyOnlyFilter filter = new KeyOnlyFilter();
    filterAdapter.adaptFilterTo(emptyScanContext, filter, outputStream);
    Assert.assertArrayEquals(
        Bytes.toBytes("strip_value()"),
        outputStream.toByteArray());
  }

  @Test
  public void testKeyOnlyFilterWithLengthAsVal() throws IOException {
    // Return all columns in all families that are prefixed by "prefix".

    KeyOnlyFilter filter = new KeyOnlyFilter(true);
    expectedException.expectMessage("KeyOnlyFilters with lenAsVal = true are not supported");
    filterAdapter.throwIfUnsupportedFilter(emptyScan, filter);
  }

  @Test
  public void testUnsupportedFilterType() {
    // Let's make a filter that there's 0% chance that we have an adapter for:
    Filter filter = new FilterBase() {
      @Override
      public ReturnCode filterKeyValue(Cell cell) throws IOException {
        return ReturnCode.INCLUDE;
      }
    };

    expectedException.expect(UnsupportedFilterException.class);
    expectedException.expectMessage("Don't know how to adapt Filter class ");
    expectedException.expectMessage("TestFilterAdapter$");
    filterAdapter.throwIfUnsupportedFilter(emptyScan, filter);
  }

  @Test
  public void testUnsupportedValueFilterCompareOp() throws IOException {
    byte[] filterValue = Bytes.toBytes("foobar");

    ValueFilter filter = new ValueFilter(
        CompareFilter.CompareOp.NOT_EQUAL,
        new BinaryComparator(filterValue));

    expectedException.expect(UnsupportedFilterException.class);
    expectedException.expectMessage(
        "Unsupported filters encountered: " +
            "FilterSupportStatus{isSupported=false, reason='CompareOp.EQUAL is the only "
            + "supported ValueFilter compareOp. Found: 'NOT_EQUAL''}");
    filterAdapter.throwIfUnsupportedFilter(emptyScan, filter);
  }

  @Test
  public void testColumnRangeFilterThrowsWithNoFamilies() throws IOException {
    ColumnRangeFilter filter = new ColumnRangeFilter(
        Bytes.toBytes("a"), true, Bytes.toBytes("b"), true);
    expectedException.expect(UnsupportedFilterException.class);
    expectedException.expectMessage(ColumnRangeFilterAdapter.UNSUPPORTED_EXCEPTION_MESSAGE);
    filterAdapter.throwIfUnsupportedFilter(emptyScan, filter);
  }

  @Test
  public void testColumnRangeFilterWithASingleFamily() throws IOException {
    ColumnRangeFilter filter = new ColumnRangeFilter(
        Bytes.toBytes("a"), true, Bytes.toBytes("b"), false);
    Scan familyScan = new Scan().addFamily(Bytes.toBytes("foo"));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    filterAdapter.adaptFilterTo(familyScan, filter, outputStream);
    Assert.assertEquals(
        "(col([{foo:a},{foo:b}),all))",
        Bytes.toString(outputStream.toByteArray()));
  }

  @Test
  /** ColumnRangeFilter doesn't accept regular expressions so don't quote them
   * like a regular expression **/
  public void testColumnRangeFilterQuoting() throws IOException {
    ColumnRangeFilter filter = new ColumnRangeFilter(
        Bytes.toBytes(".$"), true, Bytes.toBytes(".$@"), false);
    Scan familyScan = new Scan().addFamily(Bytes.toBytes("foo.bar"));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    filterAdapter.adaptFilterTo(familyScan, filter, outputStream);
    // @ is quoted to @@, all other charecters are literals
    Assert.assertEquals(
        "(col([{foo.bar:.$},{foo.bar:.$@@}),all))",
        Bytes.toString(outputStream.toByteArray()));
  }
}
