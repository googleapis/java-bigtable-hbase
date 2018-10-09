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
package com.google.cloud.bigtable.hbase.adapters.read;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest.Builder;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Lightweight tests for the ScanAdapter. Many of the methods, such as filter building are
 * already tested in {@link TestGetAdapter}.
 */
@RunWith(JUnit4.class)
public class TestScanAdapter {

  private final static ScanAdapter scanAdapter = new ScanAdapter(
      FilterAdapter.buildAdapter(), new RowRangeAdapter()
  );
  private final static ReadHooks throwingReadHooks = new ReadHooks() {
    @Override
    public void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook) {
      throw new IllegalStateException("Read hooks not supported in TestScanAdapter.");
    }

    @Override
    public ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest) {
      throw new IllegalStateException("Read hooks not supported in TestScanAdapter.");
    }
  };

  private static RowRange toRange(byte[] start, byte[] stop) {
    return RowRange.newBuilder().setStartKeyClosed(ByteStringer.wrap(start))
        .setEndKeyOpen(ByteStringer.wrap(stop)).build();
  }

  private static RowSet toRowSet(RowRange range) {
    return RowSet.newBuilder().addRowRanges(range).build();
  }

  private static byte[] calculatePrefixEnd(byte[] prefix) {
    byte[] prefixEnd = new byte[prefix.length];
    System.arraycopy(prefix, 0, prefixEnd, 0, prefixEnd.length);
    prefixEnd[prefixEnd.length - 1]++;
    return prefixEnd;
  }

  @Test
  public void testNewScan() {
    Scan scan = new Scan();
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    Assert.assertEquals(toRowSet(RowRange.getDefaultInstance()),
      request.getRows());
  }

  @Test
  public void testStartDefault() {
    byte[] startKey = Bytes.toBytes("startKey");
    Scan scan = new Scan().withStartRow(startKey);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowSet expected = toRowSet(
        RowRange.newBuilder().setStartKeyClosed(ByteString.copyFrom(startKey)).build());
    Assert.assertEquals(expected, request.getRows());
  }

  @Test
  public void testStartInclusive() {
    byte[] startKey = Bytes.toBytes("startKey");
    Scan scan = new Scan().withStartRow(startKey, true);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowSet expected = toRowSet(
        RowRange.newBuilder().setStartKeyClosed(ByteString.copyFrom(startKey)).build());
    Assert.assertEquals(expected, request.getRows());
  }

  @Test
  public void testStartExclusive() {
    byte[] startKey = Bytes.toBytes("startKey");
    Scan scan = new Scan().withStartRow(startKey, false);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowSet expected = toRowSet(
        RowRange.newBuilder().setStartKeyOpen(ByteString.copyFrom(startKey)).build());
    Assert.assertEquals(expected, request.getRows());
  }

  @Test
  public void testStopDefault() {
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan().withStopRow(stopKey);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowSet expected = toRowSet(
        RowRange.newBuilder().setEndKeyOpen(ByteString.copyFrom(stopKey)).build());
    Assert.assertEquals(expected, request.getRows());
  }

  @Test
  public void testStopInclusive() {
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan().withStopRow(stopKey, true);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowSet expected = toRowSet(
        RowRange.newBuilder().setEndKeyClosed(ByteString.copyFrom(stopKey)).build());
    Assert.assertEquals(expected, request.getRows());
  }

  @Test
  public void testStopExclusive() {
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan().withStopRow(stopKey, false);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowSet expected = toRowSet(
        RowRange.newBuilder().setEndKeyOpen(ByteString.copyFrom(stopKey)).build());
    Assert.assertEquals(expected, request.getRows());
  }

  @Test
  public void testStartAndEndKeysAreSet() {
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan()
        .withStartRow(startKey)
        .withStopRow(stopKey);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    Assert.assertEquals(toRowSet(toRange(startKey, stopKey)), request.getRows());
  }

  @Test
  public void testStartAndEndKeysNonDefault() {
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan()
        .withStartRow(startKey, false)
        .withStopRow(stopKey, true);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    Assert.assertEquals(toRowSet(
        RowRange.newBuilder().setStartKeyOpen(ByteStringer.wrap(startKey))
            .setEndKeyClosed(ByteStringer.wrap(stopKey)).build()), request.getRows());
  }

  @Test
  public void testPrefix() {
    byte[] prefix = Bytes.toBytes("prefix");
    byte[] prefixEnd = calculatePrefixEnd(prefix);
    Scan scan = new Scan();
    scan.setRowPrefixFilter(prefix);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    Assert.assertEquals(toRowSet(toRange(prefix, prefixEnd)), request.getRows());
  }

  @Test
  public void maxVersionsIsSet() {
    Scan scan = new Scan();
    scan.setMaxVersions(10);
    ReadRowsRequest.Builder rowRequestBuilder = scanAdapter.adapt(scan, throwingReadHooks);
    Assert.assertEquals(
        FILTERS.limit().cellsPerColumn(10).toProto(),
        rowRequestBuilder.getFilter());
  }

  @Test
  public void testExtendedScan(){
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");

    byte[] startRow = Bytes.toBytes("startKey");
    byte[] stopRow = Bytes.toBytes("stopKey");

    byte[] prefix = Bytes.toBytes("prefix");
    byte[] prefixEnd = calculatePrefixEnd(prefix);

    BigtableExtendedScan scan = new BigtableExtendedScan();
    scan.addRowKey(row1);
    scan.addRowKey(row2);
    scan.addRange(startRow, stopRow);
    scan.addRangeWithPrefix(prefix);

    RowSet expected = RowSet.newBuilder()
        .addRowKeys(ByteStringer.wrap(row1))
        .addRowKeys(ByteStringer.wrap(row2))
        .addRowRanges(toRange(startRow, stopRow))
        .addRowRanges(toRange(prefix, prefixEnd))
        .build();

    Assert.assertEquals(expected, scanAdapter.adapt(scan, throwingReadHooks).getRows());
  }

  @Test
  public void testNarrowedScan() throws IOException {
    FilterAdapter filterAdapter = Mockito.mock(FilterAdapter.class);
    ScanAdapter scanAdapter = new ScanAdapter(filterAdapter, new RowRangeAdapter());

    Filter fakeFilter = new FilterBase() {
      @Override
      public ReturnCode filterKeyValue(Cell v) throws IOException {
        return ReturnCode.INCLUDE;
      }
    };

    RangeSet<RowKeyWrapper> rangeSet = ImmutableRangeSet.of(
        Range.closedOpen(
            new RowKeyWrapper(ByteString.copyFromUtf8("b")),
            new RowKeyWrapper(ByteString.copyFromUtf8("d"))
        )
    );
    Mockito.when(filterAdapter.getIndexScanHint(any(Filter.class))).thenReturn(rangeSet);
    Mockito.when(filterAdapter.adaptFilter(any(FilterAdapterContext.class), eq(fakeFilter)))
        .thenReturn(Optional.of(RowFilter.getDefaultInstance()));

    Scan scan = new Scan()
        .withStartRow("a".getBytes())
        .withStopRow("z".getBytes())
        .setFilter(fakeFilter);

    Builder adapted = scanAdapter.adapt(scan, throwingReadHooks);

    Assert.assertEquals(
        RowSet.newBuilder()
            .addRowRanges(
                RowRange.newBuilder()
                    .setStartKeyClosed(ByteString.copyFromUtf8("b"))
                    .setEndKeyOpen(ByteString.copyFromUtf8("d"))
            )
            .build(),
        adapted.getRows()
    );
  }

  @Test
  /** Make sure that the scan rowSet is unaffected when the filter is not set */
  public void testNarrowedScanWithoutFilter() {
    Scan scan = new Scan()
        .withStartRow("a".getBytes())
        .withStopRow("z".getBytes());

    RowSet result = scanAdapter.adapt(scan, throwingReadHooks).build().getRows();
    RowSet expected = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(ByteString.copyFromUtf8("a"))
                .setEndKeyOpen(ByteString.copyFromUtf8("z"))
        )
        .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testEmptyFilterList(){
    Scan scan = new Scan()
        .setFilter(new FilterList())
        .withStartRow("a".getBytes())
        .withStopRow("z".getBytes());

    RowSet result = scanAdapter.adapt(scan, throwingReadHooks).build().getRows();
    RowSet expected = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(ByteString.copyFromUtf8("a"))
                .setEndKeyOpen(ByteString.copyFromUtf8("z"))
        )
        .build();

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testLimit() {
    Scan scan = new Scan().setLimit(10);
    int adaptedLimit = (int) scanAdapter.adapt(scan, throwingReadHooks).build().getRowsLimit();
    Assert.assertEquals(scan.getLimit(), adaptedLimit);
  }
}
