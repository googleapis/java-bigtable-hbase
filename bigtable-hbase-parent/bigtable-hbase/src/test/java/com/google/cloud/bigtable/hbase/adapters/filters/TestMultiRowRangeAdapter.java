/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext.ContextCloseable;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMultiRowRangeAdapter {

  private MultiRowRangeFilterAdapter adapter;
  private Scan scan;
  private FilterAdapterContext context;
  private RowFilter unaffectedRowFilter;

  @Before
  public void setup() {
    scan = new Scan();
    context = new FilterAdapterContext(scan, null);
    adapter = new MultiRowRangeFilterAdapter();
    unaffectedRowFilter = RowFilter.newBuilder()
        .setPassAllFilter(true)
        .build();
  }

  @Test
  public void testClosedSingle() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange("cc", true, "ee", true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.closed(
            new RowKeyWrapper(ByteString.copyFromUtf8("cc")),
            new RowKeyWrapper(ByteString.copyFromUtf8("ee"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testOpenSingle() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange("cc", false, "ee", false)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.open(
            new RowKeyWrapper(ByteString.copyFromUtf8("cc")),
            new RowKeyWrapper(ByteString.copyFromUtf8("ee"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testOpenClosedSingle() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange("cc", false, "ee", true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.openClosed(
            new RowKeyWrapper(ByteString.copyFromUtf8("cc")),
            new RowKeyWrapper(ByteString.copyFromUtf8("ee"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testClosedOpenSingle() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange("cc", true, "ee", false)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.closedOpen(
            new RowKeyWrapper(ByteString.copyFromUtf8("cc")),
            new RowKeyWrapper(ByteString.copyFromUtf8("ee"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testUnboundedStartSingle() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange(HConstants.EMPTY_START_ROW, true, "ee".getBytes(), true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.atMost(
            new RowKeyWrapper(ByteString.copyFromUtf8("ee"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testUnboundedStopSingle() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange("cc".getBytes(), true, HConstants.EMPTY_END_ROW, true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.atLeast(
            new RowKeyWrapper(ByteString.copyFromUtf8("cc"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testUnbounded() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Collections.singletonList(
        new RowRange(HConstants.EMPTY_START_ROW, true, HConstants.EMPTY_END_ROW, true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);
    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(Range.<RowKeyWrapper>all());
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testDisjoint() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Arrays.asList(
        new RowRange("bb", true, "cc", true),
        new RowRange("ss", true, "yy", true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.closed(
            new RowKeyWrapper(ByteString.copyFromUtf8("bb")),
            new RowKeyWrapper(ByteString.copyFromUtf8("cc"))
        ))
        .add(Range.closed(
            new RowKeyWrapper(ByteString.copyFromUtf8("ss")),
            new RowKeyWrapper(ByteString.copyFromUtf8("yy"))
        ))
        .build();
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testOverlap() throws IOException {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Arrays.asList(
        new RowRange("bb", true, "cc", true),
        new RowRange("ca", true, "yy", true)
    ));

    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(unaffectedRowFilter, adaptedFilter);

    RangeSet<RowKeyWrapper> indexScanHint = adapter.getIndexScanHint(filter);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.of(
        Range.closed(
            new RowKeyWrapper(ByteString.copyFromUtf8("bb")),
            new RowKeyWrapper(ByteString.copyFromUtf8("yy"))
        )
    );
    Assert.assertEquals(expected, indexScanHint);
  }

  @Test
  public void testInterleaveIsUnsupported() throws IOException {
    MultiRowRangeFilter rangeFilter = new MultiRowRangeFilter(Arrays.asList(
        new RowRange("b", true, "b", true)
    ));
    ColumnPrefixFilter colPrefix = new ColumnPrefixFilter("c".getBytes());
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, rangeFilter, colPrefix);

    try(ContextCloseable ignored = context.beginFilterList(filterList)) {
      Assert.assertFalse(adapter.isFilterSupported(context, rangeFilter).isSupported());
    }
  }
}
