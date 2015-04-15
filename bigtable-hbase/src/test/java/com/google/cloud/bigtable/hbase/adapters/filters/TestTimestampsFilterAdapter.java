package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTimestampsFilterAdapter {

  TimestampsFilterAdapter filterAdapter = new TimestampsFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan);

  @Test
  public void timestampFiltersAreAdapted() {
    // Timestamps are sorted by the filter min -> max
    TimestampsFilter filter = new TimestampsFilter(ImmutableList.of(1L, 10L, 20L));
    RowFilter rowFilter = filterAdapter.adapt(emptyScanContext, filter);
    Assert.assertEquals(3, rowFilter.getInterleave().getFiltersCount());
    Assert.assertEquals(
        10000L,
        rowFilter
            .getInterleave()
            .getFilters(1)
            .getTimestampRangeFilter()
            .getStartTimestampMicros());
    Assert.assertEquals(
        11000L,
        rowFilter
            .getInterleave()
            .getFilters(1)
            .getTimestampRangeFilter()
            .getEndTimestampMicros());
  }
}
