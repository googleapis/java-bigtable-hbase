/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
