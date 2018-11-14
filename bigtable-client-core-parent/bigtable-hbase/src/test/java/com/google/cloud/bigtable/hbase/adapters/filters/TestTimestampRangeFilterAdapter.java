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

import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;

@RunWith(JUnit4.class)
public class TestTimestampRangeFilterAdapter {

  TimestampRangeFilterAdapter filterAdapter = new TimestampRangeFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan, null);

  @Test
  public void timestampFiltersAreAdapted() {
    TimestampRangeFilter filter = new TimestampRangeFilter(10L, 20L);
    Filters.Filter expectedFilter = filterAdapter.adapt(emptyScanContext, filter);
    TimestampRange expectedTimestampFilter = expectedFilter.toProto().getTimestampRangeFilter();
    Assert.assertEquals(10000L, expectedTimestampFilter.getStartTimestampMicros());
    Assert.assertEquals(20000L, expectedTimestampFilter.getEndTimestampMicros());
  }
}
