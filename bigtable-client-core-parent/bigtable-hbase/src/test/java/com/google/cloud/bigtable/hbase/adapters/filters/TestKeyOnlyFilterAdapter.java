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
package com.google.cloud.bigtable.hbase.adapters.filters;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.RowFilter.Chain;
import com.google.cloud.bigtable.data.v2.models.Filters;

@RunWith(JUnit4.class)
public class TestKeyOnlyFilterAdapter {

  KeyOnlyFilterAdapter filterAdapter = new KeyOnlyFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan, null);

  @Test
  public void stripValuesIsApplied() throws IOException {
    KeyOnlyFilter filter = new KeyOnlyFilter();
    Filters.Filter expectedFilter = filterAdapter.adapt(emptyScanContext, filter);
    Chain chain = expectedFilter.toProto().getChain();
    Assert.assertTrue(
        chain.getFilters(0).getStripValueTransformer()
            || chain.getFilters(1).getStripValueTransformer());
    Filters.Filter filters = filterAdapter.adapt(emptyScanContext, filter);
    Filters f = Filters.FILTERS;
    Assert.assertEquals(filters.toProto(),
        f.chain()
            .filter(f.limit().cellsPerColumn(1))
            .filter(f.value().strip()).toProto());
  }

  @Test
  public void lengthAsValIsNotSupported() {
    KeyOnlyFilter filter = new KeyOnlyFilter(true);
    Assert.assertFalse(
        filterAdapter.isFilterSupported(emptyScanContext, filter).isSupported());
  }
}
