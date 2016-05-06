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

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.common.base.Optional;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;

@RunWith(JUnit4.class)
public class TestFilterListAdapter {

  // Adapting a filterlist is a cooperative between the filter list adapter
  // and the filter adapter.
  FilterAdapter filterAdapter = FilterAdapter.buildAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = null;

  @Before
  public void setup() {
    emptyScanContext = new FilterAdapterContext(emptyScan, null);
  }

  FilterList makeFilterList(Operator filterOperator) {
    return new FilterList(
        filterOperator,
        new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("value"))),
        new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("value2"))));
  }

  @Test
  public void interleavedFiltersAreAdapted() throws IOException {
    FilterList filterList = makeFilterList(Operator.MUST_PASS_ONE);
    RowFilter rowFilter = filterAdapter.adaptFilter(emptyScanContext, filterList).get();
    Assert.assertEquals(
        "value",
        rowFilter.getInterleave().getFilters(0).getValueRegexFilter().toStringUtf8());
    Assert.assertEquals(
        "value2",
        rowFilter.getInterleave().getFilters(1).getValueRegexFilter().toStringUtf8());
  }

  @Test
  public void chainedFiltersAreAdapted() throws IOException {
    FilterList filterList = makeFilterList(Operator.MUST_PASS_ALL);
    RowFilter rowFilter = filterAdapter.adaptFilter(emptyScanContext, filterList).get();
    Assert.assertEquals(
        "value",
        rowFilter.getChain().getFilters(0).getValueRegexFilter().toStringUtf8());
    Assert.assertEquals(
        "value2",
        rowFilter.getChain().getFilters(1).getValueRegexFilter().toStringUtf8());
  }

  @Test
  public void compositeFilterSupportStatusIsReturnedForUnsupportedChildFilters() {
    FilterListAdapter filterListAdapter = new FilterListAdapter(new FilterAdapter() {
      @Override
      public void collectUnsupportedStatuses(FilterAdapterContext context, Filter filter,
          List<FilterSupportStatus> statuses) {
        Assert.assertEquals(
            "FilterListDepth should be incremented in isFilterSupported.",
            1,
            context.getFilterListDepth());
        statuses.add(FilterSupportStatus.newNotSupported("Test"));
      }
    });

    FilterList filterList = makeFilterList(Operator.MUST_PASS_ALL);
    FilterSupportStatus status = filterListAdapter.isFilterSupported(emptyScanContext, filterList);
    Assert.assertFalse(
        "collectUnsupportedStatuses should have been invoked returning unsupported statuses.",
        status.isSupported());
  }

  @Test
  public void collectUnsupportedStatusesStartsANewContext() {
    FilterListAdapter filterListAdapter = new FilterListAdapter(new FilterAdapter() {
      @Override
      public void collectUnsupportedStatuses(FilterAdapterContext context, Filter filter,
          List<FilterSupportStatus> statuses) {
        Assert.assertEquals(
            "FilterListDepth should be incremented in isFilterSupported.",
            1,
            context.getFilterListDepth());
        statuses.add(FilterSupportStatus.newNotSupported("Test"));
      }
    });

    Assert.assertEquals("Initial depth should be 0.", 0, emptyScanContext.getFilterListDepth());
    FilterList filterList = makeFilterList(Operator.MUST_PASS_ALL);
    FilterSupportStatus status = filterListAdapter.isFilterSupported(emptyScanContext, filterList);
    Assert.assertFalse(
        "collectUnsupportedStatuses should have been invoked returning unsupported statuses.",
        status.isSupported());
  }

  @Test
  /**
   * FilterListAdapter should handle the fact that PageFilterAdapter returns null.
   */
  public void testPageFilter() throws IOException {
    byte[] qualA = Bytes.toBytes("qualA");
    PageFilter pageFilter = new PageFilter(20);
    FilterList filterList = new FilterList(
      Operator.MUST_PASS_ALL,
      new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(qualA)),
      pageFilter);
    FilterAdapter adapter = FilterAdapter.buildAdapter();
    Optional<RowFilter> adapted =
        adapter.adaptFilter(new FilterAdapterContext(new Scan(), new DefaultReadHooks()),
          filterList);
    Assert.assertTrue(adapted.isPresent());
    Optional<RowFilter> qualifierAdapted =
        adapter.adaptFilter(new FilterAdapterContext(new Scan(), new DefaultReadHooks()),
          filterList.getFilters().get(0));
    Assert.assertEquals(qualifierAdapted.get(), adapted.get());
  }
}
