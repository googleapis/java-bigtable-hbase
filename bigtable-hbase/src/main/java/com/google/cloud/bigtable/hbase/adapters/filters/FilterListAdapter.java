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
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapts a FilterList into either a RowFilter with chaining or interleaving.
 */
public class FilterListAdapter
    implements TypedFilterAdapter<FilterList>, UnsupportedStatusCollector<FilterList> {
  private final FilterAdapter subFilterAdapter;

  public FilterListAdapter(FilterAdapter subFilterAdapter) {
    this.subFilterAdapter = subFilterAdapter;
  }

  @Override
  public RowFilter adapt(FilterAdapterContext context, FilterList filter) throws IOException {
    if (filter.getOperator() == Operator.MUST_PASS_ALL) {
      return filterMustPassAll(context, filter);
    } else {
      return filterMustPassOne(context, filter);
    }
  }

  private RowFilter filterMustPassOne(FilterAdapterContext context, FilterList filter)
      throws IOException {
    Interleave.Builder interLeaveBuilder = Interleave.newBuilder();
    for (Filter subFilter : filter.getFilters()) {
      interLeaveBuilder.addFilters(subFilterAdapter.adaptFilter(context, subFilter));
    }
    return RowFilter.newBuilder()
        .setInterleave(interLeaveBuilder)
        .build();
  }

  private RowFilter filterMustPassAll(FilterAdapterContext context, FilterList filter)
      throws IOException {
    Chain.Builder chainBuilder = Chain.newBuilder();
    for (Filter subFilter : filter.getFilters()) {
      chainBuilder.addFilters(subFilterAdapter.adaptFilter(context, subFilter));
    }
    return RowFilter.newBuilder()
        .setChain(chainBuilder)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      FilterList filter) {
    List<FilterSupportStatus> unsupportedSubfilters = new ArrayList<>();
    collectUnsupportedStatuses(context, filter, unsupportedSubfilters);
    if (!unsupportedSubfilters.isEmpty()) {
      return FilterSupportStatus.SUPPORTED;
    } else {
      return FilterSupportStatus.newCompositeNotSupported(unsupportedSubfilters);
    }
  }

  @Override
  public void collectUnsupportedStatuses(
      FilterAdapterContext context,
      FilterList filter,
      List<FilterSupportStatus> unsupportedStatuses) {
    for (Filter subFilter : filter.getFilters()) {
      subFilterAdapter.collectUnsupportedStatuses(context, subFilter, unsupportedStatuses);
    }
  }
}
