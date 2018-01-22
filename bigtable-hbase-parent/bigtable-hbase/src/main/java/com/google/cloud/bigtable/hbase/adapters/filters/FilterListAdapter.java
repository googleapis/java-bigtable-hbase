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

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.filter.RowFilters;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext.ContextCloseable;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

/**
 * Adapts a FilterList into either a RowFilter with chaining or interleaving.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class FilterListAdapter
    extends TypedFilterAdapterBase<FilterList> implements UnsupportedStatusCollector<FilterList> {

  private final FilterAdapter subFilterAdapter;

  /**
   * <p>Constructor for FilterListAdapter.</p>
   *
   * @param subFilterAdapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter} object.
   */
  public FilterListAdapter(FilterAdapter subFilterAdapter) {
    this.subFilterAdapter = subFilterAdapter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowFilter adapt(FilterAdapterContext context, FilterList filter) throws IOException {
    try (ContextCloseable ignored = context.beginFilterList(filter)) {
      List<RowFilter> childFilters = collectChildFilters(context, filter);
      if (childFilters.isEmpty()) {
        return null;
      } else if (filter.getOperator() == Operator.MUST_PASS_ALL) {
        return RowFilters.RF.chain(childFilters);
      } else {
        return RowFilters.RF.interleave(childFilters);
      }
    }
  }

  List<RowFilter> collectChildFilters(FilterAdapterContext context, FilterList filter)
      throws IOException {
    List<RowFilter> result = new ArrayList<>();
    for (Filter subFilter : filter.getFilters()) {
      Optional<RowFilter> potentialFilter =
          subFilterAdapter.adaptFilter(context, subFilter);
      if (potentialFilter.isPresent()) {
        result.add(potentialFilter.get());
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      FilterList filter) {
    List<FilterSupportStatus> unsupportedSubfilters = new ArrayList<>();
    try (ContextCloseable ignored = context.beginFilterList(filter)) {
      collectUnsupportedStatuses(context, filter, unsupportedSubfilters);
    }
    if (unsupportedSubfilters.isEmpty()) {
      return FilterSupportStatus.SUPPORTED;
    } else {
      return FilterSupportStatus.newCompositeNotSupported(unsupportedSubfilters);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void collectUnsupportedStatuses(
      FilterAdapterContext context,
      FilterList filter,
      List<FilterSupportStatus> unsupportedStatuses) {
    for (Filter subFilter : filter.getFilters()) {
      subFilterAdapter.collectUnsupportedStatuses(context, subFilter, unsupportedStatuses);
    }
  }

  @Override
  public RangeSet<RowKeyWrapper> getIndexScanHint(FilterList filter) {
    final List<RangeSet<RowKeyWrapper>> childHints = collectChildHints(filter);

    if (childHints.isEmpty()) {
      return ImmutableRangeSet.of(Range.<RowKeyWrapper>all());
    }
    // Optimization
    else if (childHints.size() == 1) {
      return childHints.get(0);
    }

    TreeRangeSet<RowKeyWrapper> result = TreeRangeSet.create(childHints.get(0));

    switch (filter.getOperator()) {
      case MUST_PASS_ONE:
        // Union all
        for (int i = 1; i < childHints.size(); i++) {
          result.addAll(childHints.get(i));
        }
        break;
      case MUST_PASS_ALL:
        // Intersect all
        for (int i = 1; i < childHints.size(); i++) {
          result.removeAll(childHints.get(i).complement());
        }
        break;
      default:
        throw new IllegalStateException("Unknown operator: " + filter.getOperator());
    }
    // Wrap in an ImmutableRangeSet to keep the singleton RangeSet.all()
    return ImmutableRangeSet.copyOf(result);
  }

  private List<RangeSet<RowKeyWrapper>> collectChildHints(FilterList filter) {
    List<Filter> subFilters = filter.getFilters();

    List<RangeSet<RowKeyWrapper>> hints = new ArrayList<>(subFilters.size());
    RangeSet<RowKeyWrapper> last = null;

    for (Filter subFilter : subFilters) {
      SingleFilterAdapter<?> subAdapter = subFilterAdapter.getAdapterForFilterOrThrow(subFilter);
      RangeSet<RowKeyWrapper> subRangeSet = subAdapter.getIndexScanHint(subFilter);

      // Simple optimization to cover the case where no filters provide hints. ImmutableRangeSet use
      // a singleton to represent the universe (Range that contains all of the elements).
      // Since intersection & union with the universe is a no-op, we safely dedupe all of the
      // consecutive universes.
      if (last != subRangeSet) {
        hints.add(subRangeSet);
        last = subRangeSet;
      }
    }

    return hints;
  }
}
