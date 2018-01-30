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

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.FILTERS;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableRangeSet.Builder;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

/**
 * Adapter for {@link MultiRowRangeFilter}, it converts the filter into an index scan hint
 */
public class MultiRowRangeFilterAdapter extends TypedFilterAdapterBase<MultiRowRangeFilter>  {
  private static final FilterSupportStatus NO_MUST_PASS_ONE =
      FilterSupportStatus.newNotSupported(
          "MultiRowRange filters can not be contained in MUST_PASS_ONE FilterLists");

  @Override
  public RowFilter adapt(FilterAdapterContext context, MultiRowRangeFilter filter)
      throws IOException {

    return FILTERS.pass().toProto();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      MultiRowRangeFilter filter) {

    // Since this filter only affects the top level row ranges, it can't support arbitrary usage.
    // Specifically, it can't 'or' filter lists. Cases like:
    // MUST_PASS_ONE( MultiRange(a-b), ColumnPrefix('c'))
    // will exclude the all cells out of the key range a-b that whose column starts with c
    for (FilterList filterList : context.getCurrentFilterLists()) {
      if (filterList.getOperator() == Operator.MUST_PASS_ONE) {
        return NO_MUST_PASS_ONE;
      }
    }
    return FilterSupportStatus.SUPPORTED;
  }

  @Override
  public RangeSet<RowKeyWrapper> getIndexScanHint(MultiRowRangeFilter filter) {
    Builder<RowKeyWrapper> rangeSetBuilder = ImmutableRangeSet.builder();

    for (RowRange rowRange : filter.getRowRanges()) {
      rangeSetBuilder.add(rowRangeToRange(rowRange));
    }
    return rangeSetBuilder.build();
  }

  private Range<RowKeyWrapper> rowRangeToRange(RowRange rowRange) {
    boolean startUnbounded = HConstants.EMPTY_BYTE_ARRAY.equals(rowRange.getStartRow());
    RowKeyWrapper start = new RowKeyWrapper(ByteString.copyFrom(rowRange.getStartRow()));
    BoundType startboundType = rowRange.isStartRowInclusive() ? BoundType.CLOSED : BoundType.OPEN;

    boolean stopUnbounded = HConstants.EMPTY_BYTE_ARRAY.equals(rowRange.getStopRow());
    RowKeyWrapper stop = new RowKeyWrapper(ByteString.copyFrom(rowRange.getStopRow()));
    BoundType stopboundType = rowRange.isStopRowInclusive() ? BoundType.CLOSED : BoundType.OPEN;

    if (startUnbounded && stopUnbounded) {
      return Range.all();
    }
    else if (startUnbounded) {
      return Range.upTo(stop, stopboundType);
    }
    else if (stopUnbounded) {
      return Range.downTo(start, startboundType);
    }
    else {
      return Range.range(start, startboundType, stop, stopboundType);
    }
  }
}
