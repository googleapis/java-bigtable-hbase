package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.read.RowRangeAdapter;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

/**
 * Adapter for {@link MultiRowRangeFilter}, it converts the filter into an index scan hint
 */
public class MultiRowRangeFilterAdapter extends TypedFilterAdapterBase<MultiRowRangeFilter>  {
  @Override
  public RowFilter adapt(FilterAdapterContext context, MultiRowRangeFilter filter)
      throws IOException {

    return RowFilter.newBuilder()
        .setPassAllFilter(true)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context,
      MultiRowRangeFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }

  @Override
  public RangeSet<RowKeyWrapper> getIndexScanHint(MultiRowRangeFilter filter) {
    TreeRangeSet<RowKeyWrapper> rangeSet = TreeRangeSet.create();

    for (RowRange rowRange : filter.getRowRanges()) {
      rangeSet.add(rowRangeToRange(rowRange));
    }
    return super.getIndexScanHint(filter);
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
