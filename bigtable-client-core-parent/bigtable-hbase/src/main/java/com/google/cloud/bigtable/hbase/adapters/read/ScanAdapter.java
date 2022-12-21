/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters.read;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.ChainFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.InterleaveFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.TimestampRangeFilter;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import com.google.cloud.bigtable.hbase.BigtableFixedRequestExtendedScan;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext;
import com.google.cloud.bigtable.hbase.util.RowKeyWrapper;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.base.Optional;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An adapter for {@link Scan} operation that makes use of the proto filter language.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ScanAdapter implements ReadOperationAdapter<Scan> {

  protected static final Logger LOG = LoggerFactory.getLogger(ScanAdapter.class);

  private static final int UNSET_MAX_RESULTS_PER_COLUMN_FAMILY = -1;
  private static final boolean OPEN_CLOSED_AVAILABLE = isOpenClosedAvailable();
  private static final boolean LIMIT_AVAILABLE = isLimitAvailable();

  /**
   * HBase supports include(Stop|Start)Row only at 1.4.0+, so check to make sure that the HBase
   * runtime dependency supports this feature. Specifically, Beam uses HBase 1.2.0.
   */
  private static boolean isOpenClosedAvailable() {
    try {
      new Scan().includeStopRow();
      return true;
    } catch (NoSuchMethodError e) {
      return false;
    }
  }

  private static boolean isLimitAvailable() {
    try {
      new Scan().setLimit(1);
      return true;
    } catch (NoSuchMethodError e) {
      return false;
    }
  }

  private final FilterAdapter filterAdapter;
  private final RowRangeAdapter rowRangeAdapter;

  /**
   * Constructor for ScanAdapter.
   *
   * @param filterAdapter a {@link FilterAdapter} object.
   * @param rowRangeAdapter a {@link RowRangeAdapter} object.
   */
  public ScanAdapter(FilterAdapter filterAdapter, RowRangeAdapter rowRangeAdapter) {
    this.filterAdapter = filterAdapter;
    this.rowRangeAdapter = rowRangeAdapter;
  }

  /**
   * throwIfUnsupportedScan.
   *
   * @param scan a {@link Scan} object.
   */
  public void throwIfUnsupportedScan(Scan scan) {
    if (scan.getFilter() != null) {
      filterAdapter.throwIfUnsupportedFilter(scan, scan.getFilter());
    }

    if (scan.getMaxResultsPerColumnFamily() != UNSET_MAX_RESULTS_PER_COLUMN_FAMILY) {
      throw new UnsupportedOperationException(
          "Limiting of max results per column family is not supported.");
    }
  }

  /**
   * Given a {@link Scan}, build a {@link Filters.Filter} that include matching columns
   *
   * @param scan a {@link Scan} object.
   * @param hooks a {@link ReadHooks} object.
   * @return a {@link Filters.Filter} object.
   */
  public Filters.Filter buildFilter(Scan scan, ReadHooks hooks) {
    ChainFilter chain = FILTERS.chain();

    buildStartFilter(scan).forEach(chain::filter);

    Optional<Filters.Filter> userFilter = createUserFilter(scan, hooks);
    if (userFilter.isPresent()) {
      chain.filter(userFilter.get());
    }

    Optional<Filters.Filter> colFamilyTimeFilter = createColFamilyTimeRange(scan);
    if (colFamilyTimeFilter.isPresent()) {
      chain.filter(colFamilyTimeFilter.get());
    }
    return chain;
  }

  private List<Filters.Filter> buildStartFilter(Scan scan) {
    List<Filters.Filter> filterList = new ArrayList<>();

    Optional<Filters.Filter> familyFilter = createColumnFamilyFilter(scan);
    if (familyFilter.isPresent()) {
      filterList.add(familyFilter.get());
    }

    boolean hasTimeRange = false;
    if (scan.getTimeRange() != null && !scan.getTimeRange().isAllTime()) {
      filterList.add(createTimeRangeFilter(scan.getTimeRange()));
      hasTimeRange = true;
    }

    // maxVersions should appear as early as possible, but it must appear after timeRange
    if (scan.getMaxVersions() != Integer.MAX_VALUE) {
      int i = hasTimeRange ? filterList.size() : 0;
      filterList.add(i, createColumnLimitFilter(scan.getMaxVersions()));
    }
    return filterList;
  }

  /** {@inheritDoc} */
  @Override
  public Query adapt(Scan scan, ReadHooks readHooks, Query query) {
    LOG.info("in scan adapter");
    if (scan instanceof BigtableFixedRequestExtendedScan) {
      LOG.info("retunring bigtableFixedRequestExtendedScan");
      return ((BigtableFixedRequestExtendedScan) scan).getQuery();
    } else {
      throwIfUnsupportedScan(scan);

      toByteStringRange(scan, query);
      query.filter(buildFilter(scan, readHooks));

      if (LIMIT_AVAILABLE && scan.getLimit() > 0) {
        query.limit(scan.getLimit());
      }
      return query;
    }
  }

  private void toByteStringRange(Scan scan, Query query) {
    RangeSet<RowKeyWrapper> rangeSet = narrowRange(getRangeSet(scan), scan.getFilter());
    rowRangeAdapter.rangeSetToByteStringRange(rangeSet, query);
  }

  private RangeSet<RowKeyWrapper> getRangeSet(Scan scan) {
    if (scan instanceof BigtableExtendedScan) {
      RowSet rowSet = ((BigtableExtendedScan) scan).getRowSet();
      return rowRangeAdapter.rowSetToRangeSet(rowSet);
    } else {
      RangeSet<RowKeyWrapper> rangeSet = TreeRangeSet.create();
      final ByteString startRow = ByteString.copyFrom(scan.getStartRow());
      final ByteString stopRow = ByteString.copyFrom(scan.getStopRow());

      if (scan.isGetScan()) {
        rangeSet.add(Range.singleton(new RowKeyWrapper(startRow)));
      } else {
        final BoundType startBound =
            (!OPEN_CLOSED_AVAILABLE || scan.includeStartRow()) ? BoundType.CLOSED : BoundType.OPEN;
        final BoundType endBound =
            (!OPEN_CLOSED_AVAILABLE || !scan.includeStopRow()) ? BoundType.OPEN : BoundType.CLOSED;

        rangeSet.add(rowRangeAdapter.boundedRange(startBound, startRow, endBound, stopRow));
      }
      return rangeSet;
    }
  }

  private static ByteString quoteRegex(byte[] unquoted) {
    try {
      return ReaderExpressionHelper.quoteRegularExpression(unquoted);
    } catch (IOException e) {
      throw new IllegalStateException("IOException when writing to ByteArrayOutputStream", e);
    }
  }

  private Optional<Filters.Filter> createUserFilter(Scan scan, ReadHooks hooks) {
    if (scan.getFilter() == null) {
      return Optional.absent();
    }
    try {
      return filterAdapter.adaptFilter(new FilterAdapterContext(scan, hooks), scan.getFilter());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to adapt filter", ioe);
    }
  }

  private RangeSet<RowKeyWrapper> narrowRange(RangeSet<RowKeyWrapper> rangeSet, Filter filter) {
    if (filter == null) {
      return rangeSet;
    }
    RangeSet<RowKeyWrapper> filterRangeSet = filterAdapter.getIndexScanHint(filter);
    if (filterRangeSet.encloses(Range.<RowKeyWrapper>all())) {
      return rangeSet;
    }
    rangeSet.removeAll(filterRangeSet.complement());
    return rangeSet;
  }

  private Filters.Filter createColumnQualifierFilter(byte[] unquotedQualifier) {
    return FILTERS.qualifier().regex(quoteRegex(unquotedQualifier));
  }

  private Filters.Filter createFamilyFilter(byte[] familyName) {
    return FILTERS.family().exactMatch(new String(familyName));
  }

  private Filters.Filter createColumnLimitFilter(int maxVersionsPerColumn) {
    return FILTERS.limit().cellsPerColumn(maxVersionsPerColumn);
  }

  private Filters.Filter createTimeRangeFilter(TimeRange timeRange) {
    TimestampRangeFilter rangeBuilder = FILTERS.timestamp().range();

    rangeBuilder.startClosed(convertUnits(timeRange.getMin()));

    if (timeRange.getMax() != Long.MAX_VALUE) {
      rangeBuilder.endOpen(convertUnits(timeRange.getMax()));
    }

    return rangeBuilder;
  }

  private long convertUnits(long hbaseUnits) {
    return TimestampConverter.hbase2bigtable(hbaseUnits);
  }

  private Optional<Filters.Filter> createColumnFamilyFilter(Scan scan) {
    if (!scan.hasFamilies()) {
      return Optional.absent();
    }
    // Build a filter of the form:
    // (fam1 | (qual1 + qual2 + qual3)) + (fam2 | qual1) + (fam3)
    InterleaveFilter interleave = FILTERS.interleave();
    Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
      Filters.Filter familyFilter = createFamilyFilter(entry.getKey());

      NavigableSet<byte[]> qualifiers = entry.getValue();
      // Add a qualifier filter for each specified qualifier:
      if (qualifiers != null) {
        InterleaveFilter columnFilters = FILTERS.interleave();
        for (byte[] qualifier : qualifiers) {
          columnFilters.filter(createColumnQualifierFilter(qualifier));
        }
        // Build filter of the form "family | (qual1 + qual2 + qual3)"
        interleave.filter(FILTERS.chain().filter(familyFilter).filter(columnFilters));
      } else {
        interleave.filter(familyFilter);
      }
    }
    return Optional.<Filters.Filter>of(interleave);
  }

  private Optional<Filters.Filter> createColFamilyTimeRange(Scan scan) {
    if (scan.getColumnFamilyTimeRange().isEmpty()) {
      return Optional.absent();
    }
    // Builds filter of the form
    // ("family1" & "rangeStart, rangeEnd") | ("family2" & "rangeStart2, rangeEnd2")
    InterleaveFilter interleave = FILTERS.interleave();
    Map<byte[], TimeRange> range = scan.getColumnFamilyTimeRange();
    for (Map.Entry<byte[], TimeRange> entry : range.entrySet()) {
      interleave.filter(
          FILTERS
              .chain()
              .filter(createFamilyFilter(entry.getKey()))
              .filter(createTimeRangeFilter(entry.getValue())));
    }
    return Optional.<Filters.Filter>of(interleave);
  }
}
