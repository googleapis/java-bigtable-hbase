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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest.Builder;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.base.Optional;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * An adapter for Scan operation that makes use of the proto filter language.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ScanAdapter implements ReadOperationAdapter<Scan> {

  private static final int UNSET_MAX_RESULTS_PER_COLUMN_FAMILY = -1;

  private final FilterAdapter filterAdapter;
  /**
   * <p>Constructor for ScanAdapter.</p>
   *
   * @param filterAdapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter} object.
   */
  public ScanAdapter(FilterAdapter filterAdapter) {
    this.filterAdapter = filterAdapter;
  }

  /**
   * <p>throwIfUnsupportedScan.</p>
   *
   * @param scan a {@link org.apache.hadoop.hbase.client.Scan} object.
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
   * Given a Scan, build a RowFilter that include matching columns
   *
   * @param scan a {@link org.apache.hadoop.hbase.client.Scan} object.
   * @param hooks a {@link com.google.cloud.bigtable.hbase.adapters.read.ReadHooks} object.
   * @return a {@link com.google.bigtable.v2.RowFilter} object.
   */
  public RowFilter buildFilter(Scan scan, ReadHooks hooks) {
    RowFilter.Chain.Builder chainBuilder = RowFilter.Chain.newBuilder();
    chainBuilder.addFilters(createColumnFamilyFilter(scan));

    if (scan.getTimeRange() != null && !scan.getTimeRange().isAllTime()) {
      chainBuilder.addFilters(createTimeRangeFilter(scan.getTimeRange()));
    }

    if (scan.getFilter() != null) {
      Optional<RowFilter> userFilter = createUserFilter(scan, hooks);
      if (userFilter.isPresent()) {
        chainBuilder.addFilters(userFilter.get());
      }
    }

    chainBuilder.addFilters(createColumnLimitFilter(scan.getMaxVersions()));

    if (chainBuilder.getFiltersCount() == 1) {
      return chainBuilder.getFilters(0);
    } else {
      return RowFilter.newBuilder().setChain(chainBuilder).build();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Builder adapt(Scan scan, ReadHooks readHooks) {
    throwIfUnsupportedScan(scan);

    RowSet rowSet = getRowSet(scan);

    rowSet = narrowRowSet(rowSet, scan.getFilter());
    RowFilter rowFilter = buildFilter(scan, readHooks);


    return ReadRowsRequest.newBuilder()
        .setRows(rowSet)
        .setFilter(rowFilter);
  }

  private RowSet getRowSet(Scan scan) {
    if (scan instanceof BigtableExtendedScan) {
      return ((BigtableExtendedScan) scan).getRowSet();
    } else {
      RowSet.Builder rowSetBuilder = RowSet.newBuilder();
      ByteString startRow = ByteString.copyFrom(scan.getStartRow());
      if (scan.isGetScan()) {
        rowSetBuilder.addRowKeys(startRow);
      } else {
        ByteString stopRow = ByteString.copyFrom(scan.getStopRow());
        rowSetBuilder.addRowRangesBuilder().setStartKeyClosed(startRow).setEndKeyOpen(stopRow);
      }
      return rowSetBuilder.build();
    }
  }

  private static byte[] quoteRegex(byte[] unquoted)  {
    try {
      return ReaderExpressionHelper.quoteRegularExpression(unquoted);
    } catch (IOException e) {
      throw new IllegalStateException(
          "IOException when writing to ByteArrayOutputStream", e);
    }
  }

  private Optional<RowFilter> createUserFilter(Scan scan, ReadHooks hooks) {
    try {
      return filterAdapter
          .adaptFilter(new FilterAdapterContext(scan, hooks), scan.getFilter());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to adapt filter", ioe);
    }
  }

  private RowSet narrowRowSet(RowSet rowSet, Filter filter) {
    RangeSet<RowKeyWrapper> filterRangeSet = filterAdapter.getIndexScanHint(filter);
    if (filterRangeSet.encloses(Range.<RowKeyWrapper>all())) {
      return rowSet;
    }
    RangeSet<RowKeyWrapper> scanRangeSet = rowSetToRangeSet(rowSet);
    scanRangeSet.removeAll(filterRangeSet.complement());
    return rangeSetToRowSet(scanRangeSet);
  }
  private static RangeSet<RowKeyWrapper> rowSetToRangeSet(RowSet rowSet) {
    RangeSet<RowKeyWrapper> rangeSet = TreeRangeSet.create();
    for (RowRange rowRange : rowSet.getRowRangesList()) {
      rangeSet.add(rowRangeToRange(rowRange));
    }
    for (ByteString key : rowSet.getRowKeysList()) {
      rangeSet.add(Range.singleton(new RowKeyWrapper(key)));
    }
    return rangeSet;
  }

  /**
   * Convert Bigtable's RowRange to a guava Range
   * @param range
   * @return
   */
  private static Range<RowKeyWrapper> rowRangeToRange(RowRange range) {
    final BoundType startBound;
    final ByteString startKey;

    switch(range.getStartKeyCase()) {
      case START_KEY_OPEN:
        startBound = BoundType.OPEN;
        startKey = range.getStartKeyOpen();
        break;
      case START_KEY_CLOSED:
        startBound = BoundType.CLOSED;
        startKey = range.getStartKeyClosed();
        break;
      case STARTKEY_NOT_SET:
        startBound = null;
        startKey = null;
        break;
      default:
        throw new IllegalArgumentException("Unexpected start key case: " + range.getStartKeyCase());
    }

    final BoundType endBound;
    final ByteString endKey;
    switch (range.getEndKeyCase()) {
      case END_KEY_OPEN:
        endBound = BoundType.OPEN;
        endKey = range.getEndKeyOpen();
        break;
      case END_KEY_CLOSED:
        endBound = BoundType.CLOSED;
        endKey = range.getEndKeyClosed();
        break;
      case ENDKEY_NOT_SET:
        endBound = null;
        endKey = null;
        break;
      default:
        throw new IllegalArgumentException("Unexpected end key case: " +range.getEndKeyCase());
    }


    if (startBound == null && endBound == null) {
      return Range.all();
    }
    else if (startBound == null) {
      return Range.upTo(new RowKeyWrapper(endKey), endBound);
    }
    else if (endBound == null) {
      return Range.downTo(new RowKeyWrapper(startKey), startBound);
    }
    else {
      return Range.range(new RowKeyWrapper(startKey), startBound, new RowKeyWrapper(endKey), endBound);
    }
  }

  /**
   * Convert guava's RangeSet to Bigtable's RowSet
   */
  private static RowSet rangeSetToRowSet(RangeSet<RowKeyWrapper> rangeSet) {
    RowSet.Builder rowSet = RowSet.newBuilder();

    for (Range<RowKeyWrapper> range1 : rangeSet.asRanges()) {
      if (range1.hasLowerBound() && range1.lowerBoundType() == BoundType.CLOSED
          && range1.hasUpperBound() && range1.upperBoundType() == BoundType.CLOSED
          && range1.lowerEndpoint().equals(range1.upperEndpoint())) {

        rowSet.addRowKeys(range1.lowerEndpoint().getKey());
      }
      else {
        RowRange.Builder range2 = RowRange.newBuilder();

        if (range1.hasLowerBound()) {
          switch(range1.lowerBoundType()) {
            case CLOSED:
              range2.setStartKeyClosed(range1.lowerEndpoint().getKey());
              break;
            case OPEN:
              range2.setStartKeyOpen(range1.lowerEndpoint().getKey());
              break;
            default:
              throw new IllegalArgumentException("Unexpected lower bound type: " + range1.lowerBoundType());
          }
          if (range1.hasUpperBound()) {
            switch(range1.upperBoundType()) {
              case CLOSED:
                range2.setEndKeyClosed(range1.upperEndpoint().getKey());
                break;
              case OPEN:
                range2.setEndKeyOpen(range1.upperEndpoint().getKey());
                break;
              default:
                throw new IllegalArgumentException("Unexpected upper bound type: " + range1.upperBoundType());
            }
          }
          rowSet.addRowRanges(range2);
        }
      }
    }
    return rowSet.build();
  }

  private RowFilter createColumnQualifierFilter(byte[] unquotedQualifier) {
    return RowFilter.newBuilder()
        .setColumnQualifierRegexFilter(
            ByteStringer.wrap(quoteRegex(unquotedQualifier)))
        .build();
  }

  private RowFilter createFamilyFilter(byte[] familyName) {
    return RowFilter.newBuilder()
        .setFamilyNameRegexFilterBytes(
            ByteStringer.wrap(quoteRegex(familyName)))
        .build();
  }

  private RowFilter createColumnLimitFilter(int maxVersionsPerColumn) {
    return RowFilter.newBuilder()
        .setCellsPerColumnLimitFilter(maxVersionsPerColumn)
        .build();
  }

  private RowFilter createTimeRangeFilter(TimeRange timeRange) {
    TimestampRange.Builder rangeBuilder = TimestampRange.newBuilder();

    long lowerBound = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        timeRange.getMin(), BigtableConstants.HBASE_TIMEUNIT);
    rangeBuilder.setStartTimestampMicros(lowerBound);

    if (timeRange.getMax() != Long.MAX_VALUE) {
      long upperBound = BigtableConstants.BIGTABLE_TIMEUNIT.convert(
          timeRange.getMax(), BigtableConstants.HBASE_TIMEUNIT);
      rangeBuilder.setEndTimestampMicros(upperBound);
    }

    return RowFilter.newBuilder()
        .setTimestampRangeFilter(rangeBuilder)
        .build();
  }

  private RowFilter createColumnFamilyFilter(Scan scan) {
    // Build a filter of the form:
    // (fam1 | (qual1 + qual2 + qual3)) + (fam2 | qual1) + (fam3)
    RowFilter.Interleave.Builder interleaveBuilder =
        RowFilter.Interleave.newBuilder();
    Map<byte[],NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
    if (!scan.getFamilyMap().isEmpty()) {
      for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
        if (entry.getValue() == null) {
          // No qualifier, add the entire family:
          interleaveBuilder.addFilters(
              createFamilyFilter(entry.getKey()));
        } else if (entry.getValue().size() == 1) {
          // Build filter of the form "family | qual"
          Chain.Builder familyBuilder =
              interleaveBuilder.addFiltersBuilder().getChainBuilder();
          familyBuilder.addFilters(createFamilyFilter(entry.getKey()));
          familyBuilder.addFilters(createColumnQualifierFilter(entry.getValue().first()));
        } else {
          // Build filter of the form "family | (qual1 + qual2 + qual3)"
          Chain.Builder familyBuilder =
              interleaveBuilder.addFiltersBuilder().getChainBuilder();
          familyBuilder.addFilters(createFamilyFilter(entry.getKey()));
          // Add a qualifier filter for each specified qualifier:
          Interleave.Builder columnFilters =
              familyBuilder.addFiltersBuilder().getInterleaveBuilder();
          for (byte[] qualifier : entry.getValue()) {
            columnFilters.addFilters(createColumnQualifierFilter(qualifier));
          }
        }
      }
    } else {
      // Simplify processing a bit and add an explicit inclusion of all families:
      interleaveBuilder.addFiltersBuilder().setFamilyNameRegexFilter(".*");
    }

    if (interleaveBuilder.getFiltersCount() > 1) {
      return RowFilter.newBuilder().setInterleave(interleaveBuilder).build();
    } else {
      return interleaveBuilder.getFilters(0);
    }
  }

}
