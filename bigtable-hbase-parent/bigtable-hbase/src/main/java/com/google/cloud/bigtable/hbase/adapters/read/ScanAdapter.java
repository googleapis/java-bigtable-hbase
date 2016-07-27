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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest.Builder;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * An adapter for Scan operation that makes use of the proto filter language.
 */
public class ScanAdapter implements ReadOperationAdapter<Scan> {

  private static final int UNSET_MAX_RESULTS_PER_COLUMN_FAMILY = -1;

  private final FilterAdapter filterAdapter;
  public ScanAdapter(FilterAdapter filterAdapter) {
    this.filterAdapter = filterAdapter;
  }

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
   */
  public RowFilter buildFilter(Scan scan, ReadHooks hooks) {
    RowFilter.Chain.Builder chainBuilder = RowFilter.Chain.newBuilder();
    chainBuilder.addFilters(createColumnFamilyFilter(scan));
    chainBuilder.addFilters(createColumnLimitFilter(scan.getMaxVersions()));

    if (scan.getTimeRange() != null && !scan.getTimeRange().isAllTime()) {
      RowFilter timeRangeFilter = createTimeRangeFilter(scan.getTimeRange());
      chainBuilder.addFilters(timeRangeFilter);
    }

    if (scan.getFilter() != null) {
      Optional<RowFilter> userFilter = createUserFilter(scan, hooks);
      if (userFilter.isPresent()) {
        chainBuilder.addFilters(userFilter.get());
      }
    }

    if (chainBuilder.getFiltersCount() == 1) {
      return chainBuilder.getFilters(0);
    } else {
      return RowFilter.newBuilder().setChain(chainBuilder).build();
    }
  }

  @Override
  public Builder adapt(Scan scan, ReadHooks readHooks) {
    throwIfUnsupportedScan(scan);
    RowFilter filter = buildFilter(scan, readHooks);

    RowSet.Builder rowSetBuilder = RowSet.newBuilder();
    if (scan.isGetScan()) {
      rowSetBuilder.addRowKeys(ByteString.copyFrom(scan.getStartRow()));
    } else {
      rowSetBuilder.addRowRanges(
        RowRange.newBuilder().setStartKeyClosed(ByteString.copyFrom(scan.getStartRow()))
            .setEndKeyOpen(ByteString.copyFrom(scan.getStopRow())));
    }
    return ReadRowsRequest.newBuilder().setFilter(filter).setRows(rowSetBuilder);
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
