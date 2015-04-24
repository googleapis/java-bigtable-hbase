package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helpers for filter adapters.
 */
public class FilterAdapterHelper {

  /**
   * A RowFilter that will match all cells.
   */
  public static final RowFilter ACCEPT_ALL_FILTER =
      RowFilter.newBuilder()
          .setFamilyNameRegexFilter(ReaderExpressionHelper.ALL_FAMILIES)
          .build();

  /**
   * Extract a single family name from a FilterAdapterContext. Throws if there
   * is not exactly 1 family present in the scan.
   */
  public static String getSingleFamilyName(FilterAdapterContext context) {
    Preconditions.checkState(
        context.getScan().numFamilies() == 1,
        "Cannot getSingleFamilyName() unless there is exactly 1 family.");
    return Bytes.toString(context.getScan().getFamilies()[0]);
  }

  /**
   * Extract a regular expression from a RegexStringComparator.
   */
  public static String extractRegexPattern(RegexStringComparator comparator) {
    return Bytes.toString(comparator.getValue());
  }
}
