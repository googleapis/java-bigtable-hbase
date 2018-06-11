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

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.FILTERS;

import com.google.bigtable.v2.RowFilter;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helpers for filter adapters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class FilterAdapterHelper {

  /**
   * A RowFilter that will match all cells.
   */
  public static final RowFilter ACCEPT_ALL_FILTER = FILTERS.pass().toProto();

  /**
   * Extract a single family name from a FilterAdapterContext. Throws if there
   * is not exactly 1 family present in the scan.
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext} object.
   * @return a {@link java.lang.String} object.
   */
  public static String getSingleFamilyName(FilterAdapterContext context) {
    Preconditions.checkState(
        context.getScan().numFamilies() == 1,
        "Cannot getSingleFamilyName() unless there is exactly 1 family.");
    return Bytes.toString(context.getScan().getFamilies()[0]);
  }

  /**
   * Extract a regular expression from a RegexStringComparator.
   *
   * @param comparator a {@link org.apache.hadoop.hbase.filter.RegexStringComparator} object.
   * @return a {@link java.lang.String} object.
   */
  public static String extractRegexPattern(RegexStringComparator comparator) {
    return Bytes.toString(comparator.getValue());
  }
}
