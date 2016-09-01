/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * FilterSupportStatus is a result type indicating whether a filter has a supported adaptation
 * to bigtable reader expressions.
 *
 * The isSupported method indicates whether the Filter is supported and if isSupport() is false
 * a reason may be provided by the adapter.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class FilterSupportStatus {

  /**
   * A static instance for all supported Filter adaptations.
   */
  public static final FilterSupportStatus SUPPORTED = new FilterSupportStatus(true, null);

  /**
   * Used to indicate an internal error where an adapter for a single Filter type is passed an
   * instance of an incompatible Filter type.
   */
  public static final FilterSupportStatus NOT_SUPPORTED_WRONG_TYPE =
      newNotSupported("Wrong filter type passed to adapter.");
  /**
   * Static helper to construct not support adaptations due to no adapter being available for
   * the given Filter type.
   *
   * @param unknownFilterType The unknown filter instance.
   * @return A new FilterSupportStatus.
   */
  public static FilterSupportStatus newUnknownFilterType(Filter unknownFilterType) {
    return new FilterSupportStatus(
        false,
        String.format(
            "Don't know how to adapt Filter class '%s'", unknownFilterType.getClass()));
  }

  /**
   * Generic static constructor for not supported adaptations with the stated reason.
   *
   * @param reason a {@link java.lang.String} object.
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterSupportStatus} object.
   */
  public static FilterSupportStatus newNotSupported(String reason) {
    return new FilterSupportStatus(false, reason);
  }

  /**
   * Static constructor for a not supported status caused by sub-filters not being supported.
   *
   * @param unsupportedSubfilters a {@link java.util.List} object.
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterSupportStatus} object.
   */
  public static FilterSupportStatus newCompositeNotSupported(
      List<FilterSupportStatus> unsupportedSubfilters) {
    StringBuilder builder = new StringBuilder();
    for (FilterSupportStatus subStatus: unsupportedSubfilters) {
      builder.append(subStatus.getReason());
      builder.append("\n");
    }
    return new FilterSupportStatus(false, builder.toString());
  }

  private final boolean isSupported;
  private final String reason;

  private FilterSupportStatus(boolean isSupported, String reason) {
    this.isSupported = isSupported;
    this.reason = reason;
  }

  /**
   * True if the adaptation is supported, false otherwise.
   *
   * @return a boolean.
   */
  public boolean isSupported() {
    return isSupported;
  }

  /**
   * The reason why the adaptation is not supported, if any.
   */
  String getReason() {
    return reason;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format(
        "FilterSupportStatus{isSupported=%s, reason='%s'}", isSupported, reason);
  }
}
