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
package com.google.cloud.bigtable.hbase.adapters.filters;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Filters.QualifierRangeFilter;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Adapter for the {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class SingleColumnValueExcludeFilterAdapter
    extends TypedFilterAdapterBase<SingleColumnValueExcludeFilter> {

  private static final String REQUIRE_SINGLE_FAMILY_MESSAGE =
      "Scan or Get operations using SingleColumnValueExcludeFilter must "
          + "have a single family specified with #addFamily().";
  private static final FilterSupportStatus UNSUPPORTED_STATUS =
      FilterSupportStatus.newNotSupported(REQUIRE_SINGLE_FAMILY_MESSAGE);

  private final SingleColumnValueFilterAdapter delegateAdapter;

  /**
   * Constructor for SingleColumnValueExcludeFilterAdapter.
   *
   * @param delegateAdapter a {@link
   *     com.google.cloud.bigtable.hbase.adapters.filters.SingleColumnValueFilterAdapter} object.
   */
  public SingleColumnValueExcludeFilterAdapter(SingleColumnValueFilterAdapter delegateAdapter) {
    this.delegateAdapter = delegateAdapter;
  }

  /** {@inheritDoc} */
  @Override
  public Filter adapt(FilterAdapterContext context, SingleColumnValueExcludeFilter filter)
      throws IOException {
    String family = Bytes.toString(context.getScan().getFamilies()[0]);
    ByteString qualifier = ByteString.copyFrom(filter.getQualifier());
    return FILTERS
        .chain()
        .filter(delegateAdapter.toFilter(context, filter))
        .filter(
            FILTERS
                .interleave()
                .filter(range(family).endOpen(qualifier))
                .filter(range(family).startOpen(qualifier)));
  }

  private static QualifierRangeFilter range(String family) {
    return FILTERS.qualifier().rangeWithinFamily(family);
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, SingleColumnValueExcludeFilter filter) {
    FilterSupportStatus delegateStatus = delegateAdapter.isFilterSupported(context, filter);
    if (!delegateStatus.isSupported()) {
      return delegateStatus;
    }
    // This filter can only be adapted when there's a single family.
    if (context.getScan().numFamilies() != 1) {
      return UNSUPPORTED_STATUS;
    }
    return FilterSupportStatus.SUPPORTED;
  }
}
