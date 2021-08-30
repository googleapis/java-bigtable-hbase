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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.hbase.util.RowKeyWrapper;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * An adapter that can adapt an HBase Filter instance into a Bigtable RowFilter.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface TypedFilterAdapter<S extends Filter> {

  /**
   * Adapt the given filter. Implementers of this method should assume that isFilterSupported has
   * already been called with a result indicating it is in fact supproted.
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext}
   *     object.
   * @param filter a S object.
   * @return a {@link com.google.cloud.bigtable.data.v2.models.Filters.Filter} object.
   * @throws java.io.IOException if any.
   */
  Filters.Filter adapt(FilterAdapterContext context, S filter) throws IOException;

  /**
   * Determine if the given filter can be adapted to a Bigtable RowFilter.
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext}
   *     object.
   * @param filter a S object.
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterSupportStatus} object.
   */
  FilterSupportStatus isFilterSupported(FilterAdapterContext context, S filter);

  /**
   * Get hints how to optimize the scan. For example if the filter will narrow the scan using the
   * prefix "ab" then we can restrict the scan to ["ab" - "ac"). If the filter doesn't narrow the
   * scan then it should return Range.all()
   *
   * @param filter a {@link S} object.
   * @return a {@link RangeSet} object.
   */
  RangeSet<RowKeyWrapper> getIndexScanHint(S filter);
}
