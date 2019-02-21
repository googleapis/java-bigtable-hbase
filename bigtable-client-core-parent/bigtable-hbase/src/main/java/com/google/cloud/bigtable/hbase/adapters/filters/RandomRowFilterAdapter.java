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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.cloud.bigtable.data.v2.models.Filters.Filter;

import org.apache.hadoop.hbase.filter.RandomRowFilter;

import java.io.IOException;

/**
 * Adapter for {@link org.apache.hadoop.hbase.filter.RandomRowFilter}
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RandomRowFilterAdapter extends TypedFilterAdapterBase<RandomRowFilter> {
  /** {@inheritDoc} */
  @Override
  public Filter adapt(FilterAdapterContext context, RandomRowFilter filter)
      throws IOException {
    return FILTERS.key().sample(filter.getChance());
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, RandomRowFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
