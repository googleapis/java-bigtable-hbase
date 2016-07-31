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

import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * An interface that classes should implement if they're able to contribute
 * meaningfully to collecting unsupported status objects from child filters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface UnsupportedStatusCollector<S extends Filter> {

  /**
   * Collect FilterSupportStatuses from the filter Filter and all subfilters.
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext} object.
   * @param filter a S object.
   * @param unsupportedStatuses a {@link java.util.List} object.
   */
  void collectUnsupportedStatuses(
      FilterAdapterContext context,
      S filter,
      List<FilterSupportStatus> unsupportedStatuses);
}
