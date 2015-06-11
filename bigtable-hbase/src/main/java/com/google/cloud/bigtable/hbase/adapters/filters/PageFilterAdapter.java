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

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.RowFilter;
import com.google.common.base.Function;
import com.google.common.base.Optional;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;

import java.io.IOException;

/**
 * A TypedFilterAdapter for adapting PageFilter instances.
 */
public class PageFilterAdapter implements TypedFilterAdapter<PageFilter> {

  private static final FilterSupportStatus TOP_LEVEL_ONLY =
      FilterSupportStatus.newNotSupported(
          "Page filters may only appear as top level filters or be contained within "
              + "a top-level FilterList instances with MUST_PASS_ALL as its Operator");

  @Override
  public RowFilter adapt(FilterAdapterContext context, PageFilter filter) throws IOException {
    final long pageSize = filter.getPageSize();
    context.getReadHooks().composePreSendHook(new Function<ReadRowsRequest, ReadRowsRequest>() {
      @Override
      public ReadRowsRequest apply(ReadRowsRequest request) {
        return request.toBuilder().setNumRowsLimit(pageSize).build();
      }
    });
    // This filter cannot be translated to a RowFilter, all logic is done as a read hook.
    return null;
  }

  @Override
  public FilterSupportStatus isFilterSupported(FilterAdapterContext context, PageFilter filter) {
    Optional<FilterList> currentList = context.getCurrentFilterList();
    if ((currentList.isPresent() && !isFilterListSupported(currentList.get(), filter))
        || context.getFilterListDepth() > 1) {
      return TOP_LEVEL_ONLY;
    }
    return FilterSupportStatus.SUPPORTED;
  }

  private static boolean isFilterListSupported(FilterList list, PageFilter currentFilter) {
    // The PageFilter must be the last filter in the FilterList && it must only appear once
    // in the FilterList (perhaps this second part is a pathological case that isn't worthy
    // of the cycles required to traverse the list)...
    return list.getOperator() == Operator.MUST_PASS_ALL
        && list.getFilters().indexOf(currentFilter) == list.getFilters().size() - 1;
  }
}
