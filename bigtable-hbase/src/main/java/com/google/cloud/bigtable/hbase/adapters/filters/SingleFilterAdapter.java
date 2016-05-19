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

import com.google.common.base.Preconditions;
import com.google.bigtable.v2.RowFilter;

import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

/**
 * A class that provides untyped-to-typed access to TypedFilterAdapter instances.
 */
public class SingleFilterAdapter<T extends Filter> {

  private final Class<T> filterClass;
  private final TypedFilterAdapter<T> adapter;
  private final UnsupportedStatusCollector<T> unsupportedStatusCollector;

  public SingleFilterAdapter(
      Class<T> filterClass,
      TypedFilterAdapter<T> adapter) {
    this(filterClass, adapter, new SingleFilterStatusCollector<>(adapter));
  }

  public SingleFilterAdapter(
      Class<T> filterClass,
      TypedFilterAdapter<T> adapter,
      UnsupportedStatusCollector<T> unsupportedStatusCollector) {
    this.filterClass = filterClass;
    this.adapter = adapter;
    this.unsupportedStatusCollector = unsupportedStatusCollector;
  }

  /**
   * Adapt the untyped hbaseFilter instance into a RowFilter.
   */
  public RowFilter adapt(FilterAdapterContext context, Filter hbaseFilter)
      throws IOException {
    T typedFilter = getTypedFilter(hbaseFilter);
    return adapter.adapt(context, typedFilter);
  }

  /**
   * Determine if the untyped filter is supported.
   */
  public FilterSupportStatus isSupported(FilterAdapterContext context, Filter hbaseFilter) {
    Preconditions.checkArgument(isFilterAProperSublcass(hbaseFilter));
    return adapter.isFilterSupported(context, getTypedFilter(hbaseFilter));
  }

  /**
   * Collect unsupported status objects into the given list.
   */
  public void collectUnsupportedStatuses(
      FilterAdapterContext context,
      Filter filter,
      List<FilterSupportStatus> statuses) {
    Preconditions.checkArgument(isFilterAProperSublcass(filter));
    unsupportedStatusCollector.collectUnsupportedStatuses(
        context,
        unchecked(filter),
        statuses);
  }

  @SuppressWarnings("unchecked")
  T unchecked(Filter filter) {
    return (T)filter;
  }

  protected T getTypedFilter(Filter filter) {
    if (isFilterAProperSublcass(filter)) {
      return unchecked(filter);
    }
    throw new IllegalStateException(
        String.format(
            "FilterAdapter %s cannot operate on a filter of type %s",
            getClass().getCanonicalName(),
            filter.getClass().getCanonicalName()));
  }

  protected boolean isFilterAProperSublcass(Filter filter) {
    return filterClass.isAssignableFrom(filter.getClass());
  }

  private static class SingleFilterStatusCollector<T extends Filter>
      implements UnsupportedStatusCollector<T> {
    private final TypedFilterAdapter<T> adapter;

    public SingleFilterStatusCollector(TypedFilterAdapter<T> adapter) {
      this.adapter = adapter;
    }

    @Override
    public void collectUnsupportedStatuses(
        FilterAdapterContext context,
        T filter,
        List<FilterSupportStatus> unsupportedStatuses) {
      FilterSupportStatus status = adapter.isFilterSupported(context, filter);
      if (!status.isSupported()) {
        unsupportedStatuses.add(status);
      }
    }
  }
}
