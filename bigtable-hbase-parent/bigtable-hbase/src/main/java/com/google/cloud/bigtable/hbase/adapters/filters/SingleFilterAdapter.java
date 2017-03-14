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

import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.base.Preconditions;
import com.google.bigtable.v2.RowFilter;

import com.google.common.collect.RangeSet;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

/**
 * A class that provides untyped-to-typed access to TypedFilterAdapter instances.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class SingleFilterAdapter<T extends Filter> {

  private final Class<T> filterClass;
  private final TypedFilterAdapter<T> adapter;
  private final UnsupportedStatusCollector<T> unsupportedStatusCollector;

  /**
   * <p>Constructor for SingleFilterAdapter.</p>
   *
   * @param filterClass a {@link java.lang.Class} object.
   * @param adapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.TypedFilterAdapter} object.
   */
  public SingleFilterAdapter(
      Class<T> filterClass,
      TypedFilterAdapter<T> adapter) {
    this(filterClass, adapter, new SingleFilterStatusCollector<>(adapter));
  }

  /**
   * <p>Constructor for SingleFilterAdapter.</p>
   *
   * @param filterClass a {@link java.lang.Class} object.
   * @param adapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.TypedFilterAdapter} object.
   * @param unsupportedStatusCollector a {@link com.google.cloud.bigtable.hbase.adapters.filters.UnsupportedStatusCollector} object.
   */
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
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext} object.
   * @param hbaseFilter a {@link org.apache.hadoop.hbase.filter.Filter} object.
   * @return a {@link com.google.bigtable.v2.RowFilter} object.
   * @throws java.io.IOException if any.
   */
  public RowFilter adapt(FilterAdapterContext context, Filter hbaseFilter)
      throws IOException {
    T typedFilter = getTypedFilter(hbaseFilter);
    return adapter.adapt(context, typedFilter);
  }

  /**
   * Determine if the untyped filter is supported.
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext} object.
   * @param hbaseFilter a {@link org.apache.hadoop.hbase.filter.Filter} object.
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterSupportStatus} object.
   */
  public FilterSupportStatus isSupported(FilterAdapterContext context, Filter hbaseFilter) {
    Preconditions.checkArgument(isFilterAProperSublcass(hbaseFilter));
    return adapter.isFilterSupported(context, getTypedFilter(hbaseFilter));
  }

  /**
   * Collect unsupported status objects into the given list.
   *
   * @param context a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext} object.
   * @param filter a {@link org.apache.hadoop.hbase.filter.Filter} object.
   * @param statuses a {@link java.util.List} object.
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

  public RangeSet<RowKeyWrapper> getIndexScanHint(Filter filter) {
    return adapter.getIndexScanHint(unchecked(filter));
  }

  @SuppressWarnings("unchecked")
  T unchecked(Filter filter) {
    return (T)filter;
  }

  /**
   * <p>getTypedFilter.</p>
   *
   * @param filter a {@link org.apache.hadoop.hbase.filter.Filter} object.
   * @return a T object.
   */
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

  /**
   * <p>isFilterAProperSublcass.</p>
   *
   * @param filter a {@link org.apache.hadoop.hbase.filter.Filter} object.
   * @return a boolean.
   */
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
