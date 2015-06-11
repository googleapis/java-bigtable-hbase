/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v1.RowFilter;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An adapter for converting an HBase Filter into Bigtable RowFilter objects
 */
public class FilterAdapter {

  /**
   * Create a new FilterAdapter
   */
  public static FilterAdapter buildAdapter() {
    FilterAdapter adapter = new FilterAdapter();
    adapter.addFilterAdapter(
        ColumnPrefixFilter.class, new ColumnPrefixFilterAdapter());
    adapter.addFilterAdapter(
        ColumnRangeFilter.class, new ColumnRangeFilterAdapter());
    adapter.addFilterAdapter(
        KeyOnlyFilter.class, new KeyOnlyFilterAdapter());
    adapter.addFilterAdapter(
        MultipleColumnPrefixFilter.class, new MultipleColumnPrefixFilterAdapter());
    adapter.addFilterAdapter(
        TimestampsFilter.class, new TimestampsFilterAdapter());
    ValueFilterAdapter valueFilterAdapter = new ValueFilterAdapter();
    adapter.addFilterAdapter(
        ValueFilter.class, valueFilterAdapter);
    SingleColumnValueFilterAdapter scvfa =
        new SingleColumnValueFilterAdapter(valueFilterAdapter);
    adapter.addFilterAdapter(
        SingleColumnValueFilter.class, scvfa);
    adapter.addFilterAdapter(
        SingleColumnValueExcludeFilter.class,
        new SingleColumnValueExcludeFilterAdapter(scvfa));
    adapter.addFilterAdapter(
        ColumnPaginationFilter.class, new ColumnPaginationFilterAdapter());
    adapter.addFilterAdapter(
        FirstKeyOnlyFilter.class, new FirstKeyOnlyFilterAdapter());
    adapter.addFilterAdapter(
        ColumnCountGetFilter.class, new ColumnCountGetFilterAdapter());
    adapter.addFilterAdapter(
        RandomRowFilter.class, new RandomRowFilterAdapter());
    adapter.addFilterAdapter(
        PrefixFilter.class, new PrefixFilterAdapter());
    adapter.addFilterAdapter(
        QualifierFilter.class, new QualifierFilterAdapter());
    adapter.addFilterAdapter(
        PageFilter.class, new PageFilterAdapter());

    // Passing the FilterAdapter in to the FilterListAdapter is a bit
    // unfortunate, but makes adapting the FilterList's subfilters simpler.
    FilterListAdapter filterListAdapter = new FilterListAdapter(adapter);
    // FilterList implements UnsupportedStatusCollector so it should
    // be used when possible (third parameter to addFilterAdapter()).
    adapter.addFilterAdapter(
        FilterList.class, filterListAdapter, filterListAdapter);

    return adapter;
  }

  /**
   * A map of Class entries mapping to SingleFilterAdapter instances. Each supported Filter
   * subclass should have an entry in this map.
   */
  private Map<Class<? extends Filter>, SingleFilterAdapter<?>> adapterMap = new HashMap<>();

  private <T extends Filter> void addFilterAdapter(
      Class<T> filterType, TypedFilterAdapter<T> typedFilterAdapter) {
    adapterMap.put(filterType, new SingleFilterAdapter<>(filterType, typedFilterAdapter));
  }

  private <T extends Filter> void addFilterAdapter(
      Class<T> filterType,
      TypedFilterAdapter<T> typedFilterAdapter,
      UnsupportedStatusCollector<T> collector) {
    adapterMap.put(
        filterType,
        new SingleFilterAdapter<>(filterType, typedFilterAdapter, collector));
  }

  /**
   * Building the adapter map properly requires using a reference to the main FilterAdapter (to
   * pass to FilterListAdapter). As a result, a full adapter should be acquired via #buildAdapter().
   */
  protected FilterAdapter() {
  }

  /**
   * Adapt an HBase filter into a Cloud Bigtable Rowfilter.
   */
  public Optional<RowFilter> adaptFilter(FilterAdapterContext context, Filter filter)
      throws IOException {
    SingleFilterAdapter<?> adapter = getAdapterForFilterOrThrow(filter);
    return Optional.fromNullable(adapter.adapt(context, filter));
  }

  /**
   * Throw a new UnsupportedFilterException if the given filter cannot be adapted to bigtable
   * reader expressions.
   */
  public void throwIfUnsupportedFilter(Scan scan, Filter filter) {
    List<FilterSupportStatus> filterSupportStatuses = new ArrayList<>();
    FilterAdapterContext context = new FilterAdapterContext(scan, null);
    collectUnsupportedStatuses(context, filter, filterSupportStatuses);
    if (!filterSupportStatuses.isEmpty()) {
      throw new UnsupportedFilterException(filterSupportStatuses);
    }
  }

  /**
   * Recursively collect all unsupported filters contained in Filter (which may be a FilterList)
   * @param filter The filter to inspect
   * @param statuses A mutable list of status into which we will add any that indicate an
   * unsupported Filter was found.
   */
  public void collectUnsupportedStatuses(
      FilterAdapterContext context, Filter filter, List<FilterSupportStatus> statuses) {
    SingleFilterAdapter<?> adapter = adapterMap.get(filter.getClass());
    if (adapter == null) {
      statuses.add(FilterSupportStatus.newUnknownFilterType(filter));
    } else {
      adapter.collectUnsupportedStatuses(context, filter, statuses);
    }
  }

  /**
   * Get the adapter for the given Filter or throw an UnsupportedFilterException if one is not
   * available.
   */
  protected SingleFilterAdapter<?> getAdapterForFilterOrThrow(Filter filter) {
    if (adapterMap.containsKey(filter.getClass())) {
      return adapterMap.get(filter.getClass());
    } else {
      throw new UnsupportedFilterException(
          ImmutableList.of(FilterSupportStatus.newUnknownFilterType(filter)));
    }
  }
}
