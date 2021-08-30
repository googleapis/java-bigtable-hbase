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
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;

/**
 * Context for the currently executing filter adapter.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
@NotThreadSafe
public class FilterAdapterContext {

  public interface ContextCloseable extends AutoCloseable {
    @Override
    void close();
  }

  private final Scan scan;
  private Deque<FilterList> filterListStack;
  private ReadHooks readHooks;
  private int counter;
  private final List<WhileMatchFilter> whileMatchFilters = new ArrayList<>();

  /**
   * Constructor for FilterAdapterContext.
   *
   * @param scan a {@link org.apache.hadoop.hbase.client.Scan} object.
   * @param readHooks a {@link com.google.cloud.bigtable.hbase.adapters.read.ReadHooks} object.
   */
  public FilterAdapterContext(Scan scan, ReadHooks readHooks) {
    this.scan = scan;
    this.filterListStack = new ArrayDeque<>();
    this.readHooks = readHooks;
  }

  Scan getScan() {
    return scan;
  }

  /**
   * beginFilterList.
   *
   * @param lst a {@link org.apache.hadoop.hbase.filter.FilterList} object.
   * @return a {@link
   *     com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext.ContextCloseable}
   *     object.
   */
  public ContextCloseable beginFilterList(final FilterList lst) {
    Preconditions.checkNotNull(lst);
    filterListStack.push(lst);
    return new ContextCloseable() {
      @Override
      public void close() {
        Preconditions.checkState(filterListStack.peek().equals(lst));
        filterListStack.pop();
      }
    };
  }

  /**
   * getFilterListDepth.
   *
   * @return a int.
   */
  public int getFilterListDepth() {
    return filterListStack.size();
  }

  /**
   * getCurrentFilterList.
   *
   * @return a {@link com.google.common.base.Optional} object.
   */
  public Optional<FilterList> getCurrentFilterList() {
    if (filterListStack.isEmpty()) {
      return Optional.absent();
    }
    return Optional.of(filterListStack.peek());
  }

  /**
   * Getter for the field <code>readHooks</code>.
   *
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.read.ReadHooks} object.
   */
  public ReadHooks getReadHooks() {
    return readHooks;
  }

  /**
   * Returns the next unique ID as a {@link java.lang.String} in this adapter context.
   *
   * @return a {@link java.lang.String} object.
   */
  public String getNextUniqueId() {
    return String.valueOf(++counter);
  }

  /** Returns the current unique ID as a {@link String} in this adapter context. */
  @VisibleForTesting
  String getCurrentUniqueId() {
    return String.valueOf(counter);
  }

  /*
   * Adds a {@link WhileMatchFilter}, {@code filter} in the context.
   */
  /**
   * addWhileMatchFilter.
   *
   * @param filter a {@link org.apache.hadoop.hbase.filter.WhileMatchFilter} object.
   */
  public void addWhileMatchFilter(WhileMatchFilter filter) {
    whileMatchFilters.add(filter);
  }

  /*
   * Returns the number of {@link WhileMatchFilter}s in the context.
   */
  /**
   * getNumberOfWhileMatchFilters.
   *
   * @return a int.
   */
  public int getNumberOfWhileMatchFilters() {
    return whileMatchFilters.size();
  }

  /**
   * Get th current stack of nested FilterLists. This used by filters to check for unsupported scan
   * configurations.
   *
   * @return an unmodifiable collection of current nested FilterLists
   */
  Collection<FilterList> getCurrentFilterLists() {
    return Collections.unmodifiableCollection(filterListStack);
  }
}
