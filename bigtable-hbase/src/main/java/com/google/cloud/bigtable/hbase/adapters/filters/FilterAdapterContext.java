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

import com.google.cloud.bigtable.hbase.adapters.ReadHooks;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context for the currently executing filter adapter.
 */
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

  public FilterAdapterContext(Scan scan, ReadHooks readHooks) {
    this.scan = scan;
    this.filterListStack = new ArrayDeque<>();
    this.readHooks = readHooks;
  }

  Scan getScan() {
    return scan;
  }

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

  public int getFilterListDepth() {
    return filterListStack.size();
  }

  public Optional<FilterList> getCurrentFilterList() {
    if (filterListStack.isEmpty()) {
      return Optional.absent();
    }
    return Optional.of(filterListStack.peek());
  }

  public ReadHooks getReadHooks() {
    return readHooks;
  }

  /**
   * Returns the next unique ID as a {@link String} in this adapter context.
   */
  public String getNextUniqueId() {
    return String.valueOf(++counter);
  }

  /**
   * Returns the current unique ID as a {@link String} in this adapter context.
   */
  @VisibleForTesting
  String getCurrentUniqueId() {
    return String.valueOf(counter);
  }

  /*
   * Adds a {@link WhileMatchFilter}, {@code filter} in the context.
   */
  public void addWhileMatchFilter(WhileMatchFilter filter) {
    whileMatchFilters.add(filter);
  }

  /*
   * Returns the number of {@link WhileMatchFilter}s in the context.
   */
  public int getNumberOfWhileMatchFilters() {
    return whileMatchFilters.size();
  }
}
