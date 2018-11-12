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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.common.base.Optional;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for {@link org.apache.hadoop.hbase.filter.WhileMatchFilter}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class WhileMatchFilterAdapter extends TypedFilterAdapterBase<WhileMatchFilter> {
  
  static final String IN_LABEL_SUFFIX = "-in";
  static final String OUT_LABEL_SUFFIX = "-out";

  private final FilterAdapter subFilterAdapter;

  /**
   * <p>Constructor for WhileMatchFilterAdapter.</p>
   *
   * @param subFilterAdapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter} object.
   */
  public WhileMatchFilterAdapter(FilterAdapter subFilterAdapter) {
    this.subFilterAdapter = subFilterAdapter;
  }

  /**
   * {@inheritDoc}
   *
   * Adapt {@link WhileMatchFilter} as follow:
   *
   *                |
   *                V
   *       +--------+--------+
   *       |                 |
   *       |                 |
   * label('id-in')  wrappedFilter.filter()
   *       |                 |
   *       |                 |
   *     sink()        +-----+-----+
   *       +           |           |
   *            label('id-out')  all()
   *                   |           |
   *                 sink()        |
   *                   +           v
   *
   * The above implementation gives enough information from the server side to determine whether the
   * remaining rows should be filtered out. For each {@link WhileMatchFilter} instance, an unique ID
   * is generated for labeling. The input label is the unique ID suffixed by "-in" and the output
   * label is the unique ID suffixed by "-out". When {@code wrappedFilter} decides to filter out the
   * rest of rows, there is no out label ("id-out") applied in the output. In other words, when
   * there is a missing "id-out" for an input "id-in", {@link
   * WhileMatchFilter#filterAllRemaining()} returns {@code true}. Since the server continues to send
   * result even though {@link
   * WhileMatchFilter#filterAllRemaining()} returns {@code true}, we need to replace this {@link
   * WhileMatchFilter} instance with a "block all" filter and rescan from the next row.
   */
  @Override
  public Filters.Filter adapt(FilterAdapterContext context, WhileMatchFilter filter) throws IOException {
    // We need to eventually support more than one {@link WhileMatchFilter}s soon. Checking the size
    // of a list of {@link WhileMatchFilter}s makes more sense than verifying a single boolean flag.
    checkArgument(
        context.getNumberOfWhileMatchFilters() == 0,
        "More than one WhileMatchFilter is not supported.");
    checkNotNull(filter.getFilter(), "The wrapped filter for a WhileMatchFilter cannot be null.");
    Optional<Filters.Filter> wrappedFilter = subFilterAdapter.adaptFilter(context, filter.getFilter());
    checkArgument(
        wrappedFilter.isPresent(), "Unable to adapted the wrapped filter: " + filter.getFilter());

    String whileMatchFileterId = context.getNextUniqueId();

    Filters.Filter inLabel = FILTERS.label(whileMatchFileterId + IN_LABEL_SUFFIX);
    Filters.Filter inLabelAndSink = FILTERS.chain().filter(inLabel).filter(FILTERS.sink());

    Filters.Filter outLabel = FILTERS.label(whileMatchFileterId + OUT_LABEL_SUFFIX);
    Filters.Filter outLabelAndSink = FILTERS.chain().filter(outLabel).filter(FILTERS.sink());

    Filters.Filter outInterleave = FILTERS.interleave().filter(outLabelAndSink).filter(FILTERS.pass());
    Filters.Filter outChain = FILTERS.chain().filter(wrappedFilter.get()).filter(outInterleave);

    Filters.Filter finalFilter = FILTERS.interleave().filter(inLabelAndSink).filter(outChain);

    context.addWhileMatchFilter(filter);

    return finalFilter;
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, WhileMatchFilter filter) {
    // checks if wrapped filter is supported.
    List<FilterSupportStatus> unsupportedStatuses = new ArrayList<>();
    subFilterAdapter.collectUnsupportedStatuses(context, filter.getFilter(), unsupportedStatuses);
    if (!unsupportedStatuses.isEmpty()) {
      return FilterSupportStatus.newCompositeNotSupported(unsupportedStatuses);
    }

    // Checks if this filter is in an interleave.
    if (inInterleave(context.getScan().getFilter(), filter)) {
      return FilterSupportStatus.newNotSupported(
          "A WhileMatchFilter cannot be in a FilterList with MUST_PASS_ONE operation.");
    }

    return FilterSupportStatus.SUPPORTED;
  }

  private boolean inInterleave(Filter filter, WhileMatchFilter whileMatchFilter) {
    if (filter == whileMatchFilter) {
      return false;
    }

    if (filter instanceof FilterList) {
      FilterList list = (FilterList) filter;
      // Found an interleave, check to see if {@code whileMatchFilter} is in there.
      if (list.getOperator() == Operator.MUST_PASS_ONE) {
        for (Filter subFilter : list.getFilters()) {
          if (hasFilter(subFilter, whileMatchFilter)) {
            return true;
          }
        }
      } else {
        for (Filter subFilter : list.getFilters()) {
          if (inInterleave(subFilter, whileMatchFilter)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  /**
   * Return {@code true} iff {@code whileMatchFilter} is in {@code filter}.
   */
  private boolean hasFilter(Filter filter, WhileMatchFilter whileMatchFilter) {
    if (filter == whileMatchFilter) {
      return true;
    }

    if (filter instanceof FilterList) {
      FilterList list = (FilterList) filter;
      for (Filter subFilter : list.getFilters()) {
        if (hasFilter(subFilter, whileMatchFilter)) {
          return true;
        }
      }
    }

    return false;
  }
}
