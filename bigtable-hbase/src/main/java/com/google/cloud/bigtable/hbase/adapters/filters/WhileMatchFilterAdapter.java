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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.filter.WhileMatchFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for {@link WhileMatchFilter}.
 */
public class WhileMatchFilterAdapter implements TypedFilterAdapter<WhileMatchFilter> {
  
  static final String IN_LABEL_SUFFIX = "-in";
  static final String OUT_LABEL_SUFFIX = "-out";

  private final FilterAdapter subFilterAdapter;

  public WhileMatchFilterAdapter(FilterAdapter subFilterAdapter) {
    this.subFilterAdapter = subFilterAdapter;
  }

  /**
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
  public RowFilter adapt(FilterAdapterContext context, WhileMatchFilter filter) throws IOException {
    // We need to eventually support more than one {@link WhileMatchFilter}s soon. Checking the size
    // of a list of {@link WhileMatchFilter}s makes more sense than verifying a single boolean flag.
    checkArgument(
        context.getNumberOfWhileMatchFilters() == 0,
        "More than one WhileMatchFilter is not supported.");
    checkNotNull(filter.getFilter(), "The wrapped filter for a WhileMatchFilter cannot be null.");
    Optional<RowFilter> wrappedFilter = subFilterAdapter.adaptFilter(context, filter.getFilter());
    checkArgument(
        wrappedFilter.isPresent(), "Unable to adapted the wrapped filter: " + filter.getFilter());

    String whileMatchFileterId = context.getNextUniqueId();
    RowFilter sink = RowFilter.newBuilder().setSink(true).build();
    RowFilter inLabel =
        RowFilter.newBuilder()
            .setApplyLabelTransformer(whileMatchFileterId + IN_LABEL_SUFFIX)
            .build();
    RowFilter inLabelAndSink =
        RowFilter.newBuilder()
            .setChain(Chain.newBuilder().addAllFilters(ImmutableList.of(inLabel, sink)))
            .build();
    RowFilter outLabel =
        RowFilter.newBuilder()
            .setApplyLabelTransformer(whileMatchFileterId + OUT_LABEL_SUFFIX)
            .build();
    RowFilter outLabelAndSink =
        RowFilter.newBuilder()
            .setChain(Chain.newBuilder().addAllFilters(ImmutableList.of(outLabel, sink)))
            .build();

    RowFilter all = RowFilter.newBuilder().setPassAllFilter(true).build();
    RowFilter outInterleave =
        RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder().addAllFilters(ImmutableList.of(outLabelAndSink, all)))
            .build();
    RowFilter outChain =
        RowFilter.newBuilder()
            .setChain(Chain.newBuilder().addAllFilters(
                ImmutableList.of(wrappedFilter.get(), outInterleave)))
            .build();
    RowFilter rowFilter =
        RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder().addAllFilters(ImmutableList.of(inLabelAndSink, outChain)))
            .build();

    context.addWhileMatchFilter(filter);

    return rowFilter;
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, WhileMatchFilter filter) {
    List<FilterSupportStatus> unsupportedStatuses = new ArrayList<>();
    subFilterAdapter.collectUnsupportedStatuses(context, filter.getFilter(), unsupportedStatuses);
    if (unsupportedStatuses.isEmpty()) {
      return FilterSupportStatus.SUPPORTED;
    }
    return FilterSupportStatus.newCompositeNotSupported(unsupportedStatuses);
  }
}
