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

import static com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper.quoteRegularExpression;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Condition;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

/**
 * Adapt SingleColumnValueFilter instances into bigtable RowFilters.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class SingleColumnValueFilterAdapter
    extends TypedFilterAdapterBase<SingleColumnValueFilter> {

  @VisibleForTesting
  static final RowFilter ALL_VALUES_FILTER =
      RowFilter.newBuilder().setPassAllFilter(true).build();
  @VisibleForTesting
  static final RowFilter LATEST_ONLY_FILTER =
      RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build();
  private final ValueFilterAdapter delegateAdapter;

  /**
   * <p>Constructor for SingleColumnValueFilterAdapter.</p>
   *
   * @param delegateAdapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.ValueFilterAdapter} object.
   */
  public SingleColumnValueFilterAdapter(ValueFilterAdapter delegateAdapter) {
    this.delegateAdapter = delegateAdapter;
  }

  /**
   * {@link SingleColumnValueFilter} is a filter that will return a row if a family/qualifier
   * value matches some condition. Optionally,  if
   * {@link SingleColumnValueFilter#getFilterIfMissing()} is set to false, then also return
   * the row if the family/column is not present on the row.  There's a
   *
   * <p> Here's a rough translation of {@link SingleColumnValueFilter#getFilterIfMissing()} == true.
   *
   * <pre>
   * IF a single family/column exists AND
   *    the value of the family/column meets some condition THEN
   *       return the ROW
   * END
   * </pre>
   *
   * Here's a rough translation of {@link SingleColumnValueFilter#getFilterIfMissing()} == false.
   *
   * <pre>
   * IF a single family/column exists THEN
   *   IF the value of the family/column meets some condition THEN
   *     return the ROW
   *   END
   * ELSE IF filter.filter_if_missing == false THEN
   *   return the ROW
   * END
   * </pre>
   * 
   * The Cloud Bigtable filter translation for the
   * {@link SingleColumnValueFilter#getFilterIfMissing()} true case here's the resulting filter is
   * as follows:
   *
   * <pre>
   *   condition: {
   *      predicate: {
   *        chain: {
   *           family: [filter.family]
   *           qualifier: [filter.qualifier],
   *           value: // something interesting
   *           // if filter.latestOnly, then add
   *           // cells_per_column: 1
   *        }
   *      }
   *      true_filter: {
   *         pass_all: true
   *      }
   *   }
   * </pre>
   *
   * In addition to the default filter, there's a bit more if
   * {@link SingleColumnValueFilter#getFilterIfMissing()} is false.  Here's what the filter would
   * look like:
   *
   * <pre>
   *   interleave: [ // either
   *     {
   *       // If the family/qualifer exists and matches a value
   *       // Then return the row
   *       // Else return nothing
   *       condition: {
   *         predicate: {
   *           chain: {
   *             family: [filter.family]
   *             qualifier: [filter.qualifier],
   *             // if filter.latestOnly, then add
   *             // cells_per_column: 1
   *             value: // something interesting
   *           }
   *         },
   *         true_filter: { pass_all: true }
   *       }
   *     }, {
   *       // If the family/qualifer exists
   *       // Then return nothing
   *       // Else return row
   *       condition: {
   *         predicate: {
   *           chain: {
   *             family: [filter.family]
   *             qualifier: [filter.qualifier],
   *           }
   *         },
   *         false_filter: { pass_all: true }
   *       }
   *     }
   *   ]
   * </pre>
   *
   * NOTE: This logic can also be expressed as nested predicates, but that approach creates really poor
   * performance on the server side.
   * <p>
   */
  @Override
  public RowFilter adapt(FilterAdapterContext context, SingleColumnValueFilter filter)
      throws IOException {

    // filter to check if the column exists
    RowFilter columnSpecFilter = getColumnSpecFilter(
      filter.getFamily(),
      filter.getQualifier(),
      filter.getLatestVersionOnly());

    // filter to return the row if the condition is met
    RowFilter emitRowsWithValueFilter = RowFilter.newBuilder()
      .setCondition(
          Condition.newBuilder()
              .setPredicateFilter(
                  RowFilter.newBuilder()
                      .setChain(
                          columnSpecFilter.getChain().toBuilder()
                              .addFilters(createValueMatchFilter(context, filter))
                              .build()))
              .setTrueFilter(ALL_VALUES_FILTER))
      .build();

    if (filter.getFilterIfMissing()) {
      return emitRowsWithValueFilter;
    } else {
      return RowFilter.newBuilder().setInterleave(
        Interleave.newBuilder()
          .addFilters(emitRowsWithValueFilter)
          .addFilters(RowFilter.newBuilder()
            .setCondition(
                Condition.newBuilder()
                    .setPredicateFilter(columnSpecFilter)
                    .setFalseFilter(ALL_VALUES_FILTER))
            .build())
          .build()
      ).build();
    }
  }

  @VisibleForTesting
  static RowFilter getColumnSpecFilter(byte[] family, byte[] qualifier, boolean latestVersionOnly)
      throws IOException {
    ByteString wrappedQual = ByteStringer.wrap(quoteRegularExpression(qualifier));
    String wrappedFamily = Bytes.toString(quoteRegularExpression(family));
    Chain.Builder chainBuilder = Chain.newBuilder()
        .addFilters(RowFilter.newBuilder()
            .setFamilyNameRegexFilter(wrappedFamily)
            .build())
        .addFilters(RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(wrappedQual)
            .build());

    if (latestVersionOnly) {
      chainBuilder.addFilters(LATEST_ONLY_FILTER);
    }

    return RowFilter.newBuilder().setChain(chainBuilder.build()).build();
  }

  /**
   * Emit a filter that will match against a single value.
   */
  private RowFilter createValueMatchFilter(
      FilterAdapterContext context, SingleColumnValueFilter filter) throws IOException {
    ValueFilter valueFilter = new ValueFilter(filter.getOperator(), filter.getComparator());
    return delegateAdapter.adapt(context, valueFilter);
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, SingleColumnValueFilter filter) {
      return delegateAdapter.isFilterSupported(
          context, new ValueFilter(filter.getOperator(), filter.getComparator()));
  }
}
