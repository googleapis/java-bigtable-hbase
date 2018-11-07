/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;

import javax.annotation.Nullable;
import java.util.Arrays;

public class CheckAndMutateUtil {

  // ReadHooks don't make sense from conditional mutations. If any filter attempts to make use of
  // them (which they shouldn't since we built the filter), throw an exception.
  private static final ReadHooks UNSUPPORTED_READ_HOOKS = new ReadHooks() {
    @Override
    public void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook) {
      throw new IllegalStateException(
          "We built a bad Filter for conditional mutation.");
    }

    @Override
    public ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest) {
      throw new UnsupportedOperationException(
          "We built a bad Filter for conditional mutation.");
    }
  };

  /**
   * <p>wasMutationApplied.</p>
   *
   * @param request a {@link com.google.bigtable.v2.CheckAndMutateRowRequest} object.
   * @param response a {@link com.google.bigtable.v2.CheckAndMutateRowResponse} object.
   * @return a boolean.
   */
  public static boolean wasMutationApplied(
      CheckAndMutateRowRequest request,
      CheckAndMutateRowResponse response) {

    // If we have true mods, we want the predicate to have matched.
    // If we have false mods, we did not want the predicate to have matched.
    return (request.getTrueMutationsCount() > 0
        && response.getPredicateMatched())
        || (request.getFalseMutationsCount() > 0
        && !response.getPredicateMatched());
  }

  /**
   * This class can be used to convert HBase checkAnd* operations to Bigtable
   * {@link CheckAndMutateRowRequest}s.
   */
  public static class RequestBuilder {
    private final HBaseRequestAdapter hbaseAdapter;

    private final com.google.cloud.bigtable.data.v2.models.Mutation mutations = com.google.cloud.bigtable.data.v2.models.Mutation.createUnsafe();

    private final byte[] row;
    private final byte[] family;
    private byte[] qualifier;
    private CompareOp compareOp;
    private byte[] value;
    private boolean checkNonExistence = false;
    private Filter timeFilter = null;

    /**
     * <p>
     * RequestBuilder.
     * </p>
     * @param hbaseAdapter a {@link HBaseRequestAdapter} used to convert HBase
     *          {@link org.apache.hadoop.hbase.client.Mutation} to Cloud Bigtable {@link Mutation}
     * @param row the RowKey in which to to check value matching
     * @param family the family in which to check value matching.
     */
    public RequestBuilder(HBaseRequestAdapter hbaseAdapter, byte[] row, byte[] family) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.family = Preconditions.checkNotNull(family, "family is null");

      // The hbaseAdapter used here should not set client-side timestamps, since that may cause strange contention
      // issues.  See issue #1709.
      this.hbaseAdapter = hbaseAdapter.withServerSideTimestamps();
    }

    public RequestBuilder qualifier(byte[] qualifier) {
      this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using" +
          " an empty byte array, or just do not call this method if you want a null qualifier");
      return this;
    }

    public RequestBuilder ifNotExists() {
      Preconditions.checkState(compareOp == null,
          "ifNotExists and ifMatches are mutually exclusive");
      this.checkNonExistence = true;
      return this;
    }

    /**
     * For non-null values, this produces a {@link RowFilter} equivalent to:
     * <pre>
     *   new ValueFilter(reverseCompareOp(compareOp), new BinaryComparator(value)
     * </pre>
     * <p>
     * Null values are bit tricky. In HBase 1.* style check and mutate, value == null always means `check non-existence`
     * regardless of compareOp.  That's semantically confusing for CompareOperators other than EQUALS.
     * It's even more confusing if value == null and compareOp = NOT_EQUALS.
     * <p>
     * HBase 2.* APIs introduced an explicit method of checking for "Non-Existence" as an independent concept from
     * compareOp, which is the inspiration for the {@link #ifNotExists()} method.
     * <p>
     * Checking for existence in HBase can be expressed as follows:
     * <pre>
     * compareOp = CompareOp.GREATER_OR_EQUAL, value = new byte[]{Byte.MIN_VALUE})
     * </pre>
     * <p>
     * Bigtable decided that value == null with a compareOp of NOT_EQUALS actually means check for existence, even
     * though HBase will still treat it as non-existence.
     *
     * @param compareOp a {@link CompareOp}
     * @param value
     * @return this
     */
    public RequestBuilder ifMatches(CompareOp compareOp, @Nullable byte[] value) {
      Preconditions.checkState(checkNonExistence == false,
          "ifNotExists and ifMatches are mutually exclusive");

      this.compareOp = Preconditions.checkNotNull(compareOp, "compareOp is null");

      // TODO (issue #1704): only NOT_EQUALS should allow null.  Everything else should use ifNotExists();
      this.value = value;
      return this;
    }

    public RequestBuilder timeRange(long start, long end) {
      if (start == 0L && end == Long.MAX_VALUE) {
        return this;
      }
      Preconditions.checkArgument((0L <= start) && (start < end), "Invalid start/end");
      this.timeFilter = new TimestampRangeFilter(start, end);
      return this;
    }

    public RequestBuilder withPut(Put put) throws DoNotRetryIOException {
      adaptException(put.getRow());
      hbaseAdapter.adapt(put, mutations);
      return this;
    }

    public RequestBuilder withDelete(Delete delete) throws DoNotRetryIOException {
      adaptException(delete.getRow());
      hbaseAdapter.adapt(delete, mutations);
      return this;
    }

    public RequestBuilder withMutations(RowMutations rm) throws DoNotRetryIOException {
      adaptException(rm.getRow());
      hbaseAdapter.adapt(rm, mutations);
      return this;
    }

    private void adaptException(byte[] actionRow) throws DoNotRetryIOException {
      if (!Arrays.equals(actionRow, row)) {
        // The following odd exception message is for compatibility with HBase.
        throw new DoNotRetryIOException("Action's getRow must match the passed row");
      }
    }

    public ConditionalRowMutation build() {
      Preconditions.checkState(checkNonExistence || compareOp != null,
          "condition is null. You need to specify the condition by" +
          " calling ifNotExists/ifEquals/ifMatches before executing the request");
      ConditionalRowMutation conditionalRowMutation = ConditionalRowMutation.create(hbaseAdapter.getBigtableTableName().getTableId(), ByteString.copyFrom(row));
      Scan scan = new Scan();
      scan.setMaxVersions(1);
      scan.addColumn(family, qualifier);

      if (value == null || checkNonExistence) {
        // See ifMatches javadoc for more information on this
        if (CompareOp.NOT_EQUAL.equals(compareOp)) {
          // check for existence
          conditionalRowMutation.then(mutations);
        } else {
          // check for non-existence
          conditionalRowMutation.otherwise(mutations);
        }
        if (timeFilter != null) {
          scan.setFilter(timeFilter);
        }
      } else {
        ValueFilter valueFilter =
            new ValueFilter(reverseCompareOp(compareOp), new BinaryComparator(value));
        if (timeFilter != null) {
          scan.setFilter(new FilterList(timeFilter, valueFilter));
        } else {
          scan.setFilter(valueFilter);
        }
        conditionalRowMutation.then(mutations);
      }
      conditionalRowMutation.condition(FILTERS.fromProto(Adapters.SCAN_ADAPTER.buildFilter(scan, UNSUPPORTED_READ_HOOKS)));

      return conditionalRowMutation;
    }
  }

  /**
   * For some reason, the ordering of CheckAndMutate operations is the inverse order of normal
   * {@link ValueFilter} operations.
   *
   * @param compareOp
   * @return the inverse of compareOp
   */
  private static CompareOp reverseCompareOp(CompareOp compareOp) {
    switch (compareOp) {
    case EQUAL:
    case NOT_EQUAL:
    case NO_OP:
      return compareOp;
    case LESS:
      return CompareOp.GREATER;
    case LESS_OR_EQUAL:
      return CompareOp.GREATER_OR_EQUAL;
    case GREATER:
      return CompareOp.LESS;
    case GREATER_OR_EQUAL:
      return CompareOp.LESS_OR_EQUAL;
    default:
      return CompareOp.NO_OP;
    }
  }
}
