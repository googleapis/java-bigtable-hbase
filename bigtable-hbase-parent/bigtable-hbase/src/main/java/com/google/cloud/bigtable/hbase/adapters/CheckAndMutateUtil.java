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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;

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
   * <p>
   * makeConditionalMutationRequest.
   * </p>
   * @param hbaseAdapter a {@link HBaseRequestAdapter} used to convert HBase
   *          {@link org.apache.hadoop.hbase.client.Mutation} to Cloud Bigtable {@link Mutation}
   * @param row an array of byte.
   * @param family an array of byte.
   * @param qualifier an array of byte.
   * @param compareOp a {@link org.apache.hadoop.hbase.filter.CompareFilter.CompareOp} object.
   * @param value an array of byte.
   * @param actionRow an array of byte.
   * @param mutations a {@link java.util.List} object.
   * @return a {@link com.google.bigtable.v2.CheckAndMutateRowRequest.Builder} object.
   * @throws java.io.IOException if any.
   */
  public static CheckAndMutateRowRequest makeConditionalMutationRequest(
      HBaseRequestAdapter hbaseAdapter,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareFilter.CompareOp compareOp,
      byte[] value,
      byte[] actionRow,
      List<com.google.bigtable.v2.Mutation> mutations) throws IOException {
    if (!Arrays.equals(actionRow, row)) {
      // The following odd exception message is for compatibility with HBase.
      throw new DoNotRetryIOException("Action's getRow must match the passed row");
    }

    CheckAndMutateRowRequest.Builder requestBuilder =
        CheckAndMutateRowRequest.newBuilder();

    requestBuilder.setTableName(hbaseAdapter.getBigtableTableName().toString());

    requestBuilder.setRowKey(ByteString.copyFrom(row));
    Scan scan = new Scan().addColumn(family, qualifier);
    scan.setMaxVersions(1);
    if (value == null) {
      // If we don't have a value and we are doing CompareOp.EQUAL, we want to mutate if there
      // is no cell with the qualifier. If we are doing CompareOp.NOT_EQUAL, we want to mutate
      // if there is any cell. We don't actually want an extra filter for either of these cases,
      // but we do need to invert the compare op.
      if (CompareFilter.CompareOp.EQUAL.equals(compareOp)) {
        requestBuilder.addAllFalseMutations(mutations);
      } else if (CompareFilter.CompareOp.NOT_EQUAL.equals(compareOp)) {
        requestBuilder.addAllTrueMutations(mutations);
      }
    } else {
      ValueFilter valueFilter =
          new ValueFilter(reverseCompareOp(compareOp), new BinaryComparator(value));
      scan.setFilter(valueFilter);
      requestBuilder.addAllTrueMutations(mutations);
    }
    requestBuilder.setPredicateFilter(
      Adapters.SCAN_ADAPTER.buildFilter(scan, UNSUPPORTED_READ_HOOKS));
    return requestBuilder.build();
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
