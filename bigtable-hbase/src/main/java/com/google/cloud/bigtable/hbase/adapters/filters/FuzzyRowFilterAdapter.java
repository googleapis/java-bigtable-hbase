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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.util.Pair;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper;
import com.google.cloud.bigtable.hbase.adapters.ReaderExpressionHelper.QuoteMetaOutputStream;
import com.google.protobuf.ByteString;

/**
 * An adapter for {@link FuzzyRowFilter}.
 */
public class FuzzyRowFilterAdapter implements TypedFilterAdapter<FuzzyRowFilter> {
  private static final RowFilter ALL_VALUES_FILTER =
      RowFilter.newBuilder()
          .setCellsPerColumnLimitFilter(Integer.MAX_VALUE)
          .build();

  @Override
  public RowFilter adapt(FilterAdapterContext context, FuzzyRowFilter filter) throws IOException {
    Interleave.Builder interleaveBuilder = Interleave.newBuilder();
    List<Pair<byte[], byte[]>> pairs = extractFuzzyRowFilterPairs(filter);
    if (pairs.isEmpty()) {
      return ALL_VALUES_FILTER;
    }
    for (Pair<byte[], byte[]> pair : pairs) {
      Preconditions.checkArgument(
          pair.getFirst().length == pair.getSecond().length,
          "Fuzzy info and match mask must have the same length");
      interleaveBuilder.addFilters(
          createSingleRowFilter(
              pair.getFirst(), pair.getSecond()));
    }
    return RowFilter.newBuilder().setInterleave(interleaveBuilder).build();
  }

  private static RowFilter createSingleRowFilter(byte[] first, byte[] second) throws IOException {
    ByteArrayOutputStream baos =
        new ByteArrayOutputStream(first.length * 2);
    QuoteMetaOutputStream quotingStream = new QuoteMetaOutputStream(baos);
    for (int i = 0; i < first.length; i++) {
      if (first[i] == 0) {
        quotingStream.write(second[i]);
      } else {
        // Write unquoted to match any byte at this position:
        baos.write(ReaderExpressionHelper.ANY_BYTE_BYTES);
      }
    }
    ByteString quotedValue = ByteString.copyFrom(baos.toByteArray());
    return RowFilter.newBuilder().setRowKeyRegexFilter(quotedValue).build();
  }

  private static List<Pair<byte[], byte[]>> extractFuzzyRowFilterPairs(FuzzyRowFilter filter)
      throws IOException {
    // TODO: Change FuzzyRowFilter to expose fuzzyKeysData.
    FilterProtos.FuzzyRowFilter filterProto =
        FilterProtos.FuzzyRowFilter.parseFrom(filter.toByteArray());
    List<Pair<byte[], byte[]>> result =
        new ArrayList<>(filterProto.getFuzzyKeysDataCount());
    for (BytesBytesPair protoPair : filterProto.getFuzzyKeysDataList()) {
      result.add(
          new Pair<>(
              protoPair.getFirst().toByteArray(),
              protoPair.getSecond().toByteArray()));
    }
    return result;
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, FuzzyRowFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
