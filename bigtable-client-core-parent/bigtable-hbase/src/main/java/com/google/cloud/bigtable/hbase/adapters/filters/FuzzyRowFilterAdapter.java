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

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.FILTERS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import com.google.cloud.bigtable.config.Logger;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.cloud.bigtable.data.v2.wrappers.Filters.Filter;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.wrappers.Filters.InterleaveFilter;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper;
import com.google.cloud.bigtable.hbase.adapters.read.ReaderExpressionHelper.QuoteMetaOutputStream;
import com.google.common.base.Preconditions;

/**
 * An adapter for {@link org.apache.hadoop.hbase.filter.FuzzyRowFilter}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class FuzzyRowFilterAdapter extends TypedFilterAdapterBase<FuzzyRowFilter> {
  private static final RowFilter ALL_VALUES_FILTER = FILTERS.pass().toProto();

  private static Field FUZZY_KEY_DATA_FIELD;
  private static Exception FUZZY_KEY_DATA_FIELD_EXCEPTION;

  static {
    try {
      FUZZY_KEY_DATA_FIELD = FuzzyRowFilter.class.getDeclaredField("fuzzyKeysData");
      FUZZY_KEY_DATA_FIELD.setAccessible(true);
    } catch (NoSuchFieldException | SecurityException e) {
      FUZZY_KEY_DATA_FIELD_EXCEPTION = e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public RowFilter adapt(FilterAdapterContext context, FuzzyRowFilter filter) throws IOException {
    List<Pair<byte[], byte[]>> pairs = extractFuzzyRowFilterPairs(filter);
    if (pairs.isEmpty()) {
      return ALL_VALUES_FILTER;
    }
    InterleaveFilter interleave = FILTERS.interleave();
    for (Pair<byte[], byte[]> pair : pairs) {
      Preconditions.checkArgument(
          pair.getFirst().length == pair.getSecond().length,
          "Fuzzy info and match mask must have the same length");
      interleave.filter(
          createSingleRowFilter(
              pair.getFirst(), pair.getSecond()));
    }
    new Logger(FuzzyRowFilterAdapter.class).error("Adapted: " + interleave.toProto());
    return interleave.toProto();
  }

  private static Filter createSingleRowFilter(byte[] key, byte[] mask) throws IOException {
    ByteArrayOutputStream baos =
        new ByteArrayOutputStream(key.length * 2);
    QuoteMetaOutputStream quotingStream = new QuoteMetaOutputStream(baos);
    for (int i = 0; i < mask.length; i++) {
      if (mask[i] == -1) {
        quotingStream.write(key[i]);
      } else {
        // Write unquoted to match any byte at this position:
        baos.write(ReaderExpressionHelper.ANY_BYTE_BYTES);
      }
    }
    // match any trailing bytes
    baos.write(ReaderExpressionHelper.ALL_BYTE_BYTES);
    quotingStream.close();
    return FILTERS.key().regex(Bytes.toString(baos.toByteArray()));
  }

  @SuppressWarnings("unchecked")
  static List<Pair<byte[], byte[]>> extractFuzzyRowFilterPairs(FuzzyRowFilter filter)
      throws IOException {
    // TODO: Change FuzzyRowFilter to expose fuzzyKeysData.
    if(FUZZY_KEY_DATA_FIELD_EXCEPTION != null) {
      throw new IOException("Could not read the contents of the FuzzyRowFilter");
    }
    try {
      return (List<Pair<byte[], byte[]>>) FUZZY_KEY_DATA_FIELD.get(filter);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new IOException("Could not read the contents of the FuzzyRowFilter", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context, FuzzyRowFilter filter) {
    return FilterSupportStatus.SUPPORTED;
  }
}
