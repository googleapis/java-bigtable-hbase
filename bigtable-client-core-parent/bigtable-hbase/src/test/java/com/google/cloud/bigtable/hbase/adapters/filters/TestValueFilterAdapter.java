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

import java.io.IOException;

import com.google.cloud.bigtable.data.v2.wrappers.Filters;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.bigtable.v2.ValueRange;
import com.google.bigtable.v2.ValueRange.Builder;
import com.google.protobuf.ByteString;

@RunWith(JUnit4.class)
public class TestValueFilterAdapter {
  private static final byte[] FOO_BYTES = Bytes.toBytes("Foo");
  ByteString FOO_BYTESTRING = ByteString.copyFrom(FOO_BYTES);
  private static final BinaryComparator FOO_BINARY_COMPARATOR =
      new BinaryComparator(FOO_BYTES);

  ValueFilterAdapter adapter = new ValueFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext emptyScanContext = new FilterAdapterContext(emptyScan, null);

  protected static RowFilter toRowFilter(Builder valueRange) {
    return RowFilter.newBuilder().setValueRangeFilter(valueRange).build();
  }

  @Test
  public void testValueFilterFiltersOnValue() {

  }

  @Test
  public void testLessThanValueFilter() throws IOException {
    assertAdaptedForm(FOO_BINARY_COMPARATOR, CompareOp.LESS,
      toRowFilter(ValueRange.newBuilder().setEndValueOpen(ByteString.copyFrom(FOO_BYTES))));
  }

  @Test
  public void testLessThanEqualValueFilter() throws IOException {
    assertAdaptedForm(FOO_BINARY_COMPARATOR, CompareOp.LESS_OR_EQUAL,
      toRowFilter(ValueRange.newBuilder().setEndValueClosed(FOO_BYTESTRING)));
  }

  @Test
  public void testEqualValueFilter() throws IOException {
    Builder valueRange = ValueRange.newBuilder()
        .setStartValueClosed(FOO_BYTESTRING)
        .setEndValueClosed(FOO_BYTESTRING);
    assertAdaptedForm(FOO_BINARY_COMPARATOR, CompareOp.EQUAL, toRowFilter(valueRange));
  }

  @Test
  public void testGreaterThanValueFilter() throws IOException {
    assertAdaptedForm(FOO_BINARY_COMPARATOR, CompareOp.GREATER,
      toRowFilter(ValueRange.newBuilder().setStartValueOpen(FOO_BYTESTRING)));
  }

  @Test
  public void testGreaterThanEqualValueFilter() throws IOException {
    assertAdaptedForm(FOO_BINARY_COMPARATOR, CompareOp.GREATER_OR_EQUAL,
      toRowFilter(ValueRange.newBuilder().setStartValueClosed(FOO_BYTESTRING)));
  }

  @Test
  public void testNotEqualEmptyStringValueFilter() throws IOException {
    assertAdaptedForm(new BinaryComparator("".getBytes()), CompareOp.NOT_EQUAL,
        RowFilter.newBuilder().setValueRegexFilter(ByteString.copyFrom(".+".getBytes())).build());
  }

  @Test
  public void testNotEqualValueFilter() throws IOException {
    assertAdaptedForm(FOO_BINARY_COMPARATOR, CompareOp.NOT_EQUAL,
      RowFilter.newBuilder()
          .setInterleave(Interleave.newBuilder()
              .addFilters(toRowFilter(ValueRange.newBuilder().setEndValueOpen(FOO_BYTESTRING)))
              .addFilters(toRowFilter(ValueRange.newBuilder().setStartValueOpen(FOO_BYTESTRING))))
          .build());
 }

  @Test
  public void testRegexValueFilter() throws IOException {
    String pattern = "Foo\\d+";
    assertAdaptedForm(
        new RegexStringComparator(pattern),
        CompareOp.EQUAL,
        RowFilter.newBuilder()
            .setValueRegexFilter(ByteString.copyFromUtf8(pattern))
            .build());
  }

  private void assertAdaptedForm(
      ByteArrayComparable comparable, CompareFilter.CompareOp op, RowFilter expectedFilter)
      throws IOException {
    ValueFilter filter = new ValueFilter(op, comparable);
    RowFilter actualFilter = adapter.adapt(emptyScanContext, filter);
    Assert.assertEquals(expectedFilter, actualFilter);
  }
}
