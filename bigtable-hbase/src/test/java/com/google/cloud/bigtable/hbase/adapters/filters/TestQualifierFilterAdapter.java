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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.protobuf.ByteString;

/**
 * Tests for the {@link QualifierFilterAdapter}
 */
@RunWith(JUnit4.class)
public class TestQualifierFilterAdapter {
  private static final byte[] FOO_BYTES = Bytes.toBytes("Foo");
  private static final ByteString FOO_BYTESTRING =
      ByteString.copyFrom(FOO_BYTES);
  private static final BinaryComparator FOO_BINARY_COMPARATOR =
      new BinaryComparator(FOO_BYTES);
  private static final String FAMILY_NAME = "f1";

  QualifierFilterAdapter adapter = new QualifierFilterAdapter();
  Scan scanWithOnFamily = new Scan().addFamily(Bytes.toBytes(FAMILY_NAME));
  FilterAdapterContext scanWithOnFamilyScanContext =
      new FilterAdapterContext(scanWithOnFamily, null);

  private void assertAdaptedForm(
      ByteArrayComparable comparable, CompareFilter.CompareOp op, RowFilter expectedFilter)
      throws IOException {
    QualifierFilter filter = new QualifierFilter(op, comparable);
    RowFilter actualFilter = adapter.adapt(scanWithOnFamilyScanContext, filter);
    Assert.assertEquals(expectedFilter, actualFilter);
  }

  @Test
  public void testLessThanQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.LESS,
        RowFilter.newBuilder()
            .setColumnRangeFilter(
                ColumnRange.newBuilder()
                    .setFamilyName(FAMILY_NAME)
                    .setEndQualifierExclusive(FOO_BYTESTRING))
            .build());
  }

  @Test
  public void testLessThanEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.LESS_OR_EQUAL,
        RowFilter.newBuilder()
            .setColumnRangeFilter(
                ColumnRange.newBuilder()
                    .setFamilyName(FAMILY_NAME)
                    .setEndQualifierInclusive(FOO_BYTESTRING))
            .build());
  }

  @Test
  public void testEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.EQUAL,
        RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(
                ByteString.copyFrom(FOO_BYTES)).build());
  }

  @Test
  public void testGreaterThanQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.GREATER,
        RowFilter.newBuilder()
            .setColumnRangeFilter(
                ColumnRange.newBuilder()
                    .setFamilyName(FAMILY_NAME)
                    .setStartQualifierExclusive(FOO_BYTESTRING))
            .build());
  }

  @Test
  public void testGreaterThanEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.GREATER_OR_EQUAL,
        RowFilter.newBuilder()
            .setColumnRangeFilter(
                ColumnRange.newBuilder()
                    .setFamilyName(FAMILY_NAME)
                    .setStartQualifierInclusive(FOO_BYTESTRING))
            .build());
  }

  @Test
  public void testNotEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.NOT_EQUAL,
        RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder()
                    .addFilters(
                        RowFilter.newBuilder()
                            .setColumnRangeFilter(
                                ColumnRange.newBuilder()
                                    .setFamilyName(FAMILY_NAME)
                                    .setEndQualifierExclusive(FOO_BYTESTRING)))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setColumnRangeFilter(
                                ColumnRange.newBuilder()
                                    .setFamilyName(FAMILY_NAME)
                                    .setStartQualifierExclusive(FOO_BYTESTRING))))
            .build());
  }

  @Test
  public void testRegexQualifierFilter() throws IOException {
    String pattern = "Foo\\d+";
    assertAdaptedForm(
        new RegexStringComparator(pattern),
        CompareOp.EQUAL,
        RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(ByteString.copyFromUtf8(pattern))
            .build());
  }
}
