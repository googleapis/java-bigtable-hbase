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

import com.google.cloud.bigtable.data.v2.models.Filters;
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
      ByteArrayComparable comparable, CompareFilter.CompareOp op, Filters.Filter expectedFilter)
      throws IOException {
    QualifierFilter filter = new QualifierFilter(op, comparable);
    Filters.Filter actualFilter = adapter.adapt(scanWithOnFamilyScanContext, filter);
    Assert.assertEquals(expectedFilter.toProto(), actualFilter.toProto());
  }

  @Test
  public void testLessThanQualifierFilter() throws IOException {
    FILTERS.qualifier().rangeWithinFamily(FAMILY_NAME).endOpen(FOO_BYTESTRING);
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.LESS,
        FILTERS.qualifier()
          .rangeWithinFamily(FAMILY_NAME)
          .endOpen(FOO_BYTESTRING));
  }

  @Test
  public void testLessThanEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.LESS_OR_EQUAL,
        FILTERS.qualifier()
          .rangeWithinFamily(FAMILY_NAME)
          .endClosed(FOO_BYTESTRING));
  }

  @Test
  public void testEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.EQUAL,
        FILTERS.qualifier()
          .regex(ByteString.copyFrom(FOO_BYTES)));
  }

  @Test
  public void testGreaterThanQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.GREATER,
        FILTERS.qualifier()
          .rangeWithinFamily(FAMILY_NAME)
          .startOpen(FOO_BYTESTRING));
  }

  @Test
  public void testGreaterThanEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.GREATER_OR_EQUAL,
        FILTERS.qualifier()
          .rangeWithinFamily(FAMILY_NAME)
          .startClosed(FOO_BYTESTRING));
  }

  @Test
  public void testNotEqualQualifierFilter() throws IOException {
    assertAdaptedForm(
        FOO_BINARY_COMPARATOR,
        CompareOp.NOT_EQUAL,
      FILTERS.interleave()
          .filter(FILTERS.qualifier()
            .rangeWithinFamily(FAMILY_NAME)
            .endOpen(FOO_BYTESTRING))
          .filter(FILTERS.qualifier()
            .rangeWithinFamily(FAMILY_NAME)
            .startOpen(FOO_BYTESTRING)));
  }

  @Test
  public void testRegexQualifierFilter() throws IOException {
    String pattern = "Foo\\d+";
    assertAdaptedForm(
        new RegexStringComparator(pattern),
        CompareOp.EQUAL,
        FILTERS.qualifier().regex(ByteString.copyFromUtf8(pattern)));
  }
}
