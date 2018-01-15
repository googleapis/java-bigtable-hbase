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

import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Condition;
import com.google.bigtable.v2.ValueRange;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestSingleColumnValueFilterAdapter  {

  static final SingleColumnValueFilterAdapter UNDER_TEST =
      new SingleColumnValueFilterAdapter(new ValueFilterAdapter());

  @Test
  public void latestVersionOnlyComparisonsAreDone() throws IOException {
    byte[] filterValue = Bytes.toBytes("foobar");
    byte[] qualifier = Bytes.toBytes("someColumn");
    byte[] family = Bytes.toBytes("f");

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        family,
        qualifier,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    filter.setFilterIfMissing(false);
    filter.setLatestVersionOnly(true);

    RowFilter adaptedFilter = UNDER_TEST.adapt(
        new FilterAdapterContext(new Scan(), null),
        filter);

    assertFilterIfNotMIssingMatches(
        family,
        qualifier,
        filterValue,
        true,
        adaptedFilter);
  }

  @Test
  public void allVersionComparisonAreDone() throws IOException {
    byte[] filterValue = Bytes.toBytes("foobar");
    byte[] qualifier = Bytes.toBytes("someColumn");
    byte[] family = Bytes.toBytes("f");

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        family,
        qualifier,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    filter.setFilterIfMissing(false);
    filter.setLatestVersionOnly(false);

    RowFilter adaptedFilter = UNDER_TEST.adapt(
        new FilterAdapterContext(new Scan(), null),
        filter);

    assertFilterIfNotMIssingMatches(
        family,
        qualifier,
        filterValue,
        false,
        adaptedFilter);
  }

  @Test
  public void filterIfMissingIsApplied() throws IOException {
    String valueStr = "foobar";
    byte[] filterValue = Bytes.toBytes(valueStr);
    byte[] qualifier = Bytes.toBytes("someColumn");
    byte[] family = Bytes.toBytes("f");

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        family,
        qualifier,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(false);

    RowFilter adaptedFilter = UNDER_TEST.adapt(
        new FilterAdapterContext(new Scan(), null),
        filter);

    assertColumnSpecification(
        family,
        qualifier,
        false,
        adaptedFilter
            .getCondition()
            .getPredicateFilter());

    Assert.assertEquals(
        createValueRangeFilter(valueStr),
        getValueRangeFilter(
            adaptedFilter
            .getCondition()
            .getPredicateFilter()
            .getChain()));

    Assert.assertEquals(
        SingleColumnValueFilterAdapter.ALL_VALUES_FILTER,
        adaptedFilter.getCondition().getTrueFilter());
  }

  private static RowFilter createValueRangeFilter(String valueStr) {
    ByteString value = ByteString.copyFromUtf8(valueStr);
    return RowFilter.newBuilder()
        .setValueRangeFilter(ValueRange.newBuilder()
            .setStartValueClosed(value)
            .setEndValueClosed(value))
        .build();
  }

  // Assert that the given family, qualifier and versions are applied
  // via the given Chain.
  private static void assertColumnSpecification(
      byte[] family, byte[] qualifier, boolean latestOnly, RowFilter filter) throws IOException {
    Chain chain = filter.getChain();
    Assert.assertEquals(
        Bytes.toString(family),
        chain.getFilters(0).getFamilyNameRegexFilter());
    Assert.assertArrayEquals(
        qualifier,
        chain.getFilters(1).getColumnQualifierRegexFilter().toByteArray());

    if (latestOnly) {
      Assert.assertEquals(
          SingleColumnValueFilterAdapter.LATEST_ONLY_FILTER,
          chain.getFilters(2));
    }
  }

  private static void assertFilterIfNotMIssingMatches(
      byte[] family,
      byte[] qualifier,
      byte[] value,
      boolean latestOnly,
      RowFilter adaptedFilter) throws IOException {
    Condition cellSetCondition = adaptedFilter.getInterleave().getFilters(0).getCondition();
    Condition cellUnsetCondition = adaptedFilter.getInterleave().getFilters(1).getCondition();

    // ---------------  Check the conditions -------------------/

    // Assert that if the cell is set, that the condition includes a column check:
    assertColumnSpecification(
        family,
        qualifier,
        latestOnly,
        cellSetCondition.getPredicateFilter());
 
    // Assert that the condition also includes a value filter:
    ByteString valueBS = ByteString.copyFrom(value);
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setValueRangeFilter(ValueRange.newBuilder()
                .setStartValueClosed(valueBS)
                .setEndValueClosed(valueBS))
            .build(),
            getValueRangeFilter(cellSetCondition.getPredicateFilter().getChain()));

    // If the cell is unset, the condition includes a column check:
    assertColumnSpecification(
      family,
      qualifier,
      latestOnly,
      cellUnsetCondition.getPredicateFilter());

    // ---------------  Check true / false filters -------------------/

    // Cell is in the row, include all cells in the true branch:
    Assert.assertEquals(
        SingleColumnValueFilterAdapter.ALL_VALUES_FILTER,
        cellSetCondition.getTrueFilter());

    Assert.assertFalse(cellSetCondition.hasFalseFilter());

    // Cell is not in the row, include all cells in the false branch:
    Assert.assertEquals(
        SingleColumnValueFilterAdapter.ALL_VALUES_FILTER,
        cellUnsetCondition.getFalseFilter());

    Assert.assertFalse(cellUnsetCondition.hasTrueFilter());
  }

  private static RowFilter getValueRangeFilter(Chain chain) {
    for (RowFilter filter : chain.getFiltersList()) {
      if (filter.hasValueRangeFilter()) {
        return filter;
      }
    }
    return null;
  }
}
