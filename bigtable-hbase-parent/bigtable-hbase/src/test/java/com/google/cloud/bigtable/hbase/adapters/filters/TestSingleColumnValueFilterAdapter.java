/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

  SingleColumnValueFilterAdapter adapter =
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

    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan(), null),
        filter);

    assertFilterIfNotMIssingMatches(
        family,
        qualifier,
        filterValue,
        1 /* latest version only = true */,
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

    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan(), null),
        filter);

    assertFilterIfNotMIssingMatches(
        family,
        qualifier,
        filterValue,
        Integer.MAX_VALUE /* latest version only = false */,
        adaptedFilter);
  }

  @Test
  public void filterIfMissingIsApplied() throws IOException {
    byte[] filterValue = Bytes.toBytes("foobar");
    byte[] qualifier = Bytes.toBytes("someColumn");
    byte[] family = Bytes.toBytes("f");

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        family,
        qualifier,
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(filterValue));

    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(false);

    RowFilter adaptedFilter = adapter.adapt(
        new FilterAdapterContext(new Scan(), null),
        filter);

    assertColumnSpecification(
        Bytes.toString(family),
        qualifier,
        Integer.MAX_VALUE,
        adaptedFilter
            .getCondition()
            .getPredicateFilter()
            .getChain()
            .getFilters(0)
            .getChain());

    Assert.assertEquals(
        RowFilter.newBuilder()
            .setValueRegexFilter(ByteString.copyFromUtf8("foobar"))
            .build(),
        adaptedFilter
            .getCondition()
            .getPredicateFilter()
            .getChain()
            .getFilters(1));

    Assert.assertEquals(
        RowFilter.newBuilder()
            .setCellsPerColumnLimitFilter(Integer.MAX_VALUE)
            .build(),
        adaptedFilter.getCondition().getTrueFilter());
  }

  // Assert that the given family, qualifier and versions are applied
  // via the given Chain.
  private void assertColumnSpecification(
      String family, byte[] qualifier, int versions, Chain chain) {
    Chain expected =
        Chain.newBuilder()
          .addFilters(
              RowFilter.newBuilder()
                  .setFamilyNameRegexFilter(family))
          .addFilters(
              RowFilter.newBuilder()
                  .setColumnQualifierRegexFilter(
                      ByteString.copyFrom(qualifier)))
          .addFilters(
              RowFilter.newBuilder()
                  .setCellsPerColumnLimitFilter(versions))
        .build();

    Assert.assertEquals(expected, chain);
  }

  private void assertFilterIfNotMIssingMatches(
      byte[] family,
      byte[] qualifier,
      byte[] value,
      int versions,
      RowFilter adaptedFilter) {
    // We won't be doing a filter if missing, so start by seeing if the cell
    // is in the row - if it is, we'll perform the value filter otherwise we'll
    // include all cells in the row:
    assertColumnSpecification(
        Bytes.toString(family),
        qualifier,
        versions,
        adaptedFilter
            .getCondition()
            .getPredicateFilter()
            .getChain());

    // Cell is not in the row, include all cells in the false branch:
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setCellsPerColumnLimitFilter(
                Integer.MAX_VALUE)
            .build(),
        adaptedFilter.getCondition().getFalseFilter());

    RowFilter cellExistsBranch =
        adaptedFilter
            .getCondition()
            .getTrueFilter();

    assertColumnSpecification(
        Bytes.toString(family),
        qualifier,
        versions,
        cellExistsBranch
            .getCondition()
            .getPredicateFilter()
            .getChain()
            .getFilters(0)
            .getChain());

    // Assert that the condition includes a value filter:
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setValueRegexFilter(ByteString.copyFrom(value))
            .build(),
        cellExistsBranch
            .getCondition()
            .getPredicateFilter()
            .getChain()
            .getFilters(1));

    // Include all cells that match the value filter and cell spec:
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setCellsPerColumnLimitFilter(Integer.MAX_VALUE)
            .build(),
        cellExistsBranch
            .getCondition()
            .getTrueFilter());
  }
}
