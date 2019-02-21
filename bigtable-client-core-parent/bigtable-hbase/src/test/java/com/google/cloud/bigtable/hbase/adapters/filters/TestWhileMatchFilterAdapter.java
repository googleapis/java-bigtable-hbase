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
import static com.google.cloud.bigtable.hbase.adapters.filters.WhileMatchFilterAdapter.IN_LABEL_SUFFIX;
import static com.google.cloud.bigtable.hbase.adapters.filters.WhileMatchFilterAdapter.OUT_LABEL_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestWhileMatchFilterAdapter {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  FilterAdapter filterAdapter = FilterAdapter.buildAdapter();
  FilterAdapterContext emptyScanContext = null;
  WhileMatchFilterAdapter instance = new WhileMatchFilterAdapter(filterAdapter);

  @Before
  public void setup() {
    emptyScanContext = new FilterAdapterContext(new Scan(), new DefaultReadHooks());
  }

  @Test
  public void nullWrappedFilter() throws IOException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("The wrapped filter for a WhileMatchFilter cannot be null.");

    WhileMatchFilter filter = new WhileMatchFilter(null);
    instance.adapt(emptyScanContext, filter);    
  }

  @Test
  public void simpleWrappedFilter() throws IOException {
    ValueFilter valueFilter =
        new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("12")));
    WhileMatchFilter filter = new WhileMatchFilter(valueFilter);
    Filters.Filter actualFilter = instance.adapt(emptyScanContext, filter);
    Filters.Filter expectedFilter = buildExpectedRowFilter(
        filterAdapter.adaptFilter(emptyScanContext, valueFilter).get(),
        emptyScanContext.getCurrentUniqueId());
    assertEquals(expectedFilter.toProto(), actualFilter.toProto());
  }

  @Test
  public void twoFiltersNotSupported() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("More than one WhileMatchFilter is not supported.");

    ValueFilter valueFilter =
        new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("12")));
    WhileMatchFilter filter = new WhileMatchFilter(valueFilter);
    instance.adapt(emptyScanContext, filter);
    instance.adapt(emptyScanContext, filter);
  }

  private static Filters.Filter buildExpectedRowFilter(
      Filters.Filter wrappedFilter, String whileMatchFileterId) {
    
    Filters.Filter sink = FILTERS.sink();
    Filters.Filter inLabel = FILTERS.label(whileMatchFileterId + IN_LABEL_SUFFIX);
    
    Filters.Filter outLabel = FILTERS.label(whileMatchFileterId + OUT_LABEL_SUFFIX);
    Filters.Filter outLabelAndSink = FILTERS.chain().filter(outLabel).filter(sink);

    Filters.Filter all = FILTERS.pass();
    Filters.Filter outInterleave = FILTERS.interleave().filter(outLabelAndSink).filter(all);
    return FILTERS.interleave()
        .filter(FILTERS.chain().filter(inLabel).filter(sink))
        .filter(FILTERS.chain().filter(wrappedFilter).filter(outInterleave));
  }

  @Test
  public void unableToAdaptWrappedFilter() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to adapted the wrapped filter: PageFilter 30");

    WhileMatchFilter filter = new WhileMatchFilter(new PageFilter(30));
    instance.adapt(emptyScanContext, filter);
  }

  @Test
  public void wrappedFilterSupported() {
    WhileMatchFilter filter = new WhileMatchFilter(new PageFilter(30));
    Scan scan = new Scan();
    scan.setFilter(filter);
    FilterAdapterContext context = new FilterAdapterContext(scan, new DefaultReadHooks());
    assertEquals(
        FilterSupportStatus.SUPPORTED, instance.isFilterSupported(context, filter));
  }

  @Test
  public void wrappedFilterNotSupported() {
    FilterBase notSupported = new FilterBase() {
      @Override
      public ReturnCode filterKeyValue(Cell v) throws IOException {
        return null;
      }
    };
    WhileMatchFilter filter = new WhileMatchFilter(notSupported);
    Scan scan = new Scan();
    scan.setFilter(filter);
    FilterAdapterContext context = new FilterAdapterContext(scan, new DefaultReadHooks());
    assertFalse(instance.isFilterSupported(context, filter).isSupported());
  }

  @Test
  public void notSupported_inInterleave() {
    QualifierFilter qualifierFilter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(qualifierFilter);
    FilterList list = new FilterList(Operator.MUST_PASS_ONE, whileMatchFilter);
    Scan scan = new Scan();
    scan.setFilter(list);
    FilterAdapterContext context = new FilterAdapterContext(scan, new DefaultReadHooks());
    assertFalse(instance.isFilterSupported(context, whileMatchFilter).isSupported());
  }

  @Test
  public void notSupported_inInterleave_inChain() {
    QualifierFilter qualifierFilter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(qualifierFilter);
    FilterList interleaveList = new FilterList(Operator.MUST_PASS_ONE, whileMatchFilter);
    FilterList chainList = new FilterList(Operator.MUST_PASS_ALL, interleaveList);
    Scan scan = new Scan();
    scan.setFilter(chainList);
    FilterAdapterContext context = new FilterAdapterContext(scan, new DefaultReadHooks());
    assertFalse(instance.isFilterSupported(context, whileMatchFilter).isSupported());
  }

  @Test
  public void notSupported_inChain_inInterleave() {
    QualifierFilter qualifierFilter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(qualifierFilter);
    FilterList chainList = new FilterList(Operator.MUST_PASS_ALL, whileMatchFilter);
    FilterList interleaveList = new FilterList(Operator.MUST_PASS_ONE, chainList);
    Scan scan = new Scan();
    scan.setFilter(interleaveList);
    FilterAdapterContext context = new FilterAdapterContext(scan, new DefaultReadHooks());
    assertFalse(instance.isFilterSupported(context, whileMatchFilter).isSupported());
  }

  @Test
  public void supported_inChain() {
    QualifierFilter qualifierFilterY =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("y")));
    FilterList interleaveList = new FilterList(Operator.MUST_PASS_ONE, qualifierFilterY);
    QualifierFilter qualifierFilterX =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(qualifierFilterX);
    FilterList chainList = new FilterList(Operator.MUST_PASS_ALL, whileMatchFilter, interleaveList);
    Scan scan = new Scan();
    scan.setFilter(chainList);
    FilterAdapterContext context = new FilterAdapterContext(scan, new DefaultReadHooks());
    assertTrue(instance.isFilterSupported(context, whileMatchFilter).isSupported());
  }
}
