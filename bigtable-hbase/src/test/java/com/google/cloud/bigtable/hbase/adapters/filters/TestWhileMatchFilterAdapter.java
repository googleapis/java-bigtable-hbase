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

import static com.google.cloud.bigtable.hbase.adapters.filters.WhileMatchFilterAdapter.IN_LABEL_SUFFIX;
import static com.google.cloud.bigtable.hbase.adapters.filters.WhileMatchFilterAdapter.OUT_LABEL_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.hbase.adapters.DefaultReadHooks;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
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
    RowFilter rowFilter = instance.adapt(emptyScanContext, filter);
    RowFilter expectedFilter = buildExpectedRowFilter(
        filterAdapter.adaptFilter(emptyScanContext, valueFilter).get(),
        emptyScanContext.getCurrentUniqueId());
    assertEquals(expectedFilter, rowFilter);
  }

  private static RowFilter buildExpectedRowFilter(
      RowFilter wrappedFilter, String whileMatchFileterId) {
    RowFilter sink = RowFilter.newBuilder().setSink(true).build();
    RowFilter inLabel =
        RowFilter.newBuilder()
            .setApplyLabelTransformer(whileMatchFileterId + IN_LABEL_SUFFIX)
            .build();
    RowFilter outLabel =
        RowFilter.newBuilder()
            .setApplyLabelTransformer(whileMatchFileterId + OUT_LABEL_SUFFIX)
            .build();
    RowFilter outLabelAndSink =
        RowFilter.newBuilder()
            .setChain(Chain.newBuilder().addAllFilters(ImmutableList.of(outLabel, sink)))
            .build();

    RowFilter all = RowFilter.newBuilder().setPassAllFilter(true).build();
    RowFilter outInterleave =
        RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder().addAllFilters(ImmutableList.of(outLabelAndSink, all)))
            .build();
    return RowFilter.newBuilder()
        .setInterleave(Interleave.newBuilder()
            .addAllFilters(ImmutableList.of(
                RowFilter.newBuilder()
                    .setChain(Chain.newBuilder()
                        .addAllFilters(ImmutableList.of(inLabel, sink)))
                    .build(),
                RowFilter.newBuilder()
                    .setChain(Chain.newBuilder()
                        .addAllFilters(ImmutableList.of(wrappedFilter, outInterleave)))
                    .build())))
        .build();
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
    assertEquals(
        FilterSupportStatus.SUPPORTED, instance.isFilterSupported(emptyScanContext, filter));
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
    assertFalse(instance.isFilterSupported(emptyScanContext, filter).isSupported());
  }
}
