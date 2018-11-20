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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext.ContextCloseable;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestPageFilterAdapter {

  PageFilterAdapter pageFilterAdapter = new PageFilterAdapter();
  @Test
  public void mustPassOneIsNotSupported() {
    FilterAdapterContext context = new FilterAdapterContext(new Scan(), new DefaultReadHooks());
    PageFilter filter = new PageFilter(20);
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, filter);
    try(ContextCloseable ignroed = context.beginFilterList(filterList)) {
      FilterSupportStatus status =
          pageFilterAdapter.isFilterSupported(context, filter);
      Assert.assertFalse(
          "MUST_PASS_ONE FilterLists should not be supported.", status.isSupported());
    }
  }

  @Test
  public void topLevelPageFilterIsSupported() {
    FilterAdapterContext context = new FilterAdapterContext(new Scan(), new DefaultReadHooks());
    PageFilter filter = new PageFilter(20);
    FilterSupportStatus status =
        pageFilterAdapter.isFilterSupported(context, filter);
    Assert.assertTrue(
        "Top-level page filter should be supported", status.isSupported());
  }

  @Test
  public void mustPassAllIsSupportedAtTopLevel() {
    FilterAdapterContext context = new FilterAdapterContext(new Scan(), new DefaultReadHooks());
    PageFilter filter = new PageFilter(20);
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, filter);
    try(ContextCloseable ignored = context.beginFilterList(filterList)) {
      FilterSupportStatus status =
          pageFilterAdapter.isFilterSupported(context, filter);
      Assert.assertTrue(
          "MUST_PASS_ALL should be supported at the top-level.", status.isSupported());
    }
  }

  @Test
  public void pageFilterMustBeInLastPosition() {
    FilterAdapterContext context = new FilterAdapterContext(new Scan(), new DefaultReadHooks());
    ValueFilter valueFilter =
        new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("value")));
    PageFilter pageFilter = new PageFilter(20);
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, pageFilter, valueFilter);
    try(ContextCloseable ignored = context.beginFilterList(filterList)) {
      FilterSupportStatus status =
          pageFilterAdapter.isFilterSupported(context, pageFilter);
      Assert.assertFalse(
          "PageFilter must be in the last position of a MUST_PASS_ALL filter list",
          status.isSupported());
    }
  }

  @Test
  public void mustPassAllIsNotSupportedBelowTopLevel() {
    FilterAdapterContext context = new FilterAdapterContext(new Scan(), new DefaultReadHooks());
    PageFilter pageFilter = new PageFilter(20);
    FilterList secondLevelList = new FilterList(Operator.MUST_PASS_ALL, pageFilter);
    FilterList topLevelList = new FilterList(Operator.MUST_PASS_ALL, secondLevelList);
    try (ContextCloseable ignored = context.beginFilterList(topLevelList)) {
      try (ContextCloseable evenMoreIgnored = context.beginFilterList(secondLevelList)) {
        FilterSupportStatus status =
            pageFilterAdapter.isFilterSupported(context, pageFilter);
        Assert.assertFalse(
            "MUST_PASS_ALL should not be supported lower than top level.", status.isSupported());
      }
    }
  }

  @Test
  public void pageFilterIsAppliedToReadRowsRequest() throws IOException {
    ReadHooks hooks = new DefaultReadHooks();
    FilterAdapterContext context = new FilterAdapterContext(new Scan(), hooks);
    PageFilter pageFilter = new PageFilter(20);
    Filters.Filter adaptedFilter = pageFilterAdapter.adapt(context, pageFilter);
    Assert.assertNull("PageFilterAdapter should not return a Filters.Filter.", adaptedFilter);

    ReadRowsRequest request = ReadRowsRequest.newBuilder().setRowsLimit(100).build();
    ReadRowsRequest postHookRequest = hooks.applyPreSendHook(request);
    Assert.assertEquals(20, postHookRequest.getRowsLimit());
  }
}
