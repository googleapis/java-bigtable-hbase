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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v1.RowFilter;
import com.google.protobuf.ByteString;

@RunWith(JUnit4.class)
public class TestRowFilterAdapter {
  
  private FilterAdapterContext context;
  
  @Before
  public void before() {
    Scan emptyScan = new Scan();
    context = new FilterAdapterContext(emptyScan, null);
  }
  
  @Test
  public void testRegexAndEquals() throws IOException {
    RowFilterAdapter adapter = new RowFilterAdapter();
    String regexp = "^.*hello world.*$";
    RegexStringComparator comparator = new RegexStringComparator(regexp);
    org.apache.hadoop.hbase.filter.RowFilter filter = 
        new org.apache.hadoop.hbase.filter.RowFilter(
            CompareFilter.CompareOp.EQUAL, comparator);
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setRowKeyRegexFilter(
                ByteString.copyFrom(regexp.getBytes()))
            .build(),
        adapter.adapt(context, filter));
  }
  
  @Test
  public void testEmptyRegex() throws IOException {
    // What does BigTable do in this case?
    RowFilterAdapter adapter = new RowFilterAdapter();
    String regexp = "";
    RegexStringComparator comparator = new RegexStringComparator(regexp);
    org.apache.hadoop.hbase.filter.RowFilter filter = 
        new org.apache.hadoop.hbase.filter.RowFilter(
            CompareFilter.CompareOp.EQUAL, comparator);
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setRowKeyRegexFilter(
                ByteString.copyFrom(regexp.getBytes()))
            .build(),
        adapter.adapt(context, filter));
  }
  
  @Test
  public void testRegexNotEquals() throws IOException {
    RowFilterAdapter adapter = new RowFilterAdapter();
    String regexp = "^.*hello world.*$";
    RegexStringComparator comparator = new RegexStringComparator(regexp);
    org.apache.hadoop.hbase.filter.RowFilter filter = 
        new org.apache.hadoop.hbase.filter.RowFilter(
            CompareFilter.CompareOp.GREATER_OR_EQUAL, comparator);
    Assert.assertFalse(adapter.isFilterSupported(context, filter).isSupported());
  }
  
  @Test
  public void testNotRegex() throws IOException {
    RowFilterAdapter adapter = new RowFilterAdapter();
    BinaryComparator comparator = new BinaryComparator(new byte[] { 0, 1, 2 });
    org.apache.hadoop.hbase.filter.RowFilter filter = 
        new org.apache.hadoop.hbase.filter.RowFilter(
            CompareFilter.CompareOp.EQUAL, comparator);
    Assert.assertFalse(adapter.isFilterSupported(context, filter).isSupported());
  }
}
