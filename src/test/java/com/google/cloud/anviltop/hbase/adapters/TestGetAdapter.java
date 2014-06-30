/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopServices.GetRowRequest;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestGetAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected GetAdapter getAdapter = new GetAdapter();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  private Get makeValidGet(byte[] rowKey) throws IOException {
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    return get;
  }

  @Test
  public void testBasicRowKeyGet() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Get get = makeValidGet(rowKey);
    GetRowRequest.Builder rowRequestBuilder = getAdapter.adapt(get);
    ByteString adaptedRowKey = rowRequestBuilder.getRowKey();
    Assert.assertArrayEquals(rowKey, adaptedRowKey.toByteArray());
  }

  @Test
  public void testVersionLimitingIsNotSupported() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1-"));
    get.setMaxVersions(1);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Limiting of versions on Get is not supported.");

    getAdapter.adapt(get);
  }

  @Test
  public void testTimeRangesAreNotSupported() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1-"));
    get.setTimeStamp(100L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Time range limiting is not supported");

    getAdapter.adapt(get);
  }

  @Test
  public void testMaxCellsPerColumnFamilyIsNotSupported() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1-"));
    get.setMaxResultsPerColumnFamily(10);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Limiting of max results per column family is not supported");

    getAdapter.adapt(get);
  }

  @Test
  public void testFiltersAreNotSupported() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1-"));
    get.setFilter(
        new SingleColumnValueFilter(
          Bytes.toBytes("family1"),
          Bytes.toBytes("qualifier1"),
          CompareOp.EQUAL,
          Bytes.toBytes("someValue")));

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Filters on Get are not supported.");

    getAdapter.adapt(get);
  }

  @Test
  public void testReturningSpecificFamiliesIsNotSupported() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1-"));
    get.addFamily(Bytes.toBytes("family1"));

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Limiting of column families returned is not supported.");

    getAdapter.adapt(get);
  }

  @Test
  public void testReturningSpecificQualifiersIsNotSupported() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1-"));
    get.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("qualifier1"));

    // Same message as column family limiting
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Limiting of column families returned is not supported.");

    getAdapter.adapt(get);
  }
}
