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

package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsRequest.TargetCase;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * Lightweight tests for the ScanAdapter. Many of the methods, such as filter building are
 * already tested in {@link TestGetAdapter}.
 */
@RunWith(JUnit4.class)
public class TestScanAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  ScanAdapter scanAdapter = new ScanAdapter(new FilterAdapter());

  @Test
  public void testFilterStringIsSet() {
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan);
    Assert.assertEquals("((col({family:qualifier}, 1)))", request.getDEPRECATEDStringFilter());
  }

  @Test
  public void testStartAndEndKeysAreSet() {
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan();
    scan.setStartRow(startKey);
    scan.setStopRow(stopKey);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan);
    Assert.assertEquals(TargetCase.ROW_RANGE, request.getTargetCase());
    Assert.assertArrayEquals(startKey, request.getRowRange().getStartKey().toByteArray());
    Assert.assertArrayEquals(stopKey, request.getRowRange().getEndKey().toByteArray());
  }
}
