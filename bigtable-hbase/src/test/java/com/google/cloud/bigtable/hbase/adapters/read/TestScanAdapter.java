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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowRange;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ScanAdapter;
import com.google.common.base.Function;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Lightweight tests for the ScanAdapter. Many of the methods, such as filter building are
 * already tested in {@link TestGetAdapter}.
 */
@RunWith(JUnit4.class)
public class TestScanAdapter {

  private ScanAdapter scanAdapter = new ScanAdapter(FilterAdapter.buildAdapter());
  private ReadHooks throwingReadHooks = new ReadHooks() {
    @Override
    public void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook) {
      throw new IllegalStateException("Read hooks not supported in TestScanAdapter.");
    }

    @Override
    public ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest) {
      throw new IllegalStateException("Read hooks not supported in TestScanAdapter.");
    }
  };

  @Test
  public void testStartAndEndKeysAreSet() {
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan();
    scan.setStartRow(startKey);
    scan.setStopRow(stopKey);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan, throwingReadHooks);
    RowRange range = request.getRows().getRowRanges(0);
    Assert.assertArrayEquals(startKey, range.getStartKeyClosed().toByteArray());
    Assert.assertArrayEquals(stopKey, range.getEndKeyOpen().toByteArray());
  }

  @Test
  public void maxVersionsIsSet() {
    Scan scan = new Scan();
    scan.setMaxVersions(10);
    ReadRowsRequest.Builder rowRequestBuilder = scanAdapter.adapt(scan, throwingReadHooks);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter(".*"))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(10))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }
}
