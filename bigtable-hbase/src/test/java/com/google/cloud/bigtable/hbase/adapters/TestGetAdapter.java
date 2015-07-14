/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for the {@link GetAdapter}
 */
@RunWith(JUnit4.class)
public class TestGetAdapter {

  private GetAdapter getAdapter =
      new GetAdapter(new ScanAdapter(FilterAdapter.buildAdapter()));
  private DataGenerationHelper dataHelper = new DataGenerationHelper();
  private ReadHooks throwingReadHooks = new ReadHooks() {
    @Override
    public void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook) {
      throw new IllegalStateException("Read hooks not supported in tests.");
    }

    @Override
    public ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest) {
      throw new IllegalStateException("Read hooks not supported in tests.");
    }
  };

  private Get makeValidGet(byte[] rowKey) throws IOException {
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    return get;
  }

  @Test
  public void rowKeyIsSetInRequest() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    ByteString adaptedRowKey = rowRequestBuilder.getRowKey();
    Assert.assertEquals(
        new String(get.getRow(), StandardCharsets.UTF_8),
        adaptedRowKey.toStringUtf8());
  }

  @Test
  public void maxVersionsIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.setMaxVersions(10);
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter(".*"))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(10))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }

  @Test
  public void columnFamilyIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addFamily(Bytes.toBytes("f1"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter("f1"))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(Integer.MAX_VALUE))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }

  @Test
  public void columnQualifierIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(
                RowFilter.newBuilder().setChain(Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setFamilyNameRegexFilter("f1"))
                    .addFilters(RowFilter.newBuilder()
                        .setColumnQualifierRegexFilter(ByteString.copyFromUtf8("q1")))))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(Integer.MAX_VALUE))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }

  @Test
  public void multipleQualifiersAreSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q2"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(
                RowFilter.newBuilder().setChain(Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setFamilyNameRegexFilter("f1"))
                    .addFilters(RowFilter.newBuilder()
                        .setInterleave(
                            Interleave.newBuilder()
                                .addFilters(RowFilter.newBuilder()
                                    .setColumnQualifierRegexFilter(
                                        ByteString.copyFromUtf8("q1")))
                                .addFilters(RowFilter.newBuilder()
                                    .setColumnQualifierRegexFilter(
                                        ByteString.copyFromUtf8("q2")))))))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(Integer.MAX_VALUE))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }
}
