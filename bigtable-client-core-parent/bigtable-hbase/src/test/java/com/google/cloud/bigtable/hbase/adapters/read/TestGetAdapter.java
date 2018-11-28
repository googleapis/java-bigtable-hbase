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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
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

  public static final String PREFIX_DATA = "rk1";
  public static final String FAMILY_ID = "f1";
  public static final String QUALIFIER_ID = "q1";
  private GetAdapter getAdapter =
      new GetAdapter(new ScanAdapter(FilterAdapter.buildAdapter(), new RowRangeAdapter()));
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
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    ByteString adaptedRowKey = rowRequestBuilder.getRows().getRowKeys(0);
    Assert.assertEquals(
        new String(get.getRow(), StandardCharsets.UTF_8),
        adaptedRowKey.toStringUtf8());
  }

  @Test
  public void maxVersionsIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setMaxVersions(10);
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        FILTERS.limit().cellsPerColumn(10).toProto(),
        rowRequestBuilder.getFilter());
  }

  @Test
  public void columnFamilyIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.addFamily(Bytes.toBytes(FAMILY_ID));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        FILTERS.family().exactMatch(FAMILY_ID).toProto(),
        rowRequestBuilder.getFilter());
  }

  @Test
  public void columnQualifierIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.addColumn(Bytes.toBytes(FAMILY_ID), Bytes.toBytes(QUALIFIER_ID));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        FILTERS.chain()
            .filter(FILTERS.family().regex(FAMILY_ID))
            .filter(FILTERS.qualifier().regex(QUALIFIER_ID))
            .toProto(),
        rowRequestBuilder.getFilter());
  }

  @Test
  public void multipleQualifiersAreSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.addColumn(Bytes.toBytes(FAMILY_ID), Bytes.toBytes(QUALIFIER_ID));
    get.addColumn(Bytes.toBytes(FAMILY_ID), Bytes.toBytes("q2"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        FILTERS.chain()
            .filter(FILTERS.family().regex(FAMILY_ID))
            .filter(FILTERS.interleave()
                .filter(FILTERS.qualifier().regex(QUALIFIER_ID))
                .filter(FILTERS.qualifier().regex("q2")))
            .toProto(),
        rowRequestBuilder.getFilter());
  }

  @Test
  public void testCheckExistenceOnlyWhenFalse() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    Assert.assertTrue(getAdapter.setCheckExistenceOnly(get).isCheckExistenceOnly());
  }

  @Test
  public void testCheckExistenceOnlyWhenTrue() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setCheckExistenceOnly(true);
    Assert.assertEquals(get, getAdapter.setCheckExistenceOnly(get));
  }

  @Test
  public void testAddKeyOnlyFilterWithoutFilter() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setCheckExistenceOnly(true);
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Assert.assertEquals(
        FILTERS.chain().filter(FILTERS.limit().cellsPerColumn(1))
            .filter(FILTERS.value().strip())
            .toProto(),
        rowRequestBuilder.getFilter()
    );
  }

  @Test
  public void testAdKeyOnlyFilterWithFilter() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setCheckExistenceOnly(true);
    get.setFilter(new KeyOnlyFilter());
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get, throwingReadHooks);
    Filters.Filter filterOne = FILTERS.chain()
        .filter(FILTERS.limit().cellsPerColumn(1))
        .filter(FILTERS.value().strip());
    Filters.Filter filterTwo = FILTERS.chain()
        .filter(FILTERS.limit().cellsPerColumn(1))
        .filter(FILTERS.value().strip());
    Assert.assertEquals(
        FILTERS.chain().filter(filterOne).filter(filterTwo).toProto(),
        rowRequestBuilder.getFilter());
  }
}
