/*
 * Copyright 2015 Google LLC
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

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for the {@link GetAdapter} */
@RunWith(JUnit4.class)
public class TestGetAdapter {

  public static final String PREFIX_DATA = "rk1";
  public static final String FAMILY_ID = "f1";
  public static final String QUALIFIER_ID = "q1";

  private final RequestContext requestContext =
      RequestContext.create("ProjectId", "InstanceId", "AppProfile");
  private final Query query = Query.create("tableId");
  private GetAdapter getAdapter =
      new GetAdapter(new ScanAdapter(FilterAdapter.buildAdapter(), new RowRangeAdapter()));
  private DataGenerationHelper dataHelper = new DataGenerationHelper();
  private ReadHooks throwingReadHooks =
      new ReadHooks() {
        @Override
        public void composePreSendHook(Function<Query, Query> newHook) {
          throw new IllegalStateException("Read hooks not supported in tests.");
        }

        @Override
        public void applyPreSendHook(Query query) {
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
    getAdapter.adapt(get, throwingReadHooks, query);

    ByteString adaptedRowKey = query.toProto(requestContext).getRows().getRowKeys(0);
    Assert.assertEquals(
        new String(get.getRow(), StandardCharsets.UTF_8), adaptedRowKey.toStringUtf8());
  }

  @Test
  public void maxVersionsIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setMaxVersions(10);
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS.limit().cellsPerColumn(10).toProto(), query.toProto(requestContext).getFilter());
  }

  @Test
  public void columnFamilyIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.addFamily(Bytes.toBytes(FAMILY_ID));
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS.family().exactMatch(FAMILY_ID).toProto(),
        query.toProto(requestContext).getFilter());
  }

  @Test
  public void columnQualifierIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.addColumn(Bytes.toBytes(FAMILY_ID), Bytes.toBytes(QUALIFIER_ID));
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS
            .chain()
            .filter(FILTERS.family().regex(FAMILY_ID))
            .filter(FILTERS.qualifier().regex(QUALIFIER_ID))
            .toProto(),
        query.toProto(requestContext).getFilter());
  }

  @Test
  public void multipleQualifiersAreSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.addColumn(Bytes.toBytes(FAMILY_ID), Bytes.toBytes(QUALIFIER_ID));
    get.addColumn(Bytes.toBytes(FAMILY_ID), Bytes.toBytes("q2"));
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS
            .chain()
            .filter(FILTERS.family().regex(FAMILY_ID))
            .filter(
                FILTERS
                    .interleave()
                    .filter(FILTERS.qualifier().regex(QUALIFIER_ID))
                    .filter(FILTERS.qualifier().regex("q2")))
            .toProto(),
        query.toProto(requestContext).getFilter());
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
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS
            .chain()
            .filter(FILTERS.limit().cellsPerColumn(1))
            .filter(FILTERS.value().strip())
            .toProto(),
        query.toProto(requestContext).getFilter());
  }

  @Test
  public void testAdKeyOnlyFilterWithFilter() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setCheckExistenceOnly(true);
    get.setFilter(new KeyOnlyFilter());
    getAdapter.adapt(get, throwingReadHooks, query);
    Filters.Filter filterOne =
        FILTERS.chain().filter(FILTERS.limit().cellsPerColumn(1)).filter(FILTERS.value().strip());
    Filters.Filter filterTwo =
        FILTERS.chain().filter(FILTERS.limit().cellsPerColumn(1)).filter(FILTERS.value().strip());
    Assert.assertEquals(
        FILTERS.chain().filter(filterOne).filter(filterTwo).toProto(),
        query.toProto(requestContext).getFilter());
  }

  @Test
  public void testBuildFilter() throws IOException {
    Get get = makeValidGet(dataHelper.randomData(PREFIX_DATA));
    get.setFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("a")));
    get.setMaxVersions(5);

    Filters.Filter actualFilter = getAdapter.buildFilter(get);

    Assert.assertEquals(
        FILTERS
            .chain()
            .filter(FILTERS.limit().cellsPerColumn(5))
            .filter(FILTERS.family().regex("a"))
            .toProto(),
        actualFilter.toProto());
  }
}
