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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

/** Unit tests for {@link AbstractBigtableTable}. */
@RunWith(JUnit4.class)
public class TestBigtableTable {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  public static final String TEST_PROJECT = "testproject";
  public static final String TEST_TABLE = "testtable";
  public static final String TEST_INSTANCE = "testinstance";
  public static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(TEST_PROJECT, TEST_INSTANCE, "");

  @Mock private AbstractBigtableConnection mockConnection;

  @Mock private BigtableSession mockSession;

  @Mock private IBigtableDataClient mockBigtableDataClient;

  @Mock private ResultScanner<FlatRow> mockResultScanner;

  public AbstractBigtableTable table;

  @Before
  public void setup() throws IOException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT);
    config.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE);
    config.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, "localhost");
    config.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, "localhost");
    config.set(BigtableOptionsFactory.BIGTABLE_PORT_KEY, "0");
    config.set(BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY, "false");
    config.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    config.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "testAgent");
    BigtableHBaseSettings settings = BigtableHBaseSettings.create(config);

    TableName tableName = TableName.valueOf(TEST_TABLE);
    HBaseRequestAdapter hbaseAdapter = new HBaseRequestAdapter(settings, tableName);
    when(mockConnection.getConfiguration()).thenReturn(config);
    when(mockConnection.getSession()).thenReturn(mockSession);
    when(mockConnection.getBigtableHBaseSettings()).thenReturn(settings);
    when(mockSession.getDataClientWrapper()).thenReturn(mockBigtableDataClient);
    when(mockBigtableDataClient.readFlatRows(isA(Query.class))).thenReturn(mockResultScanner);
    table = new AbstractBigtableTable(mockConnection, hbaseAdapter) {};
  }

  @Test
  public void projectIsPopulatedInMutationRequests() throws IOException {
    doAnswer(
            new Answer<ApiFuture<Void>>() {
              @Override
              public ApiFuture<Void> answer(InvocationOnMock invocationOnMock) {
                return ApiFutures.immediateFuture(null);
              }
            })
        .when(mockBigtableDataClient)
        .mutateRowAsync(Mockito.<RowMutation>any());
    table.delete(new Delete(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<RowMutation> argument = ArgumentCaptor.forClass(RowMutation.class);
    verify(mockBigtableDataClient).mutateRowAsync(argument.capture());

    Assert.assertEquals(
        "projects/testproject/instances/testinstance/tables/testtable",
        argument.getValue().toProto(REQUEST_CONTEXT).getTableName());
  }

  @Test
  public void getRequestsAreFullyPopulated() throws IOException {
    table.get(
        new Get(Bytes.toBytes("rowKey1"))
            .addColumn(Bytes.toBytes("family"), Bytes.toBytes("qualifier")));

    ArgumentCaptor<Query> argument = ArgumentCaptor.forClass(Query.class);

    verify(mockBigtableDataClient).readFlatRowsList(argument.capture());

    ReadRowsRequest actualRequest = argument.getValue().toProto(REQUEST_CONTEXT);

    Assert.assertEquals(
        "projects/testproject/instances/testinstance/tables/testtable",
        actualRequest.getTableName());

    Assert.assertEquals(
        FILTERS
            .chain()
            .filter(
                FILTERS
                    .chain()
                    .filter(FILTERS.family().regex("family"))
                    .filter(FILTERS.qualifier().regex("qualifier")))
            .filter(FILTERS.limit().cellsPerColumn(1))
            .toProto(),
        actualRequest.getFilter());
  }

  @Test
  public void hasWhileMatchFilter_noAtTopLevel() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    assertFalse(AbstractBigtableTable.hasWhileMatchFilter(filter));
  }

  @Test
  public void hasWhileMatchFilter_yesAtTopLevel() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    assertTrue(AbstractBigtableTable.hasWhileMatchFilter(whileMatchFilter));
  }

  @Test
  public void hasWhileMatchFilter_noInNested() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    FilterList filterList = new FilterList(filter);
    assertFalse(AbstractBigtableTable.hasWhileMatchFilter(filterList));
  }

  @Test
  public void hasWhileMatchFilter_yesInNested() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    FilterList filterList = new FilterList(whileMatchFilter);
    assertTrue(AbstractBigtableTable.hasWhileMatchFilter(filterList));
  }

  @Test
  public void getScanner_withBigtableResultScannerAdapter() throws IOException {
    when(mockBigtableDataClient.readFlatRows(isA(Query.class))).thenReturn(mockResultScanner);
    // A row with no matching label. In case of {@link BigtableResultScannerAdapter} the result is
    // non-null.
    FlatRow row =
        FlatRow.newBuilder()
            .withRowKey(ByteString.copyFromUtf8("row_key"))
            .addCell(
                "family_name",
                ByteString.copyFromUtf8("q_name"),
                0,
                ByteString.EMPTY,
                Arrays.asList("label-in"))
            .addCell(
                "family_name",
                ByteString.copyFromUtf8("q_name"),
                0,
                ByteString.copyFromUtf8("value"))
            .build();
    when(mockResultScanner.next()).thenReturn(row);

    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    Scan scan = new Scan();
    scan.setFilter(filter);
    org.apache.hadoop.hbase.client.ResultScanner resultScanner = table.getScanner(scan);
    Result result = resultScanner.next();
    assertEquals("row_key", new String(result.getRow()));
    List<org.apache.hadoop.hbase.Cell> cells =
        result.getColumnCells("family_name".getBytes(), "q_name".getBytes());
    assertEquals(1, cells.size());
    assertEquals("value", new String(CellUtil.cloneValue(cells.get(0))));

    verify(mockBigtableDataClient).readFlatRows(isA(Query.class));
    verify(mockResultScanner).next();
  }

  @Test
  public void getScanner_withBigtableWhileMatchResultScannerAdapter() throws IOException {
    // A row with no matching label. In case of {@link BigtableWhileMatchResultScannerAdapter} the
    // result is null.
    FlatRow row =
        FlatRow.newBuilder()
            .withRowKey(ByteString.copyFromUtf8("row_key"))
            .addCell("", ByteString.EMPTY, 0, ByteString.EMPTY, Arrays.asList("label-in"))
            .addCell("", ByteString.EMPTY, 0, ByteString.copyFromUtf8("value"))
            .build();
    when(mockResultScanner.next()).thenReturn(row);

    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    Scan scan = new Scan();
    scan.setFilter(whileMatchFilter);
    org.apache.hadoop.hbase.client.ResultScanner resultScanner = table.getScanner(scan);
    assertNull(resultScanner.next());

    verify(mockBigtableDataClient).readFlatRows(isA(Query.class));
    verify(mockResultScanner).next();
  }

  @Test
  public void testPut() throws IOException {
    byte[] rowKey = Bytes.toBytes("rowKey");
    doAnswer(
            new Answer<ApiFuture<Void>>() {
              @Override
              public ApiFuture<Void> answer(InvocationOnMock invocationOnMock) {
                return ApiFutures.immediateFuture(null);
              }
            })
        .when(mockBigtableDataClient)
        .mutateRowAsync(Mockito.<RowMutation>any());
    Put put =
        new Put(rowKey)
            .addColumn(Bytes.toBytes("family"), Bytes.toBytes("q"), Bytes.toBytes("value"));

    table.put(put);

    table.put(ImmutableList.of(put));
    verify(mockBigtableDataClient, times(2)).mutateRowAsync(Mockito.<RowMutation>any());
  }

  @Test
  public void testMutateRow() throws IOException {
    byte[] rowKey = Bytes.toBytes("rowKey");
    RowMutations rowMutations = new RowMutations(rowKey);
    Put put =
        new Put(rowKey)
            .addColumn(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
    rowMutations.add(put);
    doAnswer(
            new Answer<ApiFuture<Void>>() {
              @Override
              public ApiFuture<Void> answer(InvocationOnMock invocationOnMock) {
                return ApiFutures.immediateFuture(null);
              }
            })
        .when(mockBigtableDataClient)
        .mutateRowAsync(Mockito.<RowMutation>any());
    table.mutateRow(rowMutations);
    verify(mockBigtableDataClient).mutateRowAsync(Mockito.<RowMutation>any());
  }

  @Test
  public void testToString() {
    String abstractTableToStr = table.toString();
    assertThat(abstractTableToStr, containsString("project=" + TEST_PROJECT));
    assertThat(abstractTableToStr, containsString("instance=" + TEST_INSTANCE));
    assertThat(abstractTableToStr, containsString("table=" + TEST_TABLE));
    assertThat(abstractTableToStr, containsString("host=" + "localhost"));
  }
}
