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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link AbstractBigtableTable}.
 */
@RunWith(JUnit4.class)
public class TestBigtableTable {

  public static final String TEST_PROJECT = "testproject";
  public static final String TEST_TABLE = "testtable";
  public static final String TEST_INSTANCE = "testinstance";

  @Mock
  private AbstractBigtableConnection mockConnection;

  @Mock
  private BigtableSession mockSession;

  @Mock
  private BigtableDataClient mockClient;

  @Mock
  private ResultScanner<FlatRow> mockResultScanner;

  public AbstractBigtableTable table;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    BigtableOptions options = new BigtableOptions.Builder()
        .setInstanceAdminHost("localhost")
        .setTableAdminHost("localhost")
        .setDataHost("localhost")
        .setPort(0)
        .setProjectId(TEST_PROJECT)
        .setInstanceId(TEST_INSTANCE)
        .setRetryOptions(new RetryOptions.Builder().setEnableRetries(false).build())
        .setCredentialOptions(null)
        .setUserAgent("testAgent")
        .build();

    Configuration config = new Configuration(false);
    TableName tableName = TableName.valueOf(TEST_TABLE);
    HBaseRequestAdapter hbaseAdapter =
        new HBaseRequestAdapter(options, tableName, config);
    when(mockConnection.getConfiguration()).thenReturn(config);
    when(mockConnection.getSession()).thenReturn(mockSession);
    when(mockSession.getOptions()).thenReturn(options);
    when(mockSession.getDataClient()).thenReturn(mockClient);
    when(mockClient.readFlatRows(isA(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    table = new AbstractBigtableTable(mockConnection, hbaseAdapter);
  }

  @Test
  public void projectIsPopulatedInMutationRequests() throws IOException {
    table.delete(new Delete(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<MutateRowRequest> argument =
        ArgumentCaptor.forClass(MutateRowRequest.class);
    verify(mockClient).mutateRow(argument.capture());

    Assert.assertEquals(
        "projects/testproject/instances/testinstance/tables/testtable",
        argument.getValue().getTableName());
  }

  @Test
  public void getRequestsAreFullyPopulated() throws IOException {
    table.get(
        new Get(Bytes.toBytes("rowKey1"))
            .addColumn(
                Bytes.toBytes("family"),
                Bytes.toBytes("qualifier")));

    ArgumentCaptor<ReadRowsRequest> argument =
        ArgumentCaptor.forClass(ReadRowsRequest.class);

    verify(mockClient).readFlatRowsList(argument.capture());

    Assert.assertEquals(
        "projects/testproject/instances/testinstance/tables/testtable",
        argument.getValue().getTableName());
    Chain expectedColumnSpecFilter =
        Chain.newBuilder()
          .addFilters(
              RowFilter.newBuilder()
                  .setChain(
                      Chain.newBuilder()
                          .addFilters(
                              RowFilter.newBuilder()
                                  .setFamilyNameRegexFilter("family"))
                          .addFilters(
                              RowFilter.newBuilder()
                                  .setColumnQualifierRegexFilter(
                                      ByteString.copyFromUtf8("qualifier")))))
            .addFilters(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1))
        .build();

    Assert.assertEquals(
        expectedColumnSpecFilter,
        argument.getValue().getFilter().getChain());
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
    when(mockClient.readFlatRows(isA(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    // A row with no matching label. In case of {@link BigtableResultScannerAdapter} the result is
    // non-null.
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("row_key"))
        .addCell("family_name", ByteString.copyFromUtf8("q_name"), 0, ByteString.EMPTY,
          Arrays.asList("label-in"))
        .addCell("family_name", ByteString.copyFromUtf8("q_name"), 0,
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
    
    verify(mockClient).readFlatRows(isA(ReadRowsRequest.class));
    verify(mockResultScanner).next();
  }

  @Test
  public void getScanner_withBigtableWhileMatchResultScannerAdapter() throws IOException {
    // A row with no matching label. In case of {@link BigtableWhileMatchResultScannerAdapter} the
    // result is null.
    FlatRow row = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("row_key"))
        .addCell("", ByteString.EMPTY, 0, ByteString.EMPTY, Arrays.asList("label-in"))
        .addCell("", ByteString.EMPTY, 0, ByteString.copyFromUtf8("value")).build();
    when(mockResultScanner.next()).thenReturn(row);

    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    Scan scan = new Scan();
    scan.setFilter(whileMatchFilter);
    org.apache.hadoop.hbase.client.ResultScanner resultScanner = table.getScanner(scan);
    assertNull(resultScanner.next());

    verify(mockClient).readFlatRows(isA(ReadRowsRequest.class));
    verify(mockResultScanner).next();
  }
}
