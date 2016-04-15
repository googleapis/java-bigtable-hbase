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

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link BigtableTable}.
 */
@RunWith(JUnit4.class)
public class TestBigtableTable {

  public static final String TEST_PROJECT = "testproject";
  public static final String TEST_TABLE = "testtable";
  public static final String TEST_CLUSTER = "testcluster";
  public static final String TEST_ZONE = "testzone";

  @Mock
  private AbstractBigtableConnection mockConnection;

  @Mock
  private BigtableSession mockSession;

  @Mock
  private BigtableDataClient mockClient;

  @Mock
  private BatchExecutor mockBatchExecutor;

  @Mock
  private ResultScanner<Row> mockResultScanner;

  public BigtableTable table;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    BigtableOptions options = new BigtableOptions.Builder()
        .setClusterAdminHost("localhost")
        .setTableAdminHost("localhost")
        .setDataHost("localhost")
        .setPort(0)
        .setProjectId(TEST_PROJECT)
        .setClusterId(TEST_CLUSTER)
        .setZoneId(TEST_ZONE)
        .setRetryOptions(new RetryOptions.Builder().setEnableRetries(false).build())
        .setCredentialOptions(null)
        .setUserAgent("testAgent")
        .build();

    Configuration config = new Configuration(false);
    TableName tableName = TableName.valueOf(TEST_TABLE);
    HBaseRequestAdapter hbaseAdapter =
        new HBaseRequestAdapter(options, tableName, config);
    Mockito.when(mockConnection.getConfiguration()).thenReturn(config);
    Mockito.when(mockConnection.getSession()).thenReturn(mockSession);
    Mockito.when(mockSession.getOptions()).thenReturn(options);
    Mockito.when(mockSession.getDataClient()).thenReturn(mockClient);
    table =
        new BigtableTable(mockConnection, tableName, hbaseAdapter, mockBatchExecutor);
  }

  @Test
  public void projectIsPopulatedInMutationRequests() throws ServiceException, IOException {
    table.delete(new Delete(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<MutateRowRequest> argument =
        ArgumentCaptor.forClass(MutateRowRequest.class);
    Mockito.verify(mockClient).mutateRow(argument.capture());

    Assert.assertEquals(
        "projects/testproject/zones/testzone/clusters/testcluster/tables/testtable",
        argument.getValue().getTableName());
  }

  @Test
  public void getRequestsAreFullyPopulated() throws IOException {
    Mockito.when(mockClient.readRows(Mockito.any(ReadRowsRequest.class)))
        .thenReturn(new ResultScanner<Row>() {
          @Override
          public Row next() throws IOException {
            return null;
          }

          @Override
          public Row[] next(int i) throws IOException {
            return new Row[i];
          }

          @Override
          public int available() {
            return 0;
          }

          @Override
          public void close() throws IOException {
          }
        });

    table.get(
        new Get(Bytes.toBytes("rowKey1"))
            .addColumn(
                Bytes.toBytes("family"),
                Bytes.toBytes("qualifier")));

    ArgumentCaptor<ReadRowsRequest> argument =
        ArgumentCaptor.forClass(ReadRowsRequest.class);

    Mockito.verify(mockClient).readRows(argument.capture());

    Assert.assertEquals(
        "projects/testproject/zones/testzone/clusters/testcluster/tables/testtable",
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
    assertFalse(BigtableTable.hasWhileMatchFilter(filter));
  }
  
  @Test
  public void hasWhileMatchFilter_yesAtTopLevel() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    assertTrue(BigtableTable.hasWhileMatchFilter(whileMatchFilter));
  }
  
  @Test
  public void hasWhileMatchFilter_noInNested() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    FilterList filterList = new FilterList(filter);
    assertFalse(BigtableTable.hasWhileMatchFilter(filterList));
  }

  @Test
  public void hasWhileMatchFilter_yesInNested() {
    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    FilterList filterList = new FilterList(whileMatchFilter);
    assertTrue(BigtableTable.hasWhileMatchFilter(filterList));
  }
  
  @Test
  public void getScanner_withBigtableResultScannerAdapter() throws IOException {
    when(mockClient.readRows(isA(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    // A row with no matching label. In case of {@link BigtableResultScannerAdapter} the result is
    // non-null.
    Row row = Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("row_key"))
        .addFamilies(Family.newBuilder().setName("family_name").addColumns(
            Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("q_name"))
                .addCells(Cell.newBuilder().addLabels("label-in"))
                .addCells(Cell.newBuilder().setValue(ByteString.copyFromUtf8("value")))))
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
    
    verify(mockClient).readRows(isA(ReadRowsRequest.class));
    verify(mockResultScanner).next();
  }

  @Test
  public void getScanner_withBigtableWhileMatchResultScannerAdapter() throws IOException {
    when(mockClient.readRows(isA(ReadRowsRequest.class))).thenReturn(mockResultScanner);
    // A row with no matching label. In case of {@link BigtableWhileMatchResultScannerAdapter} the
    // result is null.
    Row row = Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("row_key"))
        .addFamilies(Family.newBuilder().addColumns(
            Column.newBuilder()
                .addCells(Cell.newBuilder().addLabels("label-in"))
                .addCells(Cell.newBuilder().setValue(ByteString.copyFromUtf8("value")))))
        .build();
    when(mockResultScanner.next()).thenReturn(row);

    QualifierFilter filter =
        new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("x")));
    WhileMatchFilter whileMatchFilter = new WhileMatchFilter(filter);
    Scan scan = new Scan();
    scan.setFilter(whileMatchFilter);
    org.apache.hadoop.hbase.client.ResultScanner resultScanner = table.getScanner(scan);
    assertNull(resultScanner.next());

    verify(mockClient).readRows(isA(ReadRowsRequest.class));
    verify(mockResultScanner).next();
  }
}
