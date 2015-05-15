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
package com.google.cloud.bigtable.hbase;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.cloud.bigtable.grpc.BigtableClient;
import com.google.cloud.bigtable.grpc.ResultScanner;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

/**
 * Unit tests for
 */
@RunWith(JUnit4.class)
public class TestBigtableTable {

  public static final String TEST_PROJECT = "testproject";
  public static final String TEST_TABLE = "testtable";
  public static final String TEST_CLUSTER = "testcluster";
  public static final String TEST_ZONE = "testzone";

  @Mock
  public AbstractBigtableConnection mockConnection;
  @Mock
  public BigtableClient mockClient;

  public BigtableTable table;

  @Before
  public void setup() throws UnknownHostException {
    MockitoAnnotations.initMocks(this);
    BigtableOptions.Builder builder = new BigtableOptions.Builder();
    builder.setRetriesEnabled(false);
    builder.setClusterAdminHost(InetAddress.getByName("localhost"));
    builder.setTableAdminHost(InetAddress.getByName("localhost"));
    builder.setDataHost(InetAddress.getByName("localhost"));
    builder.setPort(0);
    builder.setProjectId(TEST_PROJECT);
    builder.setCluster(TEST_CLUSTER);
    builder.setZone(TEST_ZONE);
    builder.setCredential(null);
    BigtableOptions options = builder.build();
    Configuration config = new Configuration();
    Mockito.when(mockConnection.getConfiguration()).thenReturn(config);
    table = new BigtableTable(
        mockConnection,
        TableName.valueOf(TEST_TABLE),
        options,
        mockClient,
        Executors.newCachedThreadPool());
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
  public void getRequestsAreFullyPopulated() throws ServiceException, IOException {
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
}
