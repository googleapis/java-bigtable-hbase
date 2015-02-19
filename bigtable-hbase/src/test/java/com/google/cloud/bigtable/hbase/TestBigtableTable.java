package com.google.cloud.bigtable.hbase;

import com.google.bigtable.anviltop.AnviltopServiceMessages.GetRowRequest;
import com.google.bigtable.anviltop.AnviltopServiceMessages.GetRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
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
  public BigtableClient mockClient;
  public BigtableTable table;

  @Before
  public void setup() throws UnknownHostException {
    MockitoAnnotations.initMocks(this);
    BigtableOptions.Builder builder = new BigtableOptions.Builder();
    builder.setRetriesEnabled(false);
    builder.setAdminHost(InetAddress.getByName("localhost"));
    builder.setHost(InetAddress.getByName("localhost"));
    builder.setPort(0);
    builder.setProjectId(TEST_PROJECT);
    builder.setCluster(TEST_CLUSTER);
    builder.setZone(TEST_ZONE);
    builder.setCredential(null);
    BigtableOptions options = builder.build();
    table = new BigtableTable(
        TableName.valueOf(TEST_TABLE),
        options,
        new Configuration(),
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
  public void projectIsPopulatedInGetRequests() throws ServiceException, IOException {
    Mockito.when(mockClient.getRow(Mockito.any(GetRowRequest.class)))
        .thenReturn(GetRowResponse.getDefaultInstance());

    table.get(new Get(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<GetRowRequest> argument =
        ArgumentCaptor.forClass(GetRowRequest.class);
    Mockito.verify(mockClient).getRow(argument.capture());
    Assert.assertEquals(TEST_PROJECT, argument.getValue().getProjectId());
  }

  @Test
  public void tableNameIsPopulatedInGetRequests() throws ServiceException, IOException {
    Mockito.when(mockClient.getRow(Mockito.any(GetRowRequest.class)))
        .thenReturn(GetRowResponse.getDefaultInstance());

    table.get(new Get(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<GetRowRequest> argument =
        ArgumentCaptor.forClass(GetRowRequest.class);
    Mockito.verify(mockClient).getRow(argument.capture());
    Assert.assertEquals(TEST_TABLE, argument.getValue().getTableName());
  }

  @Test
  public void filterIsPopulatedInGetRequests() throws ServiceException, IOException {
    Mockito.when(mockClient.getRow(Mockito.any(GetRowRequest.class)))
        .thenReturn(GetRowResponse.getDefaultInstance());

    String expectedFilter = "((col({family:qualifier}, 1)))";
    table.get(
        new Get(Bytes.toBytes("rowKey1"))
            .addColumn(
                Bytes.toBytes("family"),
                Bytes.toBytes("qualifier")));

    ArgumentCaptor<GetRowRequest> argument =
        ArgumentCaptor.forClass(GetRowRequest.class);
    Mockito.verify(mockClient).getRow(argument.capture());
    Assert.assertEquals(expectedFilter, argument.getValue().getFilter());
  }
}
