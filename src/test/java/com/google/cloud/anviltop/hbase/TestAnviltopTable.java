package com.google.cloud.anviltop.hbase;

import com.google.bigtable.anviltop.AnviltopServices;
import com.google.cloud.hadoop.hbase.AnviltopBlockingClient;
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

/**
 * Unit tests for
 */
@RunWith(JUnit4.class)
public class TestAnviltopTable {

  public static final String TEST_PROJECT = "testproject";
  public static final String TEST_TABLE = "testtable";

  @Mock
  public AnviltopBlockingClient mockClient;
  public AnvilTopTable table;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    AnviltopOptions options = new AnviltopOptions("testhost", 0, null, TEST_PROJECT);
    table = new AnvilTopTable(
        TableName.valueOf(TEST_TABLE), options, new Configuration(), mockClient);
  }

  @Test
  public void projectIsPopulatedInMutationRequests() throws ServiceException, IOException {
    table.delete(new Delete(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<AnviltopServices.MutateRowRequest> argument =
        ArgumentCaptor.forClass(AnviltopServices.MutateRowRequest.class);
    Mockito.verify(mockClient).mutateAtomic(argument.capture());
    Assert.assertEquals(TEST_PROJECT, argument.getValue().getProjectId());
  }

  @Test
  public void tableNameIsPopulatedInMutationRequests() throws ServiceException, IOException {
    table.delete(new Delete(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<AnviltopServices.MutateRowRequest> argument =
        ArgumentCaptor.forClass(AnviltopServices.MutateRowRequest.class);
    Mockito.verify(mockClient).mutateAtomic(argument.capture());
    Assert.assertEquals(TEST_TABLE, argument.getValue().getTableName());
  }

  @Test
  public void projectIsPopulatedInGetRequests() throws ServiceException, IOException {
    Mockito.when(mockClient.getRow(Mockito.any(AnviltopServices.GetRowRequest.class)))
        .thenReturn(AnviltopServices.GetRowResponse.getDefaultInstance());

    table.get(new Get(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<AnviltopServices.GetRowRequest> argument =
        ArgumentCaptor.forClass(AnviltopServices.GetRowRequest.class);
    Mockito.verify(mockClient).getRow(argument.capture());
    Assert.assertEquals(TEST_PROJECT, argument.getValue().getProjectId());
  }

  @Test
  public void tableNameIsPopulatedInGetRequests() throws ServiceException, IOException {
    Mockito.when(mockClient.getRow(Mockito.any(AnviltopServices.GetRowRequest.class)))
        .thenReturn(AnviltopServices.GetRowResponse.getDefaultInstance());

    table.get(new Get(Bytes.toBytes("rowKey1")));

    ArgumentCaptor<AnviltopServices.GetRowRequest> argument =
        ArgumentCaptor.forClass(AnviltopServices.GetRowRequest.class);
    Mockito.verify(mockClient).getRow(argument.capture());
    Assert.assertEquals(TEST_TABLE, argument.getValue().getTableName());
  }

  @Test
  public void filterIsPopulatedInGetRequests() throws ServiceException, IOException {
    Mockito.when(mockClient.getRow(Mockito.any(AnviltopServices.GetRowRequest.class)))
        .thenReturn(AnviltopServices.GetRowResponse.getDefaultInstance());

    String expectedFilter = "(col(family:qualifier, 1))";
    table.get(
        new Get(Bytes.toBytes("rowKey1"))
            .addColumn(
                Bytes.toBytes("family"),
                Bytes.toBytes("qualifier")));

    ArgumentCaptor<AnviltopServices.GetRowRequest> argument =
        ArgumentCaptor.forClass(AnviltopServices.GetRowRequest.class);
    Mockito.verify(mockClient).getRow(argument.capture());
    Assert.assertEquals(expectedFilter, argument.getValue().getFilter());
  }
}
