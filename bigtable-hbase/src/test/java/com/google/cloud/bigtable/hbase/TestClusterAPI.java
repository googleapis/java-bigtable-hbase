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

import io.grpc.Status;
import io.grpc.Status.OperationRuntimeException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.bigtable.admin.cluster.v1.Cluster;
import com.google.bigtable.admin.cluster.v1.CreateClusterRequest;
import com.google.bigtable.admin.cluster.v1.DeleteClusterRequest;
import com.google.bigtable.admin.cluster.v1.GetClusterRequest;
import com.google.bigtable.admin.cluster.v1.ListClustersRequest;
import com.google.bigtable.admin.cluster.v1.ListZonesRequest;
import com.google.bigtable.admin.cluster.v1.ListZonesResponse;
import com.google.bigtable.admin.cluster.v1.Zone;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableClusterAdminClient;
import com.google.cloud.bigtable.grpc.BigtableClusterAdminGrpcClient;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;

/**
 * Tests the Cluster API.
 */
public class TestClusterAPI {

  private static final int MAX_WAIT_SECONDS = 20;
  private static final String TEST_CLUSTER_ID = "test-cluster-api";
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");

  @Test
  public void setup() throws IOException {
    String shouldTest = System.getProperty("bigtable.test.cluster.api");
    if (!"true".equals(shouldTest)) {
      return;
    }

    Configuration config = HBaseConfiguration.create();

    String extraResources = System.getProperty("bigtable.test.extra.resources");
    if (extraResources == null) {
      Assert.fail("Please set bigtable.test.extra.resources");
    }

    InputStream resourceStream =
        TestClusterAPI.class.getClassLoader().getResourceAsStream(extraResources);
    if (resourceStream == null) {
      Assert.fail(extraResources + " does not exist");
    }
    config.addResource(resourceStream);

    BigtableOptions bigtableOptions = BigtableOptionsFactory.fromConfiguration(config);
    BigtableClusterAdminClient client = createClusterAdminStub(bigtableOptions);

    String projectId = bigtableOptions.getProjectId();
    List<Cluster> clusters = getClusters(client, projectId);

    // cleanup any old clusters
    for (Cluster cluster : clusters) {
      if (cluster.getName().contains(TEST_CLUSTER_ID)) {
        dropCluster(client, cluster.getName());
      }
    }

    List<Zone> zoneList = getZones(client, projectId);
    String fullyQualifiedZoneName = selectZone(zoneList);
    String clusterId = fullyQualifiedZoneName + "/clusters/" + TEST_CLUSTER_ID;

    Cluster cluster = createACluster(client, fullyQualifiedZoneName, TEST_CLUSTER_ID);
    waitForOperation(client, cluster.getCurrentOperation().getName(), MAX_WAIT_SECONDS);

    Configuration newConfig = newConfiguration(config, clusterId);
    TableName autoDeletedTableName =
        TableName.valueOf("auto-deleted-" + UUID.randomUUID().toString());
    try (Connection connection = new TestBigtableConnection(newConfig);
        Admin admin = connection.getAdmin()) {
      countTables(admin, 0);
      createTable(admin, autoDeletedTableName);
      countTables(admin, 1);
      TableName tableToDelete = TableName.valueOf("test_table-" + UUID.randomUUID().toString());
      createTable(admin, tableToDelete);
      countTables(admin, 2);
      try (Table t = connection.getTable(tableToDelete)) {
        doPutGetDelete(t);
      }
      dropTable(connection, tableToDelete);
      countTables(admin, 1);
    } finally {
      dropCluster(client, clusterId);
    }
  }

  private void countTables(Admin admin, int expectedCount) throws IOException {
    TableName[] tables = admin.listTableNames();
    int actualCount = tables.length;
    Assert.assertEquals(String.format("Got %d tables, expected %d.  The tables: %s", actualCount,
      expectedCount, Arrays.asList(tables)), expectedCount, actualCount);
  }

  private Cluster getCluster(BigtableClusterAdminClient client, String clusterName) {
    GetClusterRequest request = GetClusterRequest.newBuilder().setName(clusterName).build();
    try {
      Cluster response = client.getCluster(request);
      return response;
    } catch (UncheckedExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof OperationRuntimeException) {
        Status status = ((OperationRuntimeException) e.getCause()).getStatus();
        if (status.getCode() == Status.NOT_FOUND.getCode()) {
          return null;
        }
      }
      e.printStackTrace();
      throw e;
    }
  }

  private void waitForOperation(BigtableClusterAdminClient client, String operationName,
      int maxSeconds) {
    GetOperationRequest request = GetOperationRequest.newBuilder().setName(operationName).build();
    for (int i = 0; i < maxSeconds; i++) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Operation response = client.getOperation(request);
      if (response.getError() != null) {
        return;
      }
    }
    throw new IllegalStateException(String.format(
      "Waited %d seconds and operation was not complete", maxSeconds));
  }

  private BigtableClusterAdminClient createClusterAdminStub(BigtableOptions bigtableOptions)
      throws IOException {
    return BigtableClusterAdminGrpcClient.createClient(
      bigtableOptions.getClusterAdminTransportOptions(), bigtableOptions.getChannelOptions(),
      Executors.newFixedThreadPool(10));
  }

  private List<Zone> getZones(BigtableClusterAdminClient client, String projectId) {
    ListZonesResponse zones =
        client.listZones(ListZonesRequest.newBuilder().setName("projects/" + projectId).build());
    List<Zone> zoneList = zones.getZonesList();
    Assert.assertTrue("Zones must exist", !zoneList.isEmpty());
    return zoneList;
  }

  private String selectZone(List<Zone> zoneList) {
    int zoneNumber = (int) (zoneList.size() * Math.random());
    return zoneList.get(zoneNumber).getName().replaceFirst("^/", "");
  }

  private Cluster createACluster(BigtableClusterAdminClient client, String zoneName,
      String clusterId) {
    Cluster cluster = Cluster.newBuilder()
        .setDisplayName(clusterId)
        .setServeNodes(3)
        .build();
    CreateClusterRequest request = CreateClusterRequest.newBuilder()
        .setName(zoneName)
        .setClusterId(clusterId)
        .setCluster(cluster)
        .build();
    return client.createCluster(request);
  }

  private List<Cluster> getClusters(BigtableClusterAdminClient client, String projectId) {
    ListClustersRequest request =
        ListClustersRequest.newBuilder().setName("projects/" + projectId).build();
    return client.listClusters(request).getClustersList();
  }

  private Configuration newConfiguration(Configuration base, String fullyQualifiedClusterId) {
    Configuration newConfig = new Configuration(base);
    String zone = fullyQualifiedClusterId.replaceFirst(".*/zones/([^/]+)/.*", "$1");
    String cluster = fullyQualifiedClusterId.replaceFirst(".*/clusters/([^/]+)", "$1");

    newConfig.set(BigtableOptionsFactory.ZONE_KEY, zone);
    newConfig.set(BigtableOptionsFactory.CLUSTER_KEY, cluster);
    return newConfig;
  }

  private void createTable(Admin admin, TableName tableName) throws IOException {
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(descriptor);
    Assert.assertTrue("Table does not exist", admin.tableExists(tableName));
  }

  DataGenerationHelper dataHelper = new DataGenerationHelper();

  private void doPutGetDelete(Table table) throws IOException {
    testIncrement(dataHelper, table);
    testCheckAndMutate(dataHelper, table);
  }

  private void testIncrement(DataGenerationHelper dataHelper, Table table)
      throws IOException {
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual1 = dataHelper.randomData("qual-");
    long value1 = new Random().nextInt();
    long incr1 = new Random().nextInt();
    byte[] qual2 = dataHelper.randomData("qual-");
    long value2 = new Random().nextInt();
    long incr2 = new Random().nextInt();

    // Put and increment
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, qual1, Bytes.toBytes(value1));
    put.addColumn(COLUMN_FAMILY, qual2, Bytes.toBytes(value2));
    table.put(put);
    Increment increment = new Increment(rowKey);
    increment.addColumn(COLUMN_FAMILY, qual1, incr1);
    increment.addColumn(COLUMN_FAMILY, qual2, incr2);
    Result result = table.increment(increment);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));

    // Double-check values with a Get
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    result = table.get(get);
    Assert.assertEquals("Expected four results, two for each column", 4, result.size());
    Assert.assertEquals("Value1=" + value1 + " & Incr1=" + incr1, value1 + incr1,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual1))));
    Assert.assertEquals("Value2=" + value2 + " & Incr2=" + incr2, value2 + incr2,
      Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, qual2))));
  }

  private void testCheckAndMutate(DataGenerationHelper dataHelper, Table table) throws IOException {
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put with a bad check on a null value, then try with a good one
    Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value1);
    boolean success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, value2, put);
    Assert.assertFalse("Column doesn't exist.  Should fail.", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, null, put);
    Assert.assertTrue(success);

    // Fail on null check, now there's a value there
    put = new Put(rowKey).addColumn(COLUMN_FAMILY, qual, value2);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, null, put);
    Assert.assertFalse("Null check should fail", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, value2, put);
    Assert.assertFalse("Wrong value should fail", success);
    success = table.checkAndPut(rowKey, COLUMN_FAMILY, qual, value1, put);
    Assert.assertTrue(success);

    // Check results
    Get get = new Get(rowKey);
    get.setMaxVersions(5);
    Result result = table.get(get);
    Assert.assertEquals("Should be two results", 2, result.size());
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, qual);
    Assert.assertArrayEquals(value2, CellUtil.cloneValue(cells.get(0)));
    Assert.assertArrayEquals(value1, CellUtil.cloneValue(cells.get(1)));
  }
  private void dropTable(Connection connection, TableName tableName) throws IOException {
    try (Admin admin = connection.getAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      Assert.assertFalse(admin.tableExists(tableName));
    }
  }

  private void dropCluster(BigtableClusterAdminClient client, String fullyQualifiedClusterId) {
    DeleteClusterRequest request =
        DeleteClusterRequest.newBuilder().setName(fullyQualifiedClusterId).build();
    client.deleteCluster(request);
    Assert.assertNull(getCluster(client, fullyQualifiedClusterId));
  }

}
