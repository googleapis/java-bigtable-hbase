package com.google.cloud.bigtable.hbase;

import com.google.bigtable.admin.cluster.v1.Cluster;
import com.google.bigtable.admin.cluster.v1.CreateClusterRequest;
import com.google.bigtable.admin.cluster.v1.DeleteClusterRequest;
import com.google.bigtable.admin.cluster.v1.GetClusterRequest;
import com.google.bigtable.admin.cluster.v1.ListClustersRequest;
import com.google.bigtable.admin.cluster.v1.ListZonesRequest;
import com.google.bigtable.admin.cluster.v1.ListZonesResponse;
import com.google.bigtable.admin.cluster.v1.Zone;
import com.google.cloud.bigtable.hbase.adapters.ClusterMetadataSetter;
import com.google.cloud.hadoop.hbase.BigtableClusterAdminClient;
import com.google.cloud.hadoop.hbase.BigtableClusterAdminGrpcClient;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BigtableConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.Status.OperationRuntimeException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Tests the Cluster API.
 */
public class TestClusterAPI {

  private static final int MAX_WAIT_SECONDS = 20;
  private static final String TEST_CLUSTER_ID = "test-cluster-api";

  @Test
  public void setup() throws IOException {
    if (!IntegrationTests.isBigtable()) {
      return;
    }
    Configuration config = IntegrationTests.getConfiguration();

    BigtableOptions bigtableOptions = BigtableOptionsFactory.fromConfiguration(config);
    BigtableClusterAdminClient client = createClusterAdminStub(bigtableOptions);

    String projectId = bigtableOptions.getProjectId();
    List<Cluster> clusters = getClusters(client, projectId);

    // cleanup any old clusters
    boolean createCluster = true;
    for (Cluster cluster : clusters) {
      if (cluster.getName().contains(TEST_CLUSTER_ID)) {
        dropCluster(client, cluster.getName());
        // createCluster = false;
      }
    }

    List<Zone> zoneList = getZones(client, projectId);
    String fullyQualifiedZoneName = selectZone(zoneList);
    String clusterId =
        fullyQualifiedZoneName + ClusterMetadataSetter.BIGTABLE_VA_CLUSTER_SEPARATOR
            + TEST_CLUSTER_ID;

    if (createCluster) {
      Cluster cluster = createACluster(client, fullyQualifiedZoneName, TEST_CLUSTER_ID);
      waitForOperation(client, cluster.getCurrentOperation().getName(), MAX_WAIT_SECONDS);
    }

    Configuration newConfig = newConfiguration(config, clusterId);
    TableName autoDeletedTableName =
        TableName.valueOf("auto-deleted-" + UUID.randomUUID().toString());
    try (Connection connection = new BigtableConnection(newConfig);
        Admin admin = connection.getAdmin()) {
      countTables(admin, 0);
      createTable(admin, autoDeletedTableName);
      countTables(admin, 1);
      TableName tableToDelete = IntegrationTests.newTestTableName();
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
        client.listZones(ListZonesRequest.newBuilder().setName("/projects/" + projectId).build());
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
    CreateClusterRequest request =
        CreateClusterRequest.newBuilder().setName(zoneName).setClusterId(clusterId)
            .setCluster(Cluster.newBuilder().setServeNodes(1).build()).build();
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
    String cluster =
        fullyQualifiedClusterId.replaceFirst(".*"
            + ClusterMetadataSetter.BIGTABLE_VA_CLUSTER_SEPARATOR + "([^/]+)", "$1");

    newConfig.set(BigtableOptionsFactory.ZONE_KEY, zone);
    newConfig.set(BigtableOptionsFactory.CLUSTER_KEY, cluster);
    return newConfig;
  }

  private void createTable(Admin admin, TableName tableName) throws IOException {
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(IntegrationTests.COLUMN_FAMILY));
    admin.createTable(descriptor);
    Assert.assertTrue("Table does not exist", admin.tableExists(tableName));
  }

  DataGenerationHelper dataHelper = new DataGenerationHelper();

  private void doPutGetDelete(Table t) throws IOException {
    TestIncrement.testIncrement(dataHelper, t);
    TestCheckAndMutate.testCheckAndMutate(dataHelper, t);
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
