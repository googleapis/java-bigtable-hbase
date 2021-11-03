package com.google.cloud.bigtable.hbase.replication;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HbaseToCloudBigtableReplicationEndpointTest {

  @Rule
  public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration CONF;
  private static Connection CONN;

  @Before
  public void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5); // less number of retries is needed
    conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    // Set CBT related configs.
    conf.set("google.bigtable.instance.id", "test-instance");
    conf.set("google.bigtable.project.id", "test-project");
    conf.set("google.bigtable.emulator.endpoint.host", "localhost:" + bigtableEmulator.getPort());

    UTIL.startMiniCluster(2);
    CONF = conf;
    CONF.setLong(RpcServer.MAX_REQUEST_SIZE, 102400);
    CONN = UTIL.getConnection();

    BigtableTableAdminClient tableAdminClient =
        BigtableTableAdminClient.create(
            BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort())
                .setProjectId("test-project")
                .setInstanceId("test-instance")
                .build());
    tableAdminClient
        .createTable(CreateTableRequest.of("replication-test").addFamily("cf1").addFamily("cf2"));

  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private String getZKClusterKey() {
    return String.format("127.0.0.1:%d:%s", UTIL.getZkCluster().getClientPort(),
        CONF.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }

  @Test
  public void testPeerCreated() throws IOException, ReplicationException {
    ReplicationAdmin admin = new ReplicationAdmin(UTIL.getConfiguration());
    System.out.println("#####" + HbaseToCloudBigtableReplicationEndpoint.class.getTypeName());
    ReplicationPeerConfig peerConfig2 = new ReplicationPeerConfig();
    peerConfig2
        .setReplicationEndpointImpl(HbaseToCloudBigtableReplicationEndpoint.class.getTypeName());
    peerConfig2.setClusterKey(UTIL.getClusterKey());
    admin.addPeer("cbt", peerConfig2);
    int port = bigtableEmulator.getPort();

    // String peerId = "region_replica_replication";

    // if (admin.getPeerConfig(peerId) != null) {
    //   admin.removePeer(peerId);
    // }

    // HTableDescriptor htd = UTIL.createTableDescriptor(
    //     "replication-pretest");
    // UTIL.getHBaseAdmin().createTable(htd);
    // ReplicationPeerConfig peerConfig = admin.getPeerConfig(peerId);
    // Assert.assertNull(peerConfig);

    HTableDescriptor htd = UTIL.createTableDescriptor("replication-test");
    // htd.setRegionReplication(2);
    HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
    htd.addFamily(cf1);
    cf1.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    HColumnDescriptor cf2 = new HColumnDescriptor("cf2");
    htd.addFamily(cf2);
    cf2.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    UTIL.getHBaseAdmin().createTable(htd);
    // admin.appendPeerTableCFs("cbt", "replication-test:cf1,cf2");

    // assert peer configuration is correct
    // peerConfig = admin.getPeerConfig(peerId);
    peerConfig2 = admin.getPeerConfig("cbt");
    System.out.println(peerConfig2.toString());
    Assert.assertNotNull(peerConfig2);
    // Assert.assertEquals(peerConfig.getClusterKey(), ZKConfig.getZooKeeperClusterKey(
    //     CONF));
    Assert.assertEquals(peerConfig2.getReplicationEndpointImpl(),
        HbaseToCloudBigtableReplicationEndpoint.class.getName());

    System.out.println("#### Peer configs: " + peerConfig2.getTableCFsMap());

    // TODO Write data to HBase table replication -test

    Table table = UTIL.getConnection().getTable(TableName.valueOf("replication-test"));
    for (int i = 0; i < 100; i++) {
      String rowKey = "test-row-" + i;
      Put put = new Put(rowKey.getBytes());
      byte[] val = new byte[1024];
      Bytes.random(val);
      put.addColumn(cf1.getName(), "col".getBytes(), 1000, val);
      table.put(put);
      System.out.println("##### Putting a new row in the source HBase table");
    }

    System.out.println("##### Triggering a new delete.");
    Delete delete = new Delete("test-row-0".getBytes());
    table.delete(delete);

    try {
      Thread.sleep(15 * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Create CBT table object.
    // TODO: Figure out why its not creating a connection. conf does not have project-id?
    // Connection connection = BigtableConfiguration.connect(CONF);
    // Table btTable = connection.getTable(TableName.valueOf("replication-test"));
    // TODO Read the data from the CBT table, it should be same as the HBase version.

    System.out.println("#####Closing admin");
    admin.close();
  }
  //
  // private void testHBaseReplicationEndpoint(String tableNameStr, String peerId, boolean isSerial)
  //     throws IOException, ReplicationException {
  //   int cellNum = 10000;
  //
  //   TableName tableName = TableName.valueOf(tableNameStr);
  //   byte[] family = Bytes.toBytes("f");
  //   byte[] qualifier = Bytes.toBytes("q");
  //   UTIL.createTable((HTableDescriptor) td, null);
  //
  //   try (Admin admin = CONN.getAdmin()) {
  //     ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
  //     peerConfig.setClusterKey(getZKClusterKey());
  //     peerConfig
  //         .setReplicationEndpointImpl(HbaseToCloudBigtableReplicationEndpoint.class.getName());
  //     peerConfig.setTableCFsMap(ImmutableMap.of(tableName, ImmutableList.of()));
  //     ReplicationAdmin replicationAdmin = new ReplicationAdmin(UTIL.getConfiguration());
  //     replicationAdmin.addPeer("testEndpoint", peerConfig);
  //   }
  //
  //   try (Table table = CONN.getTable(tableName)) {
  //     for (int i = 0; i < cellNum; i++) {
  //       Put put = new Put(Bytes.toBytes(i)).addColumn(family, qualifier,
  //           EnvironmentEdgeManager.currentTime(), Bytes.toBytes(i));
  //       table.put(put);
  //     }
  //   }
  //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);
  //
  //   int index = 0;
  //   Assert.assertEquals(TestEndpoint.getEntries().size(), cellNum);
  //   if (!isSerial) {
  //     Collections.sort(TestEndpoint.getEntries(), (a, b) -> {
  //       long seqA = a.getKey().getSequenceId();
  //       long seqB = b.getKey().getSequenceId();
  //       return seqA == seqB ? 0 : (seqA < seqB ? -1 : 1);
  //     });
  //   }
  //   for (Entry entry : TestEndpoint.getEntries()) {
  //     Assert.assertEquals(entry.getKey().getTableName(), tableName);
  //     Assert.assertEquals(entry.getEdit().getCells().size(), 1);
  //     Cell cell = entry.getEdit().getCells().get(0);
  //     Assert.assertArrayEquals(
  //         Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
  //         Bytes.toBytes(index));
  //     index++;
  //   }
  //   Assert.assertEquals(index, cellNum);
  // }
  //
  // public void testReplicate() {
  // }
}
