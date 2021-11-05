package com.google.cloud.bigtable.hbase.replication;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HbaseToCloudBigtableReplicationEndpointTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);

  public static final String ROW_KEY_PREFIX = "test-row-";
  public static final byte[] CF1 = "cf1".getBytes();
  public static final String TABLE_NAME = "replication-test";
  public static final byte[] COL_QUALIFIER = "col".getBytes();
  public static final String VALUE_PREFIX = "Value-";

  private HBaseTestingUtility hbaseTestingUtil = new HBaseTestingUtility();
  private Configuration hbaseConfig;
  private ReplicationAdmin replicationAdmin;

  @Rule
  public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();
  private Connection cbtConnection;

  @Before
  public void setUp() throws Exception {
    // Prepare HBase mini cluster configuration
    Configuration conf = hbaseTestingUtil.getConfiguration();
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
    // This config will connect Replication endpoint to the emulator and not the prod CBT.
    conf.set("google.bigtable.emulator.endpoint.host", "localhost:" + bigtableEmulator.getPort());

    hbaseTestingUtil.startMiniCluster(2);
    hbaseConfig = conf;
    hbaseConfig.setLong(RpcServer.MAX_REQUEST_SIZE, 102400);
    replicationAdmin = new ReplicationAdmin(hbaseTestingUtil.getConfiguration());

    // Create table in HBase
    HTableDescriptor htd = hbaseTestingUtil.createTableDescriptor("replication-test");
    HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
    htd.addFamily(cf1);
    HColumnDescriptor cf2 = new HColumnDescriptor("cf2");
    htd.addFamily(cf2);

    // Enables replication to all peers, including CBT
    cf1.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    cf2.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    hbaseTestingUtil.getHBaseAdmin().createTable(htd);

    // Setup Bigtable Emulator
    // Create the copy of HBase table in CBT. Replication will copy data into this "copy" CBT table.
    BigtableTableAdminClient tableAdminClient =
        BigtableTableAdminClient.create(
            BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort())
                .setProjectId("test-project")
                .setInstanceId("test-instance")
                .build());
    tableAdminClient
        .createTable(CreateTableRequest.of("replication-test").addFamily("cf1").addFamily("cf2"));
    cbtConnection = BigtableConfiguration.connect(conf);

    // Setup Replication in HBase mini cluster
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    peerConfig
        .setReplicationEndpointImpl(HbaseToCloudBigtableReplicationEndpoint.class.getTypeName());
    // Cluster key is required, we don't really have a clusterKey for CBT.
    peerConfig.setClusterKey(hbaseTestingUtil.getClusterKey());
    replicationAdmin.addPeer("cbt", peerConfig);

    LOG.error("#################### SETUP COMPLETE ##############################");
  }


  @After
  public void tearDown() throws Exception {
    replicationAdmin.close();
    hbaseTestingUtil.shutdownMiniCluster();
  }

  @Test
  public void testPeerCreated() throws IOException, ReplicationException {
    // assert peer configuration is correct
    ReplicationPeerConfig peerConfig = replicationAdmin.getPeerConfig("cbt");
    Assert.assertNotNull(peerConfig);
    Assert.assertEquals(peerConfig.getReplicationEndpointImpl(),
        HbaseToCloudBigtableReplicationEndpoint.class.getName());

  }

  @Test
  public void testMutationReplication() throws IOException, InterruptedException {
    Table table = hbaseTestingUtil.getConnection().getTable(TableName.valueOf(TABLE_NAME));
    for (int i = 0; i < 10; i++) {
      String rowKey = ROW_KEY_PREFIX + i;
      Put put = new Put(rowKey.getBytes());
      byte[] val = (VALUE_PREFIX + i).getBytes();
      put.addColumn(CF1, COL_QUALIFIER, 0, val);
      table.put(put);
    }

    // Wait for replication to catch up
    // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini cluster
    Thread.sleep(2000);
    //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

    Table cbtTable = cbtConnection.getTable(TableName.valueOf(TABLE_NAME));
    int i =0;
    for(Result result: cbtTable.getScanner(new Scan())){
      LOG.error("Processing result: " + Bytes.toStringBinary(result.getRow()) + " cell: " + result.listCells().get(0).toString());
      List<Cell> returnedCells = result.listCells();
      Assert.assertEquals(0, Bytes.compareTo(result.getRow(), (ROW_KEY_PREFIX + i).getBytes()));
      // Only 1 cell/row added in  HBase
      Assert.assertEquals(1, returnedCells.size());
      // Assert on cell contents
      Assert.assertEquals(0, Bytes.compareTo(
          CellUtil.cloneFamily(returnedCells.get(0)), CF1));
      Assert.assertEquals(0, Bytes.compareTo(CellUtil.cloneQualifier(returnedCells.get(0)), COL_QUALIFIER));
      Assert.assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(returnedCells.get(0)), (VALUE_PREFIX + i).getBytes()));
      i++;
    }
    Assert.assertEquals(10, i);

  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    Table table = hbaseTestingUtil.getConnection().getTable(TableName.valueOf(TABLE_NAME));
    for (int i = 0; i < 5; i++) {
      String rowKey = ROW_KEY_PREFIX + i;
      Put put = new Put(rowKey.getBytes());
      byte[] val = (VALUE_PREFIX + i).getBytes();
      put.addColumn(CF1, COL_QUALIFIER, 0, val);
      table.put(put);
    }

    // Now delete some data.
    Delete delete = new Delete("test-row-0".getBytes());
    delete.addColumn(CF1, COL_QUALIFIER, 0);
    table.delete(delete);

    // Wait for replication to catch up
    // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini cluster
    Thread.sleep(2000);
    //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

    Table cbtTable = cbtConnection.getTable(TableName.valueOf(TABLE_NAME));

    Get get = new Get("test-row-0".getBytes()).addColumn(CF1, COL_QUALIFIER);
    Result res = cbtTable.get(get);
    Assert.assertNull(res.listCells());


    int i =1; // Row 0 has been deleted.
    for(Result result: cbtTable.getScanner(new Scan())){
      LOG.error("Processing result: " + Bytes.toStringBinary(result.getRow()) + " cell: " + result.listCells().get(0).toString());
      List<Cell> returnedCells = result.listCells();
      Assert.assertEquals(0, Bytes.compareTo(result.getRow(), (ROW_KEY_PREFIX + i).getBytes()));
      // Only 1 cell/row added in  HBase
      Assert.assertEquals(1, returnedCells.size());
      // Assert on cell contents
      Assert.assertEquals(0, Bytes.compareTo(
          CellUtil.cloneFamily(returnedCells.get(0)), CF1));
      Assert.assertEquals(0, Bytes.compareTo(CellUtil.cloneQualifier(returnedCells.get(0)), COL_QUALIFIER));
      Assert.assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(returnedCells.get(0)), (VALUE_PREFIX + i).getBytes()));
      i++;
    }
    // processed rows 1-4, end state will be 5
    Assert.assertEquals(5, i);
  }

  //TODO create a multi column family and multi table tests.

}
