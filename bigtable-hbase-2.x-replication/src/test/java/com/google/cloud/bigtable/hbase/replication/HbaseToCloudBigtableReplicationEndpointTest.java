package com.google.cloud.bigtable.hbase.replication;

import com.google.cloud.bigtable.hbase.replication.utils.TestUtils;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
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

// TODO(add serial replication tests)
@RunWith(JUnit4.class)
public class HbaseToCloudBigtableReplicationEndpointTest {

    private static final Logger LOG =
        LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);

    private static HBaseTestingUtility hbaseTestingUtil = new HBaseTestingUtility();
    private Configuration hbaseConfig;

    @Rule public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();
    private Connection cbtConnection;

    private Table hbaseTable;
    private Table hbaseTable2;
    private Table cbtTable;
    private Table cbtTable2;

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
        LOG.info("heta ahahah 81448945585492");

        hbaseTestingUtil.startMiniCluster(2);

        hbaseConfig = conf;
        hbaseConfig.setLong(RpcServer.MAX_REQUEST_SIZE, 102400);

        cbtConnection = BigtableConfiguration.connect(conf);
        // Setup Replication in HBase mini cluster
        ReplicationPeerConfig peerConfig =
            ReplicationPeerConfig.newBuilder()
                .setClusterKey(hbaseTestingUtil.getClusterKey())
                    .setReplicateAllUserTables(true)
                        .setReplicationEndpointImpl(HbaseToCloudBigtableReplicationEndpoint.class.getTypeName())
                            .build();

        hbaseTestingUtil.getAdmin().addReplicationPeer("cbt", peerConfig, true);

        setupTables(TestUtils.TABLE_NAME);
        setupTables(TestUtils.TABLE_NAME_2);

        cbtTable = cbtConnection.getTable(TestUtils.TABLE_NAME);
        cbtTable2 = cbtConnection.getTable(TestUtils.TABLE_NAME_2);
        hbaseTable = hbaseTestingUtil.getConnection().getTable(TestUtils.TABLE_NAME);
        hbaseTable2 = hbaseTestingUtil.getConnection().getTable(TestUtils.TABLE_NAME_2);

        LOG.error("#################### SETUP COMPLETE ##############################");
    }

    private void setupTables(TableName tableName) throws IOException {
        // Create table in HBase
        ColumnFamilyDescriptor cf1 =
            ColumnFamilyDescriptorBuilder.newBuilder(TestUtils.CF1)
                .setMaxVersions(100)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL)
                .build();

        ColumnFamilyDescriptor cf2 =
            ColumnFamilyDescriptorBuilder.newBuilder(TestUtils.CF2)
                .setMaxVersions(100)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL)
                .build();
        TableDescriptor htd =
            TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cf1)
                .setColumnFamily(cf1)
                .build();

        hbaseTestingUtil.getAdmin().createTable(htd);
        cbtConnection.getAdmin().createTable(htd);
    }

    @After
    public void tearDown() throws Exception {
        hbaseTestingUtil.shutdownMiniCluster();
    }

    @Test
    public void testPeerCreated() throws IOException, ReplicationException {
        // assert peer configuration is correct
        ReplicationPeerConfig peerConfig =  hbaseTestingUtil.getAdmin().getReplicationPeerConfig("cbt");
        Assert.assertNotNull(peerConfig);
        Assert.assertEquals(
            peerConfig.getReplicationEndpointImpl(),
            HbaseToCloudBigtableReplicationEndpoint.class.getName());
    }

    @Test
    public void testMutationReplication() throws IOException, InterruptedException {
        Table table = hbaseTestingUtil.getConnection().getTable(TestUtils.TABLE_NAME);
        // Add 10 rows with 1 cell/family
        for (int i = 0; i < 10000; i++) {
            Put put = new Put(TestUtils.getRowKey(i));
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(i));
            put.addColumn(TestUtils.CF2, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(i));
            table.put(put);
        }

        // Wait for replication to catch up
        // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini
        // cluster
        Thread.sleep(2000);
        //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

        // Validate that both the databases have same data
        TestUtils.assertTableEquals(hbaseTable, cbtTable);
    }

    @Test
    public void testDelete() throws IOException, InterruptedException {

        // Add 4 rows with many cells/column
        for (int i = 0; i < 4; i++) {
            Put put = new Put(TestUtils.getRowKey(i));
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(10 + i));
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 1, TestUtils.getValue(20 + i));
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 2, TestUtils.getValue(30 + i));
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER_2, 3, TestUtils.getValue(40 + i));
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER_2, 4, TestUtils.getValue(50 + i));
            put.addColumn(TestUtils.CF2, TestUtils.COL_QUALIFIER, 5, TestUtils.getValue(60 + i));
            hbaseTable.put(put);
        }

        // Now delete some cells with all supported delete types from CF1. CF2 should exist to validate
        // we don't delete anything else
        Delete delete = new Delete(TestUtils.getRowKey(0));
        // Delete individual cell
        delete.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0);
        // Delete latest cell
        delete.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER);
        // Delete non existent cells
        delete.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 100);
        hbaseTable.delete(delete);

        delete = new Delete(TestUtils.getRowKey(1));
        // Delete columns. Deletes all cells from a column
        delete.addColumns(TestUtils.CF1, TestUtils.COL_QUALIFIER, 20); // Delete first 2 cells and leave the last
        delete.addColumns(TestUtils.CF1, TestUtils.COL_QUALIFIER_2); // Delete all cells from col2
        hbaseTable.delete(delete);

        delete = new Delete(TestUtils.getRowKey(2));
        // Delete a family
        delete.addFamily(TestUtils.CF1);
        hbaseTable.delete(delete);

        // Delete a row
        delete = new Delete(TestUtils.getRowKey(3));
        hbaseTable.delete(delete);

        // Wait for replication to catch up
        // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini
        // cluster
        //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);
        Thread.sleep(2000);

        // Validate that both the databases have same data
        TestUtils.assertTableEquals(hbaseTable, cbtTable);
    }

    @Test
    public void testIncrements() throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        Table table = hbaseTestingUtil.getConnection().getTable(TestUtils.TABLE_NAME);
        Put put = new Put(TestUtils.ROW_KEY);
        byte[] val = Bytes.toBytes(4l);
        put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, val);
        table.put(put);

        // Now Increment the value
        Increment increment = new Increment("test-row-0".getBytes());
        increment.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 10l);
        table.increment(increment);

        // Wait for replication to catch up
        // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini
        // cluster
        Thread.sleep(2000);
        //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

        // Validate that both the databases have same data
        TestUtils.assertTableEquals(hbaseTable, cbtTable);
    }

    @Test
    public void testAppends() throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        Table table = hbaseTestingUtil.getConnection().getTable(TestUtils.TABLE_NAME);
        Put put = new Put(TestUtils.ROW_KEY);
        byte[] val = "aaaa".getBytes();
        put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, val);
        table.put(put);

        // Now append the value
        Append append = new Append(TestUtils.ROW_KEY);
        append.add(TestUtils.CF1, TestUtils.COL_QUALIFIER, "bbbb".getBytes());
        table.append(append);

        // Wait for replication to catch up
        // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini
        // cluster
        Thread.sleep(2000);
        //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

        // Validate that both the databases have same data
        TestUtils.assertTableEquals(hbaseTable, cbtTable);
    }

    @Test
    public void testMultiTableMultiColumnFamilyReplication()
        throws IOException, InterruptedException {

        for (int i = 0; i < 8; i++) {
            // Add a put to table 1
            // rowkey 10-19 for table1, 20-29 for table 2
            byte[] rowKey = TestUtils.getRowKey(10 + i);
            Put put = new Put(rowKey);
            // Value 100s place for table, 10s place for CF, 1s place for index
            byte[] val11 = TestUtils.getValue(100 + 10 + i);
            put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, val11);
            byte[] val12 = TestUtils.getValue(100 + 20 + i);
            put.addColumn(TestUtils.CF2, TestUtils.COL_QUALIFIER, 0, val12);
            hbaseTable.put(put);

            // Add a put to table 2
            byte[] rowKey2 = TestUtils.getRowKey(20 + i);
            Put put2 = new Put(rowKey2);
            byte[] val21 = TestUtils.getValue(200 + 10 + i);
            put2.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, val21);
            byte[] val22 = TestUtils.getValue(200 + 20 + i);
            put.addColumn(TestUtils.CF2, TestUtils.COL_QUALIFIER, 0, val22);
            hbaseTable2.put(put2);
        }

        // Wait for replication to catch up
        // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini
        // cluster
        Thread.sleep(2000);
        //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

        TestUtils.assertTableEquals(hbaseTable, cbtTable);
        TestUtils.assertTableEquals(hbaseTable2, cbtTable2);
    }

    @Test
    public void testWriteFailureToBigtableDoesNotStallReplication()
        throws IOException, InterruptedException {
        Put put = new Put(TestUtils.ROW_KEY);
        put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(0));
        hbaseTable.put(put);

        // Trigger a delete that will never succeed.
        Delete delete = new Delete(TestUtils.ROW_KEY);
        delete.addFamily(TestUtils.CF1, 20);
        hbaseTable.delete(delete);

        // Add another put to validate that an incompatible delete does not stall replication
        put = new Put(TestUtils.ROW_KEY);
        put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 1, TestUtils.getValue(1));
        hbaseTable.put(put);

        // Wait for replication to catch up
        // TODO Find a better alternative than sleeping? Maybe disable replication or turnoff mini
        // cluster
        Thread.sleep(2000);
        //   Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

        List<Cell> actualCells = cbtTable.get(new Get(TestUtils.ROW_KEY).readAllVersions()).listCells();
        Assert.assertEquals(
            "Number of cells mismatched, actual cells: " + actualCells, 2, actualCells.size());

        TestUtils.assertEquals(
            "Qualifiers mismatch", TestUtils.COL_QUALIFIER, CellUtil.cloneQualifier(actualCells.get(1)));
        TestUtils.assertEquals("Value mismatch", TestUtils.getValue(0), CellUtil.cloneValue(actualCells.get(1)));
        Assert.assertEquals(0, actualCells.get(1).getTimestamp());
    }
}
