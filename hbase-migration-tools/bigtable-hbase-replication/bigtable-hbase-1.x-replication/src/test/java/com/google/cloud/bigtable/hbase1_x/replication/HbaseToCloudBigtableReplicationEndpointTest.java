/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.bigtable.hbase1_x.replication;

import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.replication.utils.TestUtils;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HbaseToCloudBigtableReplicationEndpointTest {

  public static class TestReplicationEndpoint extends BaseReplicationEndpoint {

    static AtomicInteger replicatedEntries = new AtomicInteger();
    static HbaseToCloudBigtableReplicationEndpoint delegate;

    public TestReplicationEndpoint() {
      delegate = new HbaseToCloudBigtableReplicationEndpoint();
    }

    @Override
    protected void doStart() {
      try {
        delegate.start().get();
      } catch (Exception e) {
        new RuntimeException("Failed to start Replication Endpoint.", e);
      }
      notifyStarted();
    }

    @Override
    protected void doStop() {
      try {
        delegate.stop().get();
      } catch (Exception e) {
        new RuntimeException("Failed to stop Replication Endpoint.", e);
      }
      notifyStopped();
    }

    @Override
    public UUID getPeerUUID() {
      return delegate.getPeerUUID();
    }

    @Override
    public void init(Context ctx) throws IOException {
      super.init(ctx);
      delegate.init(ctx);
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      boolean result = delegate.replicate(replicateContext);
      replicatedEntries.getAndAdd(replicateContext.getEntries().size());
      return result;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpointTest.class);

  private static HBaseTestingUtility hbaseTestingUtil = new HBaseTestingUtility();
  private static Configuration hbaseConfig;
  private static ReplicationAdmin replicationAdmin;

  @ClassRule
  public static final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

  private static Connection cbtConnection;
  private static Connection hbaseConnection;

  private Table hbaseTable;
  private Table hbaseTable2;
  private Table cbtTable;
  private Table cbtTable2;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    // Prepare HBase mini cluster configuration
    Configuration conf = hbaseTestingUtil.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5); // less number of retries is needed

    // Set CBT related configs.
    conf.set("google.bigtable.instance.id", "test-instance");
    conf.set("google.bigtable.project.id", "test-project");
    // This config will connect Replication endpoint to the emulator and not the prod CBT.
    conf.set("google.bigtable.emulator.endpoint.host", "localhost:" + bigtableEmulator.getPort());

    hbaseTestingUtil.startMiniCluster(2);
    hbaseConfig = conf;
    hbaseConfig.setLong(RpcServer.MAX_REQUEST_SIZE, 102400);
    replicationAdmin = new ReplicationAdmin(hbaseTestingUtil.getConfiguration());

    cbtConnection = BigtableConfiguration.connect(conf);
    hbaseConnection = hbaseTestingUtil.getConnection();

    // Setup Replication in HBase mini cluster
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    peerConfig.setReplicationEndpointImpl(TestReplicationEndpoint.class.getTypeName());
    // Cluster key is required, we don't really have a clusterKey for CBT.
    peerConfig.setClusterKey(hbaseTestingUtil.getClusterKey());
    replicationAdmin.addPeer("cbt", peerConfig);

    LOG.info("#################### SETUP COMPLETE ##############################");
  }

  @Before
  public void setupTestCase() throws IOException {

    // Create and set the empty tables
    TableName table1 = TableName.valueOf(UUID.randomUUID().toString());
    TableName table2 = TableName.valueOf(UUID.randomUUID().toString());
    createTables(table1);
    createTables(table2);

    cbtTable = cbtConnection.getTable(table1);
    cbtTable2 = cbtConnection.getTable(table2);
    hbaseTable = hbaseConnection.getTable(table1);
    hbaseTable2 = hbaseConnection.getTable(table2);

    // Reset the entry counts for TestReplicationEndpoint
    TestReplicationEndpoint.replicatedEntries.set(0);
  }

  private void createTables(TableName tableName) throws IOException {
    // Create table in HBase
    HTableDescriptor htd = hbaseTestingUtil.createTableDescriptor(tableName.getNameAsString());
    HColumnDescriptor cf1 = new HColumnDescriptor(TestUtils.CF1);
    cf1.setMaxVersions(100);
    htd.addFamily(cf1);
    HColumnDescriptor cf2 = new HColumnDescriptor(TestUtils.CF2);
    cf2.setMaxVersions(100);
    htd.addFamily(cf2);

    // Enables replication to all peers, including CBT
    cf1.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    cf2.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    hbaseTestingUtil.getHBaseAdmin().createTable(htd);

    cbtConnection.getAdmin().createTable(htd);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cbtConnection.close();
    hbaseConnection.close();
    replicationAdmin.close();
    hbaseTestingUtil.shutdownMiniCluster();
  }

  @Test
  public void testPeerCreated() throws IOException, ReplicationException {
    // assert peer configuration is correct
    ReplicationPeerConfig peerConfig = replicationAdmin.getPeerConfig("cbt");
    Assert.assertNotNull(peerConfig);
    Assert.assertEquals(
        peerConfig.getReplicationEndpointImpl(), TestReplicationEndpoint.class.getName());
  }

  @Test
  public void testMutationReplication() throws IOException, InterruptedException {
    // Add 10K rows with 1 cell/family
    for (int i = 0; i < 10000; i++) {
      Put put = new Put(TestUtils.getRowKey(i));
      put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(i));
      put.addColumn(TestUtils.CF2, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(i));
      hbaseTable.put(put);
    }

    // Validate that both the databases have same data
    TestUtils.assertTableEventuallyEquals(
        hbaseTable,
        cbtTable,
        () -> {
          // 10K Puts.
          return TestReplicationEndpoint.replicatedEntries.get() >= 10000;
        });
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
    delete.addColumns(
        TestUtils.CF1, TestUtils.COL_QUALIFIER, 20); // Delete first 2 cells and leave the last
    delete.addColumns(TestUtils.CF1, TestUtils.COL_QUALIFIER_2); // Delete all cells from col2
    hbaseTable.delete(delete);

    delete = new Delete(TestUtils.getRowKey(2));
    // Delete a family
    delete.addFamily(TestUtils.CF1);
    hbaseTable.delete(delete);

    // Delete a row
    delete = new Delete(TestUtils.getRowKey(3));
    hbaseTable.delete(delete);

    // Validate that both the databases have same data
    TestUtils.assertTableEventuallyEquals(
        hbaseTable,
        cbtTable,
        () -> {
          /* 4 put and 4 delete*/
          return TestReplicationEndpoint.replicatedEntries.get() >= 8;
        });
  }

  @Test
  public void testIncrements() throws IOException, InterruptedException {
    Put put = new Put(TestUtils.ROW_KEY);
    byte[] val = Bytes.toBytes(4l);
    put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, val);
    hbaseTable.put(put);

    // Now Increment the value
    Increment increment = new Increment(TestUtils.ROW_KEY);
    increment.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 10l);
    hbaseTable.increment(increment);

    // Validate that both the databases have same data
    TestUtils.assertTableEventuallyEquals(
        hbaseTable,
        cbtTable,
        () -> {
          /* 1 put and 1 increment*/
          return TestReplicationEndpoint.replicatedEntries.get() >= 2;
        });
  }

  @Test
  public void testAppends() throws IOException, InterruptedException {
    Put put = new Put(TestUtils.ROW_KEY);
    byte[] val = "aaaa".getBytes();
    put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, val);
    hbaseTable.put(put);

    // Now append the value
    Append append = new Append(TestUtils.ROW_KEY);
    append.add(TestUtils.CF1, TestUtils.COL_QUALIFIER, "bbbb".getBytes());
    hbaseTable.append(append);

    // Validate that both the databases have same data
    TestUtils.assertTableEventuallyEquals(
        hbaseTable,
        cbtTable,
        () -> {
          /* 1 put and 1 append*/
          return TestReplicationEndpoint.replicatedEntries.get() >= 2;
        });
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

    // Validate that both the databases have same data
    TestUtils.assertTableEventuallyEquals(
        hbaseTable,
        cbtTable,
        () -> {
          /* 16 puts total*/
          return TestReplicationEndpoint.replicatedEntries.get() >= 16;
        });
    TestUtils.assertTableEventuallyEquals(
        hbaseTable,
        cbtTable,
        () -> {
          return TestReplicationEndpoint.replicatedEntries.get() >= 16;
        });
  }

  @Test
  public void testWriteFailureToBigtableDoesNotStallReplication()
      throws IOException, InterruptedException {
    Put put = new Put(TestUtils.ROW_KEY);
    put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.getValue(0));
    hbaseTable.put(put);

    // Trigger delete that will never succeed.
    Delete delete = new Delete(TestUtils.ROW_KEY);
    delete.addFamily(TestUtils.CF1, 20);
    hbaseTable.delete(delete);

    // Let replication process 2 mutations, 1 of them will never succeed. TestReplicationEndpoint
    // counts the incompatible mutations
    TestUtils.waitForReplication(
        () -> {
          return TestReplicationEndpoint.replicatedEntries.get() >= 2;
        });

    // Add another put to validate that an incompatible delete does not stall replication.
    // This put will only succeed on CBT if incompatible mutation is dropped. If this put is
    // bundled with earlier put/delete, 3 of them will be part of same RowMutations and will fail.
    // If this put and  earlier put/delete are part of separate replicate calls, the new put
    // will never be passed to ReplicationEndpoint unless incompatible delete is dropped.
    put = new Put(TestUtils.ROW_KEY);
    put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 1, TestUtils.getValue(1));
    hbaseTable.put(put);

    TestUtils.waitForReplication(
        () -> {
          // 1put + 1 delete from previous call and 1 new put
          return TestReplicationEndpoint.replicatedEntries.get() >= 3;
        });

    List<Cell> actualCells = cbtTable.get(new Get(TestUtils.ROW_KEY).setMaxVersions()).listCells();
    Assert.assertEquals(
        "Number of cells mismatched, actual cells: " + actualCells, 2, actualCells.size());

    TestUtils.assertEquals(
        "Qualifiers mismatch",
        TestUtils.COL_QUALIFIER,
        CellUtil.cloneQualifier(actualCells.get(1)));
    TestUtils.assertEquals(
        "Value mismatch", TestUtils.getValue(0), CellUtil.cloneValue(actualCells.get(1)));
    Assert.assertEquals(0, actualCells.get(1).getTimestamp());
  }

  @Test
  public void testHBaseCBTTimestampTruncation() throws IOException, InterruptedException {
    Put put = new Put(TestUtils.ROW_KEY);
    byte[] val = Bytes.toBytes(1);
    put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, Long.MAX_VALUE - 1, val);
    hbaseTable.put(put);

    TestUtils.waitForReplication(
        () -> {
          // 1put
          return TestReplicationEndpoint.replicatedEntries.get() >= 1;
        });

    List<Cell> expectedCells =
        hbaseTable.get(new Get(TestUtils.ROW_KEY).setMaxVersions()).listCells();
    List<Cell> actualCells = cbtTable.get(new Get(TestUtils.ROW_KEY).setMaxVersions()).listCells();

    for (int i = 0; i < expectedCells.size(); i++) {
      Assert.assertNotEquals(
          "Timestamp mismatch for row " + TestUtils.ROW_KEY,
          expectedCells.get(i).getTimestamp(),
          actualCells.get(i).getTimestamp());
    }
  }
}
