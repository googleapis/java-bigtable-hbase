/*
 * Copyright 2023 Google LLC
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

import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF2;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.replication.utils.TestUtils;
import com.google.cloud.bigtable.test.helper.BigtableEmulatorRule;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint.ReplicateContext;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.junit.After;
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

/**
 * Test bidirectional replication.
 * This test is separate from the other endpoint tests because it requires spinning up a
 * cluster with additional config settings.
 */
@RunWith(JUnit4.class)
public class HbaseToCloudBigtableBidirectionalReplicationEndpointTest {

  public static class TestReplicationEndpoint extends HbaseToCloudBigtableReplicationEndpoint {

    static AtomicInteger replicatedEntries = new AtomicInteger();

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      boolean result = super.replicate(replicateContext);
      replicatedEntries.getAndAdd(replicateContext.getEntries().size());
      return result;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableBidirectionalReplicationEndpointTest.class);

  private static HBaseTestingUtility hbaseTestingUtil;
  private static ReplicationAdmin replicationAdmin;

  @ClassRule
  public static final BigtableEmulatorRule bigtableEmulator = new BigtableEmulatorRule();

  private static Connection cbtConnection;
  private static Connection hbaseConnection;

  private Table hbaseTable;
  private Table cbtTable;

  private static byte[] cbtQualifier = "customCbtQualifier".getBytes();
  private static byte[] hbaseQualifier = "customHbaseQualifier".getBytes();

  @BeforeClass
  public static void setUpCluster() throws Exception {
    // Prepare HBase mini cluster configuration
    Configuration conf = new HBaseTestingUtility().getConfiguration();
    // Set CBT related configs.
    conf.set("google.bigtable.instance.id", "test-instance");
    conf.set("google.bigtable.project.id", "test-project");
    // This config will connect Replication endpoint to the emulator and not the prod CBT.
    conf.set("google.bigtable.emulator.endpoint.host", "localhost:" + bigtableEmulator.getPort());
    // Set bidirectional replication related settings
    conf.set("google.bigtable.replication.enable_bidirectional_replication", "true");
    conf.set("google.bigtable.replication.hbase_qualifier", new String(hbaseQualifier));
    conf.set("google.bigtable.replication.cbt_qualifier", new String(cbtQualifier));

    hbaseTestingUtil = new HBaseTestingUtility(conf);
    hbaseTestingUtil.startMiniCluster(2);
    replicationAdmin = new ReplicationAdmin(hbaseTestingUtil.getConfiguration());

    cbtConnection = BigtableConfiguration.connect(conf);
    hbaseConnection = hbaseTestingUtil.getConnection();

    // Setup Replication in HBase mini cluster
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    peerConfig.setReplicationEndpointImpl(
        TestReplicationEndpoint.class.getTypeName());
    // Cluster key is required, we don't really have a clusterKey for CBT.
    peerConfig.setClusterKey(hbaseTestingUtil.getClusterKey());
    replicationAdmin.addPeer("cbt", peerConfig);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cbtConnection.close();
    hbaseConnection.close();
    replicationAdmin.close();
    hbaseTestingUtil.shutdownMiniCluster();
  }

  @After
  public void tearDownTable() throws IOException {
    cbtTable.close();
    hbaseTable.close();
  }

  @Before
  public void setupTestCase() throws IOException {

    // Create and set the empty tables
    TableName table1 = TableName.valueOf(UUID.randomUUID().toString());
    createTables(table1, HConstants.REPLICATION_SCOPE_GLOBAL, HConstants.REPLICATION_SCOPE_GLOBAL);

    cbtTable = cbtConnection.getTable(table1);
    hbaseTable = hbaseConnection.getTable(table1);

    // Reset the entry counts for TestReplicationEndpoint
    TestReplicationEndpoint.replicatedEntries.set(0);
  }

  private void createTables(TableName tableName, int cf1Scope, int cf2Scope) throws IOException {
    // Create table in HBase
    HTableDescriptor htd = hbaseTestingUtil.createTableDescriptor(tableName.getNameAsString());
    HColumnDescriptor cf1 = new HColumnDescriptor(TestUtils.CF1);
    cf1.setMaxVersions(100);
    htd.addFamily(cf1);
    HColumnDescriptor cf2 = new HColumnDescriptor(CF2);
    cf2.setMaxVersions(100);
    htd.addFamily(cf2);

    // Enables replication to all peers, including CBT
    cf1.setScope(cf1Scope);
    cf2.setScope(cf2Scope);
    hbaseTestingUtil.getHBaseAdmin().createTable(htd);
    cbtConnection.getAdmin().createTable(htd);
  }
  
  /**
   * Bidirectional replication should replicate source entry and drop cbt-replicated entry to
   * prevent loops from forming.
   */
  @Test
  public void testDropsReplicatedEntry() throws IOException, InterruptedException {
    RowMutations mutationToDrop = new RowMutations(TestUtils.ROW_KEY);
    mutationToDrop.add(new Put(TestUtils.ROW_KEY).addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.VALUE));
    mutationToDrop.add(
      // Special delete mutation signifying this came from Bigtable replicator
      new Delete(TestUtils.ROW_KEY).addColumns(TestUtils.CF1, cbtQualifier, 0)
    );
    RowMutations mutationToReplicate = new RowMutations(TestUtils.ROW_KEY_2);
    mutationToReplicate.add(
        new Put(TestUtils.ROW_KEY_2).addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.VALUE)
    );

    hbaseTable.mutateRow(mutationToDrop);
    hbaseTable.mutateRow(mutationToReplicate);

    // Wait for replication
    TestUtils.waitForReplication(
        () -> {
          // Only one entry should've been replicated
          return TestReplicationEndpoint.replicatedEntries.get() >= 1;
        });

    // Hbase table should have both mutations
    Assert.assertTrue(hbaseTable.get(new Get(TestUtils.ROW_KEY)).size() == 1);
    Assert.assertTrue(hbaseTable.get(new Get(TestUtils.ROW_KEY_2)).size() == 1);
    // Cbt table should have only one mutation
    Assert.assertTrue(cbtTable.get(new Get(TestUtils.ROW_KEY)).isEmpty());
    Assert.assertTrue(cbtTable.get(new Get(TestUtils.ROW_KEY_2)).size() == 1);
  }
}
