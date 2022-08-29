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

package com.google.cloud.bigtable.hbase2_x.replication;

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.ENABLE_DRY_RUN_MODE_KEY;
import static org.junit.Assert.assertFalse;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.replication.utils.TestUtils;
import com.google.cloud.bigtable.test.helper.VeneerEmulatorRule;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HbaseToCloudBigtableReplicationEndpointDryRunTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpointDryRunTest.class);

  private static HBaseTestingUtility hbaseTestingUtil = new HBaseTestingUtility();
  private static ReplicationAdmin replicationAdmin;

  @ClassRule public static final VeneerEmulatorRule bigtableEmulator = new VeneerEmulatorRule();

  private static Connection cbtConnection;
  private static Connection hbaseConnection;

  private Table hbaseTable;
  private Table cbtTable;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    // Prepare HBase mini cluster configuration
    Configuration conf = hbaseTestingUtil.getConfiguration();

    // Set CBT related configs.
    conf.set("google.bigtable.instance.id", "test-instance");
    conf.set("google.bigtable.project.id", "test-project");
    // This config will connect Replication endpoint to the emulator and not the prod CBT.
    conf.set("google.bigtable.emulator.endpoint.host", "localhost:" + bigtableEmulator.getPort());
    conf.setBoolean(ENABLE_DRY_RUN_MODE_KEY, true);

    hbaseTestingUtil.startMiniCluster(2);
    replicationAdmin = new ReplicationAdmin(hbaseTestingUtil.getConfiguration());

    cbtConnection = BigtableConfiguration.connect(conf);
    hbaseConnection = hbaseTestingUtil.getConnection();

    // Setup Replication in HBase mini cluster
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    peerConfig.setReplicationEndpointImpl(
        HbaseToCloudBigtableReplicationEndpoint.class.getTypeName());
    // Cluster key is required, we don't really have a clusterKey for CBT.
    peerConfig.setClusterKey(hbaseTestingUtil.getClusterKey());
    replicationAdmin.addPeer("cbt", peerConfig);

    LOG.info("#################### SETUP COMPLETE ##############################");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cbtConnection.close();
    hbaseConnection.close();
    replicationAdmin.close();
    hbaseTestingUtil.shutdownMiniCluster();
  }

  @Before
  public void setupTestCase() throws IOException {

    // Create and set the empty tables
    TableName table1 = TableName.valueOf(UUID.randomUUID().toString());
    createTables(table1);

    cbtTable = cbtConnection.getTable(table1);
    hbaseTable = hbaseConnection.getTable(table1);
  }

  private void createTables(TableName tableName) throws IOException {
    // Create table in HBase
    HTableDescriptor htd = hbaseTestingUtil.createTableDescriptor(tableName.getNameAsString());
    HColumnDescriptor cf1 = new HColumnDescriptor(TestUtils.CF1);
    htd.addFamily(cf1);

    // Enables replication to all peers, including CBT
    cf1.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    hbaseTestingUtil.getHBaseAdmin().createTable(htd);

    cbtConnection.getAdmin().createTable(htd);
  }

  @Test
  public void testDryRunDoesNotReplicateToCloudBigtable()
      throws IOException, InterruptedException, ReplicationException {
    Put put = new Put(TestUtils.ROW_KEY);
    put.addColumn(TestUtils.CF1, TestUtils.COL_QUALIFIER, 0, TestUtils.VALUE);
    hbaseTable.put(put);

    // Give enough time for replication to catch up. Nothing should be replicated as its dry-run
    Thread.sleep(5000);

    ResultScanner cbtScanner = cbtTable.getScanner(new Scan());
    assertFalse(cbtScanner.iterator().hasNext());
  }
}
