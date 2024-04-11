/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mapreduce.hbasesnapshots;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.test.helper.BigtableEmulatorRule;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** end to end integration test * */
// TODO - parameterize this to run against prod in future
public class EndToEndIT {

  private static final Log LOG = LogFactory.getLog(EndToEndIT.class);

  private static final HBaseTestingUtility HB_TEST_UTIL = new HBaseTestingUtility();

  @Rule public final BigtableEmulatorRule bigtableEmulator = new BigtableEmulatorRule();

  // Clients that will be connected to the emulator
  private Connection bigtableConn;

  private static final long TEST_TS = System.currentTimeMillis();

  // bigtable
  private static final String BT_TEST_PROJECT = "testproject-" + TEST_TS;
  private static final String BT_TEST_INSTANCE = "testinstance-" + TEST_TS;
  private static final TableName BT_TEST_TABLE = TableName.valueOf("testtable-" + TEST_TS);

  // hbase
  private static final byte[] HB_TEST_SNAPSHOTNAME = Bytes.toBytes("snaptb0-" + TEST_TS);
  private TableName HB_TEST_TABLE = TableName.valueOf("testtb-" + TEST_TS);
  private static final byte[] TEST_CF = Bytes.toBytes("cf");

  public static void setupBaseConf(Configuration conf) {
    // hbase snapshot config
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    conf.setInt("mapreduce.map.maxattempts", 10);
    conf.setInt(
        "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", 99);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBaseConf(HB_TEST_UTIL.getConfiguration());
    HB_TEST_UTIL.startMiniCluster(1);
    HB_TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HB_TEST_UTIL.shutdownMiniMapReduceCluster();
    HB_TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    // bigtable config setup, create table
    bigtableSetup();

    // hbase setup, create table
    hbaseSetup();
  }

  @After
  public void tearDown() throws Exception {
    HB_TEST_UTIL.deleteTable(HB_TEST_TABLE);
    SnapshotTestingUtils.deleteAllSnapshots(HB_TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(HB_TEST_UTIL);
  }

  /** Set configs, create conn to bigtable emulator, and setup table for tests. */
  private void bigtableSetup() throws IOException {
    Configuration conf = new Configuration(false);
    BigtableConfiguration.configure(conf, BT_TEST_PROJECT, BT_TEST_INSTANCE);
    conf.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
        "localhost:" + bigtableEmulator.getPort());
    bigtableConn = BigtableConfiguration.connect(conf);

    Admin admin = bigtableConn.getAdmin();
    HTableDescriptor htd = new HTableDescriptor(BT_TEST_TABLE);
    HColumnDescriptor columnDesc = new HColumnDescriptor(TEST_CF);
    columnDesc.setMaxVersions(Integer.MAX_VALUE - 1);
    htd.addFamily(columnDesc);
    admin.createTable(htd);

    LOG.info("bigtable table created: " + htd.toString());
  }

  /** Create a table. */
  private void hbaseSetup() throws IOException {
    // create Table
    Admin admin = HB_TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(HB_TEST_TABLE);
    HColumnDescriptor columnDesc = new HColumnDescriptor(TEST_CF);
    columnDesc.setMaxVersions(Integer.MAX_VALUE);
    htd.addFamily(columnDesc);
    admin.createTable(htd);

    LOG.info("hbase table created: " + htd.toString());
  }

  @Test
  public void testRunImport() throws Exception {
    // test data for import
    final List<Put> testData =
        Arrays.asList(
            new Put(Bytes.toBytes("row_key_1"))
                .addColumn(TEST_CF, "col1".getBytes(), 1L, "v1".getBytes())
                .addColumn(TEST_CF, "col1".getBytes(), 2L, "v2".getBytes()),
            new Put(Bytes.toBytes("row_key_2"))
                .addColumn(TEST_CF, "col2".getBytes(), 1L, "v3".getBytes())
                .addColumn(TEST_CF, "col2".getBytes(), 3L, "v4".getBytes()));

    final Set<Cell> flattenedTestData = Sets.newHashSet();
    for (Put put : testData) {
      for (List<Cell> cells : put.getFamilyCellMap().values()) {
        flattenedTestData.addAll(cells);
      }
    }

    // create test data in hbase & take a snapshot
    BufferedMutator mutator = HB_TEST_UTIL.getConnection().getBufferedMutator(HB_TEST_TABLE);
    mutator.mutate(testData);
    mutator.flush();
    HB_TEST_UTIL.getHBaseAdmin().snapshot(HB_TEST_SNAPSHOTNAME, HB_TEST_TABLE);
    LOG.debug(
        "hbase snapshot created: "
            + Bytes.toString(HB_TEST_SNAPSHOTNAME)
            + ", from: "
            + HB_TEST_TABLE.getNameAsString());

    // import snapshot job, create clean conf for mr job
    Configuration mrJobConf = new Configuration(false);
    mrJobConf.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
        "localhost:" + bigtableEmulator.getPort());
    mrJobConf.setInt(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, 1);
    mrJobConf.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    mrJobConf.set(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "false");

    Path rootDir = HB_TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    String[] args =
        new String[] {
          "-D" + BigtableOptionsFactory.PROJECT_ID_KEY + "=" + BT_TEST_PROJECT,
          "-D" + BigtableOptionsFactory.INSTANCE_ID_KEY + "=" + BT_TEST_INSTANCE,
          Bytes.toString(HB_TEST_SNAPSHOTNAME),
          rootDir.toString(),
          BT_TEST_TABLE.getNameAsString(),
          "table1-restore"
        };

    // run import
    ImportHBaseSnapshotJob.innerMain(mrJobConf, args);

    // exact match verification
    Set<Cell> destTableData = readAllCellsFromTable(bigtableConn, BT_TEST_TABLE);
    Assert.assertEquals(flattenedTestData, destTableData);
  }

  /**
   * Returns the content of the table as a {@link Set} of {@link Cell}s. This is only suitable for
   * small tables.
   */
  private Set<Cell> readAllCellsFromTable(Connection conn, TableName tableName) throws IOException {
    Table table = conn.getTable(tableName);
    Scan scan = new Scan().setMaxVersions();
    ResultScanner resultScanner = table.getScanner(scan);
    Set<Cell> cells = Sets.newHashSet();
    for (Result result : resultScanner) {
      cells.addAll(result.listCells());

      if (LOG.isDebugEnabled()) {
        for (Cell cell : result.listCells()) {
          String cellRow =
              Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          String family =
              Bytes.toStringBinary(
                  cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
          String qualifier =
              Bytes.toStringBinary(
                  cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
          String value =
              Bytes.toStringBinary(
                  cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

          LOG.debug(
              "result row:"
                  + cellRow
                  + ", c:"
                  + family
                  + ", q:"
                  + qualifier
                  + ", v:"
                  + value
                  + ", ts:"
                  + cell.getTimestamp());
        }
      }
    }
    return cells;
  }
}
