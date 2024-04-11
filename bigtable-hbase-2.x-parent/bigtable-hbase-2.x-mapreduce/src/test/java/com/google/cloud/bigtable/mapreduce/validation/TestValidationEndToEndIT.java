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
package com.google.cloud.bigtable.mapreduce.validation;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.test.helper.BigtableEmulatorRule;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor;
import org.apache.hadoop.hbase.mapreduce.HashTable;
import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper.Counter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * end to end integration test for HashTable and SyncTableValidationOnlyJob from Bigtable to HBase
 * and HBase to Bigtable
 */
// TODO - parameterize this to run against prod in future
public class TestValidationEndToEndIT {

  private static final Log LOG = LogFactory.getLog(TestValidationEndToEndIT.class);

  private static final HBaseTestingUtility HB_TEST_UTIL = new HBaseTestingUtility();

  @ClassRule public static final BigtableEmulatorRule bigtableEmulator = new BigtableEmulatorRule();

  // Clients that will be connected to the emulator
  private static Connection bigtableConn;

  private static final long TEST_TS = System.currentTimeMillis();

  // bigtable
  private static final String BT_TEST_PROJECT = "testproject-" + TEST_TS;
  private static final String BT_TEST_INSTANCE = "testinstance-" + TEST_TS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HB_TEST_UTIL.startMiniCluster(1);
    // HB_TEST_UTIL.setJobWithoutMRCluster();

    Configuration conf = BigtableConfiguration.configure(BT_TEST_PROJECT, BT_TEST_INSTANCE);
    conf.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
        "localhost:" + bigtableEmulator.getPort());
    bigtableConn = BigtableConfiguration.connect(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HB_TEST_UTIL.shutdownMiniCluster();
  }

  /** Set configs, create conn to bigtable emulator, and setup table for test. */
  private Table createBigtable(TableName table, byte[] cf, int numRows, int numRegions)
      throws IOException {
    Admin admin = bigtableConn.getAdmin();
    HTableDescriptor htd = new HTableDescriptor(table);
    HColumnDescriptor columnDesc = new HColumnDescriptor(cf);
    columnDesc.setMaxVersions(Integer.MAX_VALUE);
    htd.addFamily(columnDesc);
    admin.createTable(htd, generateSplits(numRows, numRegions));
    LOG.info("bigtable table created: " + htd.toString());

    return bigtableConn.getTable(table);
  }

  /** Create a table and setup for test. */
  private Table createHBaseTable(TableName table, byte[] cf, int numRows, int numRegions)
      throws IOException {
    // create Table
    Admin admin = HB_TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(table);

    HColumnDescriptor columnDesc = new HColumnDescriptor(cf);
    columnDesc.setMaxVersions(Integer.MAX_VALUE);
    htd.addFamily(columnDesc);
    admin.createTable(htd, generateSplits(numRows, numRegions));

    LOG.info("hbase table created: " + htd.toString());

    return HB_TEST_UTIL.getConnection().getTable(table);
  }

  private static byte[][] generateSplits(int numRows, int numRegions) {
    byte[][] splitRows = new byte[numRegions - 1][];
    for (int i = 1; i < numRegions; i++) {
      splitRows[i - 1] = Bytes.toBytes(numRows * i / numRegions);
    }
    return splitRows;
  }

  public static String getZkConnArgFromConfig(Configuration conf) {
    return new StringBuilder()
        .append(conf.get(HConstants.ZOOKEEPER_QUORUM))
        .append(":")
        .append(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT))
        .append(":")
        .append(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT))
        .toString();
  }

  /** validate data migrated from Bigtable to HBase */
  @Test
  public void testSyncTableBigtabletoHBaseValidationMatches() throws Exception {
    long testRunTs = System.currentTimeMillis();

    // source and target
    TableName BT_TEST_SOURCE_TABLE = TableName.valueOf("cbt-testsourcetable-matches-" + testRunTs);
    TableName HB_TEST_TARGET_TABLE =
        TableName.valueOf("hbase-testtargettable-matches-" + testRunTs);
    Path testHashOutDir = HB_TEST_UTIL.getDataTestDirOnTestFS("hash-out-cbt-matches-" + testRunTs);

    // generate data
    writeMatchTestData(BT_TEST_SOURCE_TABLE, false, HB_TEST_TARGET_TABLE, true);

    // hash table
    hashSourceBigtable(BT_TEST_SOURCE_TABLE, testHashOutDir);

    // sync table validation
    Counters syncCounters =
        bigtableSyncTableJobSourceBigtableTargetHBase(
            BT_TEST_SOURCE_TABLE, HB_TEST_TARGET_TABLE, testHashOutDir);

    assertVerifySyncMatches(syncCounters);
  }

  /** validate data migrated from Bigtable to HBase */
  @Test
  public void testSyncTableBigtabletoHBaseValidationMismatches() throws Exception {
    long testRunTs = System.currentTimeMillis();

    // source and target
    TableName BT_TEST_SOURCE_TABLE =
        TableName.valueOf("cbt-testsourcetable-mismatches-" + testRunTs);
    TableName HB_TEST_TARGET_TABLE =
        TableName.valueOf("hbase-testtargettable-mismatches-" + testRunTs);
    Path testHashOutDir =
        HB_TEST_UTIL.getDataTestDirOnTestFS("hash-out-cbt-mismatches-" + testRunTs);

    // generate data
    writeMismatchTestData(BT_TEST_SOURCE_TABLE, false, HB_TEST_TARGET_TABLE, true);

    // hash table
    hashSourceBigtable(BT_TEST_SOURCE_TABLE, testHashOutDir);

    // sync table validation
    Counters syncCounters =
        bigtableSyncTableJobSourceBigtableTargetHBase(
            BT_TEST_SOURCE_TABLE, HB_TEST_TARGET_TABLE, testHashOutDir);

    assertVerifySyncMismatches(syncCounters);
  }

  /** validate data migrated from HBase to Bigtable */
  @Test
  public void testSyncTableHBaseToBigtableValidationMatches() throws Exception {
    long testRunTs = System.currentTimeMillis();

    // source and target
    TableName HB_TEST_SOURCE_TABLE =
        TableName.valueOf("hbase-testsourcetable-matches-" + testRunTs);
    TableName BT_TEST_TARGET_TABLE = TableName.valueOf("cbt-testtargettable-matches-" + testRunTs);
    Path testHashOutDir =
        HB_TEST_UTIL.getDataTestDirOnTestFS("hash-out-hbase-matches-" + +testRunTs);

    // generate data
    writeMatchTestData(HB_TEST_SOURCE_TABLE, true, BT_TEST_TARGET_TABLE, false);

    // hash table
    hashSourceHBaseTable(HB_TEST_SOURCE_TABLE, testHashOutDir);

    // sync table validation
    Counters syncCounters =
        bigtableSyncTableJobSourceHBaseTargetBigtable(
            HB_TEST_SOURCE_TABLE, BT_TEST_TARGET_TABLE, testHashOutDir);

    assertVerifySyncMatches(syncCounters);
  }

  /** validate data migrated from HBase to Bigtable */
  @Test
  public void testSyncTableHBaseToBigtableValidationMismatches() throws Exception {
    long testRunTs = System.currentTimeMillis();

    // source and target
    TableName HB_TEST_SOURCE_TABLE =
        TableName.valueOf("hbase-testsourcetable-mismatches-" + testRunTs);
    TableName BT_TEST_TARGET_TABLE =
        TableName.valueOf("cbt-testtargettable-mismatches-" + testRunTs);
    Path testHashOutDir =
        HB_TEST_UTIL.getDataTestDirOnTestFS("hash-out-hbase-mismatches-" + +testRunTs);

    // generate data
    writeMismatchTestData(HB_TEST_SOURCE_TABLE, true, BT_TEST_TARGET_TABLE, false);

    // hash table
    hashSourceHBaseTable(HB_TEST_SOURCE_TABLE, testHashOutDir);

    // sync table validation
    Counters syncCounters =
        bigtableSyncTableJobSourceHBaseTargetBigtable(
            HB_TEST_SOURCE_TABLE, BT_TEST_TARGET_TABLE, testHashOutDir);

    assertVerifySyncMismatches(syncCounters);
  }

  /** assert match/mismatch results from counters and output records */
  private void assertVerifySyncMatches(Counters syncCounters) {
    assertEquals(
        syncCounters.findCounter(Counter.HASHES_MATCHED).getValue(),
        syncCounters.findCounter(Counter.BATCHES).getValue());
    assertEquals(0, syncCounters.findCounter(Counter.HASHES_NOT_MATCHED).getValue());
  }

  /** assert match/mismatch results from counters and output records */
  private void assertVerifySyncMismatches(Counters syncCounters) {
    // verify counters
    assertEquals(60, syncCounters.findCounter(Counter.ROWSWITHDIFFS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.SOURCEMISSINGROWS).getValue());
    assertEquals(10, syncCounters.findCounter(Counter.TARGETMISSINGROWS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.SOURCEMISSINGCELLS).getValue());
    assertEquals(50, syncCounters.findCounter(Counter.TARGETMISSINGCELLS).getValue());
    assertEquals(20, syncCounters.findCounter(Counter.DIFFERENTCELLVALUES).getValue());
  }

  /** setup config for launching with bigtable as source and hbase as target */
  private Counters bigtableSyncTableJobSourceBigtableTargetHBase(
      TableName sourceTableName, TableName targetTableName, Path testHashOutDir, String... options)
      throws Exception {

    String[] args = Arrays.copyOf(options, options.length + 3);
    args[options.length] = "--sourcebigtableproject=" + BT_TEST_PROJECT;
    args[options.length + 1] = "--sourcebigtableinstance=" + BT_TEST_INSTANCE;
    args[options.length + 2] =
        "--targetzkcluster=" + getZkConnArgFromConfig(HB_TEST_UTIL.getConfiguration());

    return bigtableSyncTableJob(sourceTableName, targetTableName, testHashOutDir, args);
  }

  /** setup config for launching with hbase as source and bigtable as target */
  private Counters bigtableSyncTableJobSourceHBaseTargetBigtable(
      TableName sourceTableName, TableName targetTableName, Path testHashOutDir, String... options)
      throws Exception {

    String[] args = Arrays.copyOf(options, options.length + 3);
    args[options.length] =
        "--sourcezkcluster=" + getZkConnArgFromConfig(HB_TEST_UTIL.getConfiguration());
    args[options.length + 1] = "--targetbigtableproject=" + BT_TEST_PROJECT;
    args[options.length + 2] = "--targetbigtableinstance=" + BT_TEST_INSTANCE;

    return bigtableSyncTableJob(sourceTableName, targetTableName, testHashOutDir, args);
  }

  /** run synctable validation job */
  private Counters bigtableSyncTableJob(
      TableName sourceTableName, TableName targetTableName, Path testHashOutDir, String... options)
      throws Exception {

    // set emulator config for job
    Configuration conf = new Configuration(false);
    conf.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
        "localhost:" + bigtableEmulator.getPort());
    conf.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "false");

    BigtableSyncTableJob syncTable = new BigtableSyncTableJob(conf);
    String[] args = Arrays.copyOf(options, options.length + 3);
    args[options.length] = testHashOutDir.toString(); // sourcehashdir
    args[options.length + 1] = sourceTableName.getNameAsString(); // sourcetablename
    args[options.length + 2] = targetTableName.getNameAsString(); // targettablename
    int code = syncTable.run(args);
    assertEquals("sync table job failed", 0, code);

    LOG.info("Sync tables completed");
    return syncTable.counters;
  }

  /** initialize configuration for hashing source on HBase */
  private void hashSourceHBaseTable(
      TableName sourceTableName, Path testHashOutDir, String... options) throws Exception {
    Configuration conf = HB_TEST_UTIL.getConfiguration();
    hashSourceTable(sourceTableName, testHashOutDir, conf, options);
  }

  /** initialize configuration for hashing source on Bigtable */
  private void hashSourceBigtable(TableName sourceTableName, Path testHashOutDir, String... options)
      throws Exception {
    Configuration conf = new Configuration(false);
    BigtableConfiguration.configure(conf, BT_TEST_PROJECT, BT_TEST_INSTANCE);
    conf.set(
        BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
        "localhost:" + bigtableEmulator.getPort());
    hashSourceTable(sourceTableName, testHashOutDir, conf, options);
  }

  /** run hashtable on source */
  private void hashSourceTable(
      TableName sourceTableName, Path testHashOutDir, Configuration conf, String... options)
      throws Exception {
    long batchSize = 100;
    int numHashFiles = 2;
    int scanBatch = 1;
    String[] args = Arrays.copyOf(options, options.length + 5);
    args[options.length] = "--batchsize=" + batchSize;
    args[options.length + 1] = "--numhashfiles=" + numHashFiles;
    args[options.length + 2] = "--scanbatch=" + scanBatch;
    args[options.length + 3] = sourceTableName.getNameAsString();
    args[options.length + 4] = testHashOutDir.toString();

    HashTable hashTable = new HashTable(conf);
    int code = hashTable.run(args);
    assertEquals("hash table job failed", 0, code);

    FileSystem fs = HB_TEST_UTIL.getTestFileSystem();
    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testHashOutDir);
    assertEquals(
        sourceTableName.getNameAsString(), BigtableTableHashAccessor.getTableName(tableHash));
    assertEquals(batchSize, BigtableTableHashAccessor.getBatchSize(tableHash));
    assertEquals(numHashFiles, BigtableTableHashAccessor.getNumHashFiles(tableHash));
    assertEquals(numHashFiles - 1, BigtableTableHashAccessor.getPartitions(tableHash).size());

    LOG.info("Hash table completed");
  }

  private void writeMatchTestData(
      TableName sourceTableName,
      boolean isSourceHBase,
      TableName targetTableName,
      boolean isTargetHBase,
      long... timestamps)
      throws IOException {
    final byte[] family = Bytes.toBytes("cf");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] value1 = Bytes.toBytes("val1");
    final byte[] value2 = Bytes.toBytes("val2");

    int numRows = 100;
    int sourceRegions = 10;
    int targetRegions = 6;
    if (timestamps.length == 0) {
      long current = System.currentTimeMillis();
      timestamps = new long[] {current, current};
    }

    // create source/target tables
    Table sourceTable = null;
    if (isSourceHBase) {
      sourceTable = createHBaseTable(sourceTableName, family, numRows, sourceRegions);
    } else {
      sourceTable = createBigtable(sourceTableName, family, numRows, sourceRegions);
    }

    Table targetTable = null;
    if (isTargetHBase) {
      targetTable = createHBaseTable(targetTableName, family, numRows, targetRegions);
    } else {
      targetTable = createBigtable(targetTableName, family, numRows, targetRegions);
    }

    int rowIndex = 0;
    // a bunch of identical rows
    for (; rowIndex < numRows; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetPut.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(targetPut);
    }

    sourceTable.close();
    targetTable.close();
  }

  private void writeMismatchTestData(
      TableName sourceTableName,
      boolean isSourceHBase,
      TableName targetTableName,
      boolean isTargetHBase,
      long... timestamps)
      throws IOException {
    final byte[] family = Bytes.toBytes("cf");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] value1 = Bytes.toBytes("val1");
    final byte[] value2 = Bytes.toBytes("val2");
    final byte[] value3 = Bytes.toBytes("val3");

    int numRows = 100;
    int sourceRegions = 10;
    int targetRegions = 6;
    if (timestamps.length == 0) {
      long current = System.currentTimeMillis();
      timestamps = new long[] {current, current};
    }

    // create source/target tables
    Table sourceTable = null;
    if (isSourceHBase) {
      sourceTable = createHBaseTable(sourceTableName, family, numRows, sourceRegions);
    } else {
      sourceTable = createBigtable(sourceTableName, family, numRows, sourceRegions);
    }

    Table targetTable = null;
    if (isTargetHBase) {
      targetTable = createHBaseTable(targetTableName, family, numRows, targetRegions);
    } else {
      targetTable = createBigtable(targetTableName, family, numRows, targetRegions);
    }

    int rowIndex = 0;
    // a bunch of identical rows
    for (; rowIndex < 40; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetPut.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(targetPut);
    }
    // some rows only in the source table
    // ROWSWITHDIFFS: 10
    // TARGETMISSINGROWS: 10
    // TARGETMISSINGCELLS: 20
    for (; rowIndex < 50; rowIndex++) {
      Put put = new Put(Bytes.toBytes(rowIndex));
      put.addColumn(family, column1, timestamps[0], value1);
      put.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(put);
    }
    // some rows only in the target table
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGROWS: 10
    // SOURCEMISSINGCELLS: 20
    for (; rowIndex < 60; rowIndex++) {
      Put put = new Put(Bytes.toBytes(rowIndex));
      put.addColumn(family, column1, timestamps[1], value1);
      put.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(put);
    }
    // some rows with 1 missing cell in target table
    // ROWSWITHDIFFS: 10
    // TARGETMISSINGCELLS: 10
    for (; rowIndex < 70; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetTable.put(targetPut);
    }
    // some rows with 1 missing cell in source table
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGCELLS: 10
    for (; rowIndex < 80; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value1);
      targetPut.addColumn(family, column2, timestamps[1], value2);
      targetTable.put(targetPut);
    }
    // some rows differing only in timestamp
    // ROWSWITHDIFFS: 10
    // SOURCEMISSINGCELLS: 20
    // TARGETMISSINGCELLS: 20
    for (; rowIndex < 90; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], column1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1] + 1, column1);
      targetPut.addColumn(family, column2, timestamps[1] - 1, value2);
      targetTable.put(targetPut);
    }
    // some rows with different values
    // ROWSWITHDIFFS: 10
    // DIFFERENTCELLVALUES: 20
    for (; rowIndex < numRows; rowIndex++) {
      Put sourcePut = new Put(Bytes.toBytes(rowIndex));
      sourcePut.addColumn(family, column1, timestamps[0], value1);
      sourcePut.addColumn(family, column2, timestamps[0], value2);
      sourceTable.put(sourcePut);

      Put targetPut = new Put(Bytes.toBytes(rowIndex));
      targetPut.addColumn(family, column1, timestamps[1], value3);
      targetPut.addColumn(family, column2, timestamps[1], value3);
      targetTable.put(targetPut);
    }

    sourceTable.close();
    targetTable.close();
  }
}
