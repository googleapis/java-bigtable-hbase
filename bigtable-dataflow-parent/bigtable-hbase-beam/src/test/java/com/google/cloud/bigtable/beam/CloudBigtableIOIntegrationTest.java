/*
 * Copyright 2017 Google Inc.
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
package com.google.cloud.bigtable.beam;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.util.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloudBigtableIOIntegrationTest {
  private static final String BIGTABLE_PROJECT_KEY = "google.bigtable.project.id";
  private static final String BIGTABLE_INSTANCE_KEY = "google.bigtable.instance.id";

  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  public static final byte[] QUALIFIER1 = Bytes.toBytes("qualifier1");

  private static final Logger LOG = new Logger(CloudBigtableIOIntegrationTest.class);

  private static final String projectId = System.getProperty(BIGTABLE_PROJECT_KEY);
  private static final String instanceId = System.getProperty(BIGTABLE_INSTANCE_KEY);

  private static final int LARGE_VALUE_SIZE = 201326;

  @Rule public ExpectedException expectedException = ExpectedException.none();

  public static TableName newTestTableName() {
    return TableName.valueOf("test-dataflow-" + UUID.randomUUID());
  }

  private static TableName createNewTable(Admin admin) throws IOException {
    TableName tableName = newTestTableName();
    admin.createTable(
        new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));
    return tableName;
  }

  private static Connection connection;
  private static CloudBigtableConfiguration config;

  @BeforeClass
  public static void setup() throws IOException {
    config =
        new CloudBigtableConfiguration.Builder()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .build();
    connection = BigtableConfiguration.connect(projectId, instanceId);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }

  private static CloudBigtableTableConfiguration createTableConfig(TableName tableName) {
    CloudBigtableTableConfiguration.Builder builder = new CloudBigtableTableConfiguration.Builder();
    config.copyConfig(builder);
    return builder.withTableId(tableName.getNameAsString()).build();
  }

  private static CloudBigtableScanConfiguration createScanConfig(TableName tableName) {
    CloudBigtableScanConfiguration.Builder builder = new CloudBigtableScanConfiguration.Builder();
    config.copyConfig(builder);
    return builder.withTableId(tableName.getNameAsString()).build();
  }

  @Test
  @Ignore
  public void testWriteToTable_dataWrittenBuffered() throws Exception {
    final int INSERT_COUNT = 50;
    try (Admin admin = connection.getAdmin()) {
      TableName tableName = createNewTable(admin);
      DoFn<Mutation, Void> writer =
          new CloudBigtableIO.CloudBigtableSingleTableBufferedWriteFn(createTableConfig(tableName));

      try {
        writeThroughDataflow(writer, INSERT_COUNT);
        checkTableRowCount(tableName, INSERT_COUNT);
      } finally {
        admin.deleteTable(tableName);
      }
    }
  }

  @Test
  @Ignore
  public void testWriteToTable_multiTablewrites() throws Exception {
    final int INSERT_COUNT_PER_TABLE = 50;
    int BATCH_SIZE = INSERT_COUNT_PER_TABLE / 2;
    try (Admin admin = connection.getAdmin()) {
      TableName tableName1 = createNewTable(admin);
      TableName tableName2 = createNewTable(admin);
      DoFn<KV<String, Iterable<Mutation>>, Void> writer =
          new CloudBigtableIO.CloudBigtableMultiTableWriteFn(config);

      try {
        DoFnTester<KV<String, Iterable<Mutation>>, Void> tester = DoFnTester.of(writer);
        tester.processBundle(
            createKV(tableName1, 0, BATCH_SIZE),
            createKV(tableName2, 0, BATCH_SIZE),
            createKV(tableName1, BATCH_SIZE, BATCH_SIZE),
            createKV(tableName2, BATCH_SIZE, BATCH_SIZE));
        checkTableRowCount(tableName1, INSERT_COUNT_PER_TABLE);
        checkTableRowCount(tableName2, INSERT_COUNT_PER_TABLE);
      } finally {
        admin.deleteTable(tableName1);
        admin.deleteTable(tableName2);
      }
    }
  }

  protected KV<String, Iterable<Mutation>> createKV(
      TableName tableName, int start_count, int insertCount) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < insertCount; i++) {
      byte[] row = Bytes.toBytes("row_" + (i + start_count));
      mutations.add(
          new Put(row)
              .addColumn(
                  COLUMN_FAMILY,
                  QUALIFIER1,
                  Bytes.toBytes(RandomStringUtils.randomAlphanumeric(8))));
    }
    return KV.of(tableName.getNameAsString(), mutations);
  }

  private void writeThroughDataflow(DoFn<Mutation, Void> writer, int insertCount) throws Exception {
    DoFnTester<Mutation, Void> fnTester = DoFnTester.of(writer);
    for (int i = 0; i < insertCount; i++) {
      byte[] row = Bytes.toBytes("row_" + i);
      Mutation mutation =
          new Put(row)
              .addColumn(
                  COLUMN_FAMILY,
                  QUALIFIER1,
                  Bytes.toBytes(RandomStringUtils.randomAlphanumeric(8)));

      fnTester.processBundle(mutation);
    }
  }

  private void checkTableRowCount(TableName tableName, int rowCount) throws IOException {
    int readCount = 0;
    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan())) {
      while (scanner.next() != null) {
        readCount++;
      }
    }
    Assert.assertEquals(rowCount, readCount);
  }

  @Test
  @Ignore
  public void testReadFromTable_singleResultDataflowReader() throws Exception {
    final int INSERT_COUNT = 50;
    try (Admin admin = connection.getAdmin()) {
      TableName tableName = createNewTable(admin);
      try {
        writeViaTable(tableName, INSERT_COUNT);
        checkTableRowCountViaDataflowResultReader(tableName, INSERT_COUNT);
      } finally {
        admin.deleteTable(tableName);
      }
    }
  }

  private void writeViaTable(TableName tableName, int rowCount) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes("row_" + i);
      puts.add(
          new Put(row)
              .addColumn(
                  COLUMN_FAMILY,
                  QUALIFIER1,
                  Bytes.toBytes(RandomStringUtils.randomAlphanumeric(8))));
    }
    try (Table t = connection.getTable(tableName)) {
      t.put(puts);
    }
  }

  private void checkTableRowCountViaDataflowResultReader(TableName tableName, int rowCount)
      throws Exception {
    BoundedSource<Result> source = CloudBigtableIO.read(createScanConfig(tableName));
    List<? extends BoundedSource<Result>> splits = source.split(1 << 20, null);
    int count = 0;
    for (BoundedSource<Result> sourceWithKeys : splits) {
      try (BoundedReader<Result> reader = sourceWithKeys.createReader(null)) {
        reader.start();
        while (reader.getCurrent() != null) {
          count++;
          reader.advance();
        }
      }
    }
    Assert.assertEquals(rowCount, count);
  }

  @Test
  @Ignore
  public void testEstimatedAndSplitForSmallTable() throws Exception {
    try (Admin admin = connection.getAdmin()) {
      LOG.info("Creating table in testEstimatedAndSplitForSmallTable()");
      TableName tableName = createNewTable(admin);
      try (Table table = connection.getTable(tableName)) {
        table.put(
            Arrays.asList(
                new Put(Bytes.toBytes("row1"))
                    .addColumn(COLUMN_FAMILY, QUALIFIER1, Bytes.toBytes("1")),
                new Put(Bytes.toBytes("row2"))
                    .addColumn(COLUMN_FAMILY, QUALIFIER1, Bytes.toBytes("2"))));
      }

      LOG.info("getSampleKeys() in testEstimatedAndSplitForSmallTable()");

      try {
        CloudBigtableIO.Source source =
            (CloudBigtableIO.Source) CloudBigtableIO.read(createScanConfig(tableName));
        List<KeyOffset> sampleRowKeys = source.getSampleRowKeys();
        LOG.info("Creating BoundedSource in testEstimatedAndSplitForSmallTable()");
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        KeyOffset lastSample = sampleRowKeys.get(sampleRowKeys.size() - 1);
        Assert.assertEquals(lastSample.getOffsetBytes(), estimatedSizeBytes);

        LOG.info("Creating Bundles in testEstimatedAndSplitForSmallTable()");
        List<? extends BoundedSource<Result>> bundles =
            source.split(estimatedSizeBytes / 2 + 1, null);
        // This will be a small table with no splits, so we return HConstants.EMPTY_START_ROW
        // which can't be split.
        LOG.info("Created Bundles in testEstimatedAndSplitForSmallTable()");
        Assert.assertEquals(sampleRowKeys.size() * 2 - 1, bundles.size());
        Assert.assertSame(sampleRowKeys, source.getSampleRowKeys());
      } finally {
        LOG.info("Deleting table in testEstimatedAndSplitForSmallTable()");
        admin.deleteTable(tableName);
        LOG.info("Deleted table in testEstimatedAndSplitForSmallTable()");
      }
    }
  }

  @Test
  public void testEstimatedAndSplitForLargeTable() throws Exception {
    try (Admin admin = connection.getAdmin()) {
      LOG.info("Creating table in testEstimatedAndSplitForLargeTable()");
      TableName tableName = createNewTable(admin);

      final int rowCount = 1000;
      LOG.info("Adding %d rows in testEstimatedAndSplitForLargeTable()", rowCount);
      try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
        for (int i = 0; i < rowCount; i++) {
          byte[] largeValue = Bytes.toBytes(RandomStringUtils.randomAlphanumeric(LARGE_VALUE_SIZE));
          mutator.mutate(
              new Put(Bytes.toBytes("row" + i)).addColumn(COLUMN_FAMILY, QUALIFIER1, largeValue));
        }
      }

      try {
        LOG.info("Getting Source in testEstimatedAndSplitForLargeTable()");
        CloudBigtableIO.Source source =
            (CloudBigtableIO.Source) CloudBigtableIO.read(createScanConfig(tableName));
        List<KeyOffset> sampleRowKeys = source.getSampleRowKeys();
        LOG.info("Getting estimated size in testEstimatedAndSplitForLargeTable()");
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        KeyOffset lastSample = sampleRowKeys.get(sampleRowKeys.size() - 1);
        Assert.assertEquals(lastSample.getOffsetBytes(), estimatedSizeBytes);

        LOG.info("Getting Bundles in testEstimatedAndSplitForLargeTable()");
        List<? extends BoundedSource<Result>> bundles =
            source.split(sampleRowKeys.get(0).getOffsetBytes() / 2, null);
        // The last sample includes the EMPTY_END_ROW key, which cannot be split.
        Assert.assertEquals(sampleRowKeys.size() * 2 - 1, bundles.size());
        final AtomicInteger count = new AtomicInteger();
        LOG.info("Reading Bundles in testEstimatedAndSplitForLargeTable()");
        ExecutorService es = Executors.newCachedThreadPool();
        try {
          for (final BoundedSource<Result> bundle : bundles) {
            es.submit(
                () -> {
                  try (BoundedReader<Result> reader = bundle.createReader(null)) {
                    reader.start();
                    while (reader.getCurrent() != null) {
                      count.incrementAndGet();
                      reader.advance();
                    }
                  } catch (IOException e) {
                    LOG.warn("Could not read bundle: %s", e, bundle);
                  }
                });
          }
        } finally {
          LOG.info("Shutting down executor in testEstimatedAndSplitForLargeTable()");
          es.shutdown();
          while (!es.isTerminated()) {
            es.awaitTermination(1, TimeUnit.SECONDS);
          }
        }
        Assert.assertSame(sampleRowKeys, source.getSampleRowKeys());
        Assert.assertEquals(rowCount, count.intValue());
      } finally {
        LOG.info("Deleting table in testEstimatedAndSplitForLargeTable()");
        admin.deleteTable(tableName);
      }
    }
  }
}
