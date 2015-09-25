package com.google.cloud.bigtable.dataflow;

import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase1_0.BigtableConnection;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudBigtableIOIntegrationTest {
  private static final String BIGTABLE_PROJECT_KEY = "google.bigtable.project.id";
  private static final String BIGTABLE_ZONE_KEY = "google.bigtable.zone.name";
  private static final String BIGTABLE_CLUSTER_KEY = "google.bigtable.cluster.name";

  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  public static final byte[] QUALIFIER1 = Bytes.toBytes("qualifier1");

  private static final Logger LOG = new Logger(CloudBigtableIOIntegrationTest.class);

  private static String projectId = System.getProperty(BIGTABLE_PROJECT_KEY);
  private static String zoneId = System.getProperty(BIGTABLE_ZONE_KEY);
  private static String clusterId = System.getProperty(BIGTABLE_CLUSTER_KEY);

  public static TableName newTestTableName() {
    return TableName.valueOf("test-dataflow-" + UUID.randomUUID().toString());
  }

  private static BigtableConnection connection;
  private static CloudBigtableConfiguration config;

  @BeforeClass
  public static void setup() throws IOException {
    config =
        new CloudBigtableConfiguration.Builder()
            .withProjectId(projectId)
            .withZoneId(zoneId)
            .withClusterId(clusterId)
            .build();
    connection = new BigtableConnection(config.toHBaseConfig());
  }

  @AfterClass
  public static void shutdown() throws IOException {
    connection.close();
  }

  @Test
  public void testEstimatedAndSplitForSmallTable() throws Exception {
    TableName tableName = newTestTableName();
    try (Admin admin = connection.getAdmin()) {
      LOG.info("Creating table in testEstimatedAndSplitForSmallTable()");
      admin.createTable(
          new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));
      try (Table table = connection.getTable(tableName)) {
        table.put(Arrays.asList(
          new Put(Bytes.toBytes("row1")).addColumn(COLUMN_FAMILY, QUALIFIER1, Bytes.toBytes("1")),
          new Put(Bytes.toBytes("row2")).addColumn(COLUMN_FAMILY, QUALIFIER1, Bytes.toBytes("2"))));
      }

      LOG.info("getSampleKeys() in testEstimatedAndSplitForSmallTable()");

      try {
        CloudBigtableScanConfiguration config =
            new CloudBigtableScanConfiguration(projectId, zoneId, clusterId,
                tableName.getQualifierAsString(), new Scan());
        CloudBigtableIO.Source source = new CloudBigtableIO.Source(config);
        List<SampleRowKeysResponse> sampleRowKeys = source.getSampleRowKeys();
        LOG.info("Creating BoundedSource in testEstimatedAndSplitForSmallTable()");
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        SampleRowKeysResponse lastSample = sampleRowKeys.get(sampleRowKeys.size() - 1);
        Assert.assertEquals(lastSample.getOffsetBytes(), estimatedSizeBytes);

        LOG.info("Creating Bundles in testEstimatedAndSplitForSmallTable()");
        List<? extends BoundedSource<Result>> bundles =
            source.splitIntoBundles(estimatedSizeBytes / 2 + 1, null);
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
    TableName tableName = newTestTableName();
    try (Admin admin = connection.getAdmin()) {
      LOG.info("Creating table in testEstimatedAndSplitForLargeTable()");
      admin.createTable(
          new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));

      final int rowCount = 1000;
      LOG.info("Adding %d rows in testEstimatedAndSplitForLargeTable()", rowCount);
      try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
        for (int i = 0; i < rowCount; i++ ) {
          byte[] largeValue = Bytes.toBytes(RandomStringUtils.randomAlphanumeric(LARGE_VALUE_SIZE));
          mutator.mutate(new Put(Bytes.toBytes("row" + i)).addColumn(COLUMN_FAMILY, QUALIFIER1,
            largeValue));
        }
      }

      try {
        LOG.info("Getting Source in testEstimatedAndSplitForLargeTable()");
        CloudBigtableScanConfiguration config =
            new CloudBigtableScanConfiguration(projectId, zoneId, clusterId,
                tableName.getQualifierAsString(), new Scan());
        CloudBigtableIO.Source source = new CloudBigtableIO.Source(config);
        List<SampleRowKeysResponse> sampleRowKeys = source.getSampleRowKeys();
        LOG.info("Getting estimated size in testEstimatedAndSplitForLargeTable()");
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        SampleRowKeysResponse lastSample = sampleRowKeys.get(sampleRowKeys.size() - 1);
        Assert.assertEquals(lastSample.getOffsetBytes(), estimatedSizeBytes);

        LOG.info("Getting Bundles in testEstimatedAndSplitForLargeTable()");
        List<? extends BoundedSource<Result>> bundles =
            source.splitIntoBundles(sampleRowKeys.get(0).getOffsetBytes() / 2, null);
        // The last sample includes the EMPTY_END_ROW key, which cannot be split.
        Assert.assertEquals(sampleRowKeys.size() * 2 - 1, bundles.size());
        final AtomicInteger count = new AtomicInteger();
        LOG.info("Reading Bundles in testEstimatedAndSplitForLargeTable()");
        ExecutorService es = Executors.newCachedThreadPool();
        try {
          for (final BoundedSource<Result> bundle : bundles) {
            es.submit(new Runnable() {
              @Override
              public void run() {
                try (BoundedReader<Result> reader = bundle.createReader(null)) {
                  reader.start();
                  while (reader.getCurrent() != null) {
                    count.incrementAndGet();
                    reader.advance();
                  }
                } catch (IOException e) {
                  LOG.warn("Could not read bundle: %s", e, bundle);
                }
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

  private static int LARGE_VALUE_SIZE = 201326;
}
