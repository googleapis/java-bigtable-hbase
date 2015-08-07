package com.google.cloud.bigtable.dataflow;

import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO.Source.SourceWithKeys;
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

public class CloudBigtableIOIntegrationTest {
  private static final String BIGTABLE_PROJECT_KEY = "google.bigtable.project.id";
  private static final String BIGTABLE_ZONE_KEY = "google.bigtable.zone.name";
  private static final String BIGTABLE_CLUSTER_KEY = "google.bigtable.cluster.name";

  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  public static final byte[] QUALIFIER1 = Bytes.toBytes("qualifier1");

  private static String projectId = System.getProperty(BIGTABLE_PROJECT_KEY);
  private static String zoneId = System.getProperty(BIGTABLE_ZONE_KEY);
  private static String clusterId = System.getProperty(BIGTABLE_CLUSTER_KEY);

  public static TableName newTestTableName() {
    return TableName.valueOf("test-dataflow-" + UUID.randomUUID().toString());
  }

  private static BigtableConnection connection;
  private static CloudBigtableConfiguration config;

  @SuppressWarnings("rawtypes")
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
  public static void shutdown() {
    connection.close();
  }

  @Test
  public void testEstimatedAndSplitForSmallTable() throws Exception {
    TableName tableName = newTestTableName();
    try (Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));
      try (Table table = connection.getTable(tableName)) {
        table.put(Arrays.asList(
          new Put(Bytes.toBytes("row1")).addColumn(COLUMN_FAMILY, QUALIFIER1, Bytes.toBytes("1")),
          new Put(Bytes.toBytes("row2")).addColumn(COLUMN_FAMILY, QUALIFIER1, Bytes.toBytes("2"))));
      }

      List<SampleRowKeysResponse> sampleRowKeys = getSampleKeys(tableName);

      try {
        BoundedSource<Result> source =
            CloudBigtableIO.read(new CloudBigtableScanConfiguration(
                projectId, zoneId, clusterId, tableName.getQualifierAsString(), new Scan()));
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        SampleRowKeysResponse lastSample = sampleRowKeys.get(sampleRowKeys.size() - 1);
        Assert.assertEquals(lastSample.getOffsetBytes(), estimatedSizeBytes);

        List<? extends BoundedSource<Result>> bundles =
            source.splitIntoBundles(estimatedSizeBytes / 2 + 1, null);
        // This will be a small table with no splits, so we return HConstants.EMPTY_START_ROW
        // which can't be split.
        Assert.assertEquals(sampleRowKeys.size() * 2 - 1, bundles.size());
      } finally {
        admin.deleteTable(tableName);
      }
    }
  }

  @Test
  public void testEstimatedAndSplitForLargeTable() throws Exception {
    TableName tableName = newTestTableName();
    try (Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(COLUMN_FAMILY)));

      final int rowCount = 1000;
      try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
        for (int i = 0; i < rowCount; i++ ) {
          mutator.mutate(
              new Put(Bytes.toBytes("row" + i))
                  .addColumn(COLUMN_FAMILY, QUALIFIER1, getLargeValue()));
        }
      }

      List<SampleRowKeysResponse> sampleRowKeys = getSampleKeys(tableName);

      try {
        BoundedSource<Result> source =
            CloudBigtableIO.read(new CloudBigtableScanConfiguration(projectId, zoneId, clusterId,
                tableName.getQualifierAsString(), new Scan()));
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        SampleRowKeysResponse lastSample = sampleRowKeys.get(sampleRowKeys.size() - 1);
        Assert.assertEquals(lastSample.getOffsetBytes(), estimatedSizeBytes);

        List<? extends BoundedSource<Result>> bundles =
            source.splitIntoBundles(sampleRowKeys.get(0).getOffsetBytes() / 2, null);
        // The last sample includes the EMPTY_END_ROW key, which cannot be split.
        Assert.assertEquals(sampleRowKeys.size() * 2 - 1, bundles.size());
        int count = 0;
        for (BoundedSource<Result> bundle : bundles) {
          BoundedReader<Result> reader = bundle.createReader(null);
          reader.start();
          while(reader.getCurrent() != null) {
            count++;
            reader.advance();
          }
          reader.close();
        }
        Assert.assertEquals(rowCount, count);
      } finally {
        admin.deleteTable(tableName);
      }
    }
  }

  private static int LARGE_VALUE_SIZE = 201326;

  private static byte[] getLargeValue() {
    return Bytes.toBytes(RandomStringUtils.randomAlphanumeric(LARGE_VALUE_SIZE));
  }

  private List<SampleRowKeysResponse> getSampleKeys(TableName tableName) throws IOException {
    BigtableClusterName clusterName = config.toBigtableOptions().getClusterName();
    BigtableTableName btTableName = clusterName.toTableName(tableName.getQualifierAsString());
    SampleRowKeysRequest.Builder request = SampleRowKeysRequest.newBuilder();
    request.setTableName(btTableName.toString());
    return connection.getSession().getDataClient().sampleRowKeys(request.build());
  }
}
