/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.bigtable.beam.it;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.test_env.EnvSetup;
import com.google.cloud.bigtable.beam.test_env.TestProperties;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This class contains integration test for Beam Dataflow.It creates dataflow pipelines that perform
 * the following task using pipeline chain process:
 *
 * <ol>
 *   <li>Creates records and performs a Bigtable Put on each record.
 *   <li>Creates Scan and perform count for each Row of Bigtable.
 * </ol>
 */
@RunWith(JUnit4.class)
public class CloudBigtableBeamIT {

  private final Log LOG = LogFactory.getLog(getClass());

  private TestProperties properties;
  private TableName tableName;
  private Connection connection;

  private static final byte[] FAMILY = Bytes.toBytes("test-family");
  private static final byte[] QUALIFIER = Bytes.toBytes("test-qualifier");
  private static final int CELL_SIZE = Integer.getInteger("cell_size", 1_000);
  private static final long TOTAL_ROW_COUNT = Integer.getInteger("total_row_count", 100_000);
  private static final int PREFIX_COUNT = Integer.getInteger("prefix_count", 1_000);

  @Before
  public void setUp() throws IOException {
    EnvSetup.initialize();
    properties = TestProperties.fromSystem();

    Configuration config =
        BigtableConfiguration.configure(properties.getProjectId(), properties.getInstanceId());
    properties.getDataEndpoint().ifPresent(endpoint -> config.set(BIGTABLE_HOST_KEY, endpoint));
    properties
        .getAdminEndpoint()
        .ifPresent(endpoint -> config.set(BIGTABLE_ADMIN_HOST_KEY, endpoint));

    connection = BigtableConfiguration.connect(config);

    // TODO: use timebased names to enable GC
    tableName = TableName.valueOf("test-" + UUID.randomUUID());
    Admin admin = connection.getAdmin();
    admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(FAMILY)));
    LOG.info(String.format("Created a table to perform batching: %s", tableName));
  }

  @After
  public void tearDown() throws Exception {
    connection.getAdmin().deleteTable(tableName);
    connection.close();
  }

  private static final DoFn<String, Mutation> WRITE_ONE_TENTH_PERCENT =
      new DoFn<String, Mutation>() {

        private static final long serialVersionUID = 1L;

        private Counter rowCounter = Metrics.counter(CloudBigtableBeamIT.class, "sent_puts");

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
          String prefix = context.element() + "_";
          int max = (int) (TOTAL_ROW_COUNT / PREFIX_COUNT);
          for (int i = 0; i < max; i++) {
            rowCounter.inc();
            context.output(
                new Put(Bytes.toBytes(prefix + i))
                    .addColumn(FAMILY, QUALIFIER, createRandomValue()));
          }
        }
      };

  @Test
  public void testWriteToBigtable() throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    properties.applyTo(options);
    options.setAppName("testWriteToBigtable-" + System.currentTimeMillis());
    LOG.info(
        String.format("Started writeToBigtable test with jobName as: %s", options.getAppName()));

    CloudBigtableTableConfiguration.Builder configBuilder =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(properties.getProjectId())
            .withInstanceId(properties.getInstanceId())
            .withTableId(tableName.getNameAsString());

    properties
        .getDataEndpoint()
        .ifPresent(endpoint -> configBuilder.withConfiguration(BIGTABLE_HOST_KEY, endpoint));
    properties
        .getAdminEndpoint()
        .ifPresent(endpoint -> configBuilder.withConfiguration(BIGTABLE_ADMIN_HOST_KEY, endpoint));

    CloudBigtableTableConfiguration config = configBuilder.build();

    List<String> keys = new ArrayList<>();
    for (int i = 0; i < PREFIX_COUNT; i++) {
      keys.add(RandomStringUtils.randomAlphanumeric(10));
    }

    PipelineResult.State result =
        Pipeline.create(options)
            .apply("Keys", Create.of(keys))
            .apply("Create Puts", ParDo.of(WRITE_ONE_TENTH_PERCENT))
            .apply("Write to BT", CloudBigtableIO.writeToTable(config))
            .getPipeline()
            .run()
            .waitUntilFinish();

    Assert.assertEquals(PipelineResult.State.DONE, result);

    try (ResultScanner scanner =
        connection.getTable(tableName).getScanner(new Scan().setFilter(new KeyOnlyFilter()))) {
      int count = 0;
      while (scanner.next() != null) {
        count++;
      }
      Assert.assertEquals(TOTAL_ROW_COUNT, count);
    }
  }

  @Test
  public void testReadFromBigtable() throws IOException {
    // Populate the data
    try (BufferedMutator batcher = connection.getBufferedMutator(tableName)) {
      int rowCount = 0;
      for (int i = 0; i < PREFIX_COUNT; i++) {
        String prefix = RandomStringUtils.randomAlphanumeric(10);

        int max = (int) (TOTAL_ROW_COUNT / PREFIX_COUNT);
        for (int j = 0; j < max && rowCount < TOTAL_ROW_COUNT; j++) {
          batcher.mutate(
              new Put(Bytes.toBytes(prefix + (rowCount++)))
                  .addColumn(FAMILY, QUALIFIER, createRandomValue()));
        }
      }
    }

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    properties.applyTo(options);
    options.setJobName("testReadFromBigtable-" + System.currentTimeMillis());
    LOG.info(
        String.format("Started readFromBigtable test with jobName as: %s", options.getJobName()));

    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());

    CloudBigtableScanConfiguration.Builder configBuilder =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(properties.getProjectId())
            .withInstanceId(properties.getInstanceId())
            .withTableId(tableName.getNameAsString())
            .withScan(scan);

    properties
        .getDataEndpoint()
        .ifPresent(endpoint -> configBuilder.withConfiguration(BIGTABLE_HOST_KEY, endpoint));
    properties
        .getAdminEndpoint()
        .ifPresent(endpoint -> configBuilder.withConfiguration(BIGTABLE_ADMIN_HOST_KEY, endpoint));

    CloudBigtableScanConfiguration config = configBuilder.build();

    Pipeline pipeLine = Pipeline.create(options);
    PCollection<Long> count =
        pipeLine
            .apply("Read from BT", Read.from(CloudBigtableIO.read(config)))
            .apply("Count", Count.globally());

    PAssert.thatSingleton(count).isEqualTo(TOTAL_ROW_COUNT);

    PipelineResult.State result = pipeLine.run().waitUntilFinish();
    Assert.assertEquals(PipelineResult.State.DONE, result);
  }

  private static byte[] createRandomValue() {
    byte[] bytes = new byte[CELL_SIZE];
    new Random().nextBytes(bytes);
    return bytes;
  }
}
