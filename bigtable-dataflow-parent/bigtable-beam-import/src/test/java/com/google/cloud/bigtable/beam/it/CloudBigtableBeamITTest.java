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

import static com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT;
import static com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_BATCH_DATA_HOST_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.TestHelper;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for Beam Dataflow. The test creates dataflow pipelines that perform the
 * following tasks using pipeline chain process:
 *
 * <ol>
 *   <li>Create records and perform the Bigtable Put on each record.
 *   <li>Create Scan and perform Count for each row of Bigtable.
 * </ol>
 *
 * <p>Mandatory parameters:
 *
 * <pre>
 *  -Dgoogle.bigtable.project.id=[bigtable project]
 *  -Dgoogle.bigtable.instance.id=[bigtable instance id]
 *  -Dgoogle.dataflow.gcsPath=gs://[google storage path]
 * </pre>
 *
 * <p>Optional parameters, if not provided defaults will be used:
 *
 * <pre>
 *  -Dgoogle.bigtable.endpoint.host=[bigtable batch host]
 *  -Dgoogle.bigtable.admin.endpoint.host=[bigtable admin host]
 *  -Dgoogle.dataflow.zoneId=[dataflow zone Id]
 *  -Dgoogle.dataflow.tableName=[table name to be used]
 *  -Dgoogle.dataflow.cell_size=[cell size]
 *  -Dgoogle.dataflow.total_row_count=[number of rows to write and read]
 *  -Dgoogle.dataflow.prefix_count=[cell prefix count]
 * </pre>
 */
@RunWith(JUnit4.class)
public class CloudBigtableBeamITTest {

  private final Log LOG = LogFactory.getLog(getClass());

  private static String projectId;
  private static String instanceId;
  private static String gcsPath;
  private static String gcsWorkDir;
  private static String stagingLocation;
  private static String tempLocation;

  private static final String zoneId = System.getProperty("google.dataflow.zoneId");

  private static final String workerMachineType =
      System.getProperty("workerMachineType", "n1" + "-standard-8");
  private static final String dataEndpoint =
      System.getProperty(BIGTABLE_HOST_KEY, BIGTABLE_BATCH_DATA_HOST_DEFAULT);
  private static final String adminEndpoint =
      System.getProperty(BIGTABLE_ADMIN_HOST_KEY, BIGTABLE_ADMIN_HOST_DEFAULT);
  private static final String TABLE_NAME_STR =
      System.getProperty("google.dataflow.tableName", "BeamCloudBigtableIOIntegrationTest");

  private static final TableName TABLE_NAME = TableName.valueOf(TABLE_NAME_STR);
  private static final byte[] FAMILY = Bytes.toBytes("google.dataflow.test-family");
  private static final byte[] QUALIFIER = Bytes.toBytes("test-qualifier");
  private static final int CELL_SIZE = Integer.getInteger("google.dataflow.cell_size", 1_000);
  private static final long TOTAL_ROW_COUNT =
      Integer.getInteger("google.dataflow.total_row_count", 1_000_000);
  private static final int PREFIX_COUNT = Integer.getInteger("google.dataflow.prefix_count", 1_000);

  @BeforeClass
  public static void setUpConfiguration() {
    projectId = TestHelper.getTestProperty(PROJECT_ID_KEY);
    instanceId = TestHelper.getTestProperty(INSTANCE_ID_KEY);
    gcsPath = TestHelper.getTestProperty("google.dataflow.gcsPath");
    gcsWorkDir = gcsPath + "/" + UUID.randomUUID().toString();
    stagingLocation = gcsWorkDir + "/staging";
    tempLocation = gcsWorkDir + "/temp";
  }

  @Before
  public void setUp() throws IOException {
    Configuration config = BigtableConfiguration.configure(projectId, instanceId);
    config.set(BIGTABLE_HOST_KEY, dataEndpoint);
    config.set(BIGTABLE_ADMIN_HOST_KEY, adminEndpoint);
    try (Connection conn = BigtableConfiguration.connect(config);
        Admin admin = conn.getAdmin()) {
      if (admin.tableExists(TABLE_NAME)) {
        admin.deleteTable(TABLE_NAME);
      }
      admin.createTable(new HTableDescriptor(TABLE_NAME).addFamily(new HColumnDescriptor(FAMILY)));
      LOG.info(String.format("Created a table to perform batching: %s", TABLE_NAME));
    }
  }

  @After
  public void tearDown() throws IOException {
    TestHelper.init(projectId);
    TestHelper.cleanUpStorageFolder(stagingLocation);
    TestHelper.cleanUpStorageFolder(tempLocation);
    TestHelper.cleanUpStorageFolder(gcsWorkDir);
  }

  private static final DoFn<String, Mutation> WRITE_ONE_TENTH_PERCENT =
      new DoFn<String, Mutation>() {

        private static final long serialVersionUID = 1L;

        private Counter rowCounter = Metrics.counter(CloudBigtableBeamITTest.class, "sent_puts");

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

  private void testWriteToBigtable() {
    DataflowPipelineOptions options = createOptions();
    options.setAppName("testWriteToBigtable-" + System.currentTimeMillis());
    LOG.info(
        String.format("Started writeToBigtable test with jobName as: %s", options.getAppName()));

    CloudBigtableTableConfiguration config =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withTableId(TABLE_NAME.getNameAsString())
            .withConfiguration(BIGTABLE_ADMIN_HOST_KEY, adminEndpoint)
            .withConfiguration(BIGTABLE_HOST_KEY, dataEndpoint)
            .build();

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
  }

  private Pipeline testReadFromBigtable() {
    PipelineOptions options = createOptions();
    options.setJobName("testReadFromBigtable-" + System.currentTimeMillis());
    LOG.info(
        String.format("Started readFromBigtable test with jobName as: %s", options.getJobName()));

    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());

    CloudBigtableScanConfiguration config =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withTableId(TABLE_NAME.getNameAsString())
            .withScan(scan)
            .withConfiguration(BIGTABLE_ADMIN_HOST_KEY, adminEndpoint)
            .withConfiguration(BIGTABLE_HOST_KEY, dataEndpoint)
            .build();

    Pipeline pipeLine = Pipeline.create(options);
    PCollection<Long> count =
        pipeLine
            .apply("Read from BT", Read.from(CloudBigtableIO.read(config)))
            .apply("Count", Count.<Result>globally());

    PAssert.thatSingleton(count).isEqualTo(TOTAL_ROW_COUNT);
    return pipeLine;
  }

  @Test
  public void testRunner() {
    try {
      // Submitted write pipeline to mutate the Bigtable.
      testWriteToBigtable();

      Pipeline result = testReadFromBigtable();
      PipelineResult.State readJobStatue = result.run().waitUntilFinish();

      Assert.assertEquals(PipelineResult.State.DONE, readJobStatue);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new AssertionError("Exception occurred while pipeline execution");
    }
  }

  private static byte[] createRandomValue() {
    byte[] bytes = new byte[CELL_SIZE];
    new Random().nextBytes(bytes);
    return bytes;
  }

  private DataflowPipelineOptions createOptions() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject(projectId);
    if (zoneId != null) {
      options.setZone(zoneId);
    }
    options.setStagingLocation(stagingLocation);
    options.setTempLocation(tempLocation);
    options.setRunner(DataflowRunner.class);
    options.setWorkerMachineType(workerMachineType);
    return options;
  }
}
