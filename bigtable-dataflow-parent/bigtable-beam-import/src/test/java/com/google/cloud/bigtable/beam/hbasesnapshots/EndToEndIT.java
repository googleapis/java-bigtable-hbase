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
package com.google.cloud.bigtable.beam.hbasesnapshots;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigtable.beam.sequencefiles.testing.BigtableTableUtils;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*
 * End to end integration test for pipeline that import HBase snapshot data into Cloud Bigtable.
 * Prepare test data with gsutil(https://cloud.google.com/storage/docs/quickstart-gsutil):
 * gsutil -m cp -r <PATH_TO_REPO>/bigtable-dataflow-parent/bigtable-beam-import/src/test/data/ \
 *  gs://<test_bucket>/integration-test/
 *
 * Setup GCP credential: https://cloud.google.com/docs/authentication
 *  Ensure your credential have access to Bigtable and Dataflow
 *
 * Run with:
 * mvn integration-test -PhbasesnapshotsIntegrationTest \
 * -Dgoogle.bigtable.project.id=<project_id> \
 * -Dgoogle.bigtable.instance.id=<instance_id> \
 * -Dgoogle.dataflow.stagingLocation=gs://<test_bucket>/staging \
 * -Dcloud.test.data.folder=gs://<test_bucket>/integration-test/
 */
public class EndToEndIT {

  private static final String TEST_SNAPSHOT_NAME = "test-snapshot";
  // Location of test data hosted on Google Cloud Storage, for on-cloud dataflow tests.
  private static final String CLOUD_TEST_DATA_FOLDER = "cloud.test.data.folder";
  private static final String DATAFLOW_REGION = "region";

  // Column family name used in all test bigtables.
  private static final String CF = "cf";

  // Full path of the Cloud Storage folder where dataflow jars are uploaded to.
  private static final String GOOGLE_DATAFLOW_STAGING_LOCATION = "google.dataflow.stagingLocation";

  private Connection connection;
  private String projectId;
  private String instanceId;
  private String tableId;
  private String region;

  private GcsUtil gcsUtil;
  private String dataflowStagingLocation;
  private String workDir;
  private byte[][] keySplits;

  // Snapshot data setup
  private String hbaseSnapshotDir;
  private String restoreDir;

  @Before
  public void setup() throws Exception {
    projectId = getTestProperty(BigtableOptionsFactory.PROJECT_ID_KEY);
    instanceId = getTestProperty(BigtableOptionsFactory.INSTANCE_ID_KEY);
    dataflowStagingLocation = getTestProperty(GOOGLE_DATAFLOW_STAGING_LOCATION);
    region = getTestProperty(DATAFLOW_REGION);
    String cloudTestDataFolder = getTestProperty(CLOUD_TEST_DATA_FOLDER);
    if (!cloudTestDataFolder.endsWith(File.separator)) {
      cloudTestDataFolder = cloudTestDataFolder + File.separator;
    }

    hbaseSnapshotDir = cloudTestDataFolder + "data/";
    UUID test_uuid = UUID.randomUUID();
    restoreDir = cloudTestDataFolder + "restore/" + test_uuid;

    // Cloud Storage config
    GcpOptions gcpOptions = PipelineOptionsFactory.create().as(GcpOptions.class);
    gcpOptions.setProject(projectId);
    gcsUtil = new GcsUtil.GcsUtilFactory().create(gcpOptions);

    // Bigtable config
    connection = BigtableConfiguration.connect(projectId, instanceId);
    tableId = "test_" + UUID.randomUUID().toString();

    System.out.println("Setting up integration tests");

    String[] keys = new String[] {"1", "2", "3", "4", "5", "6", "7", "8", "9"};
    keySplits = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      keySplits[i] = keys[i].getBytes();
    }
  }

  private static String getTestProperty(String name) {
    return checkNotNull(System.getProperty(name), "Required property missing: " + name);
  }

  @After
  public void teardown() throws IOException {
    final List<GcsPath> paths = gcsUtil.expand(GcsPath.fromUri(restoreDir + "/*"));

    if (!paths.isEmpty()) {
      final List<String> pathStrs = new ArrayList<>();

      for (GcsPath path : paths) {
        pathStrs.add(path.toString());
      }
      this.gcsUtil.remove(pathStrs);
    }

    connection.close();

    // delete test table
    BigtableConfiguration.connect(projectId, instanceId)
        .getAdmin()
        .deleteTable(TableName.valueOf(tableId));
  }

  @Test
  public void testHBaseSnapshotImport() throws Exception {

    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      // Crete table
      System.out.println("DEBUG (create test table) ==>");
      TableName tableName = TableName.valueOf(tableId);
      HTableDescriptor descriptor = new HTableDescriptor(tableName);

      descriptor.addFamily(new HColumnDescriptor(CF));

      connection.getAdmin().createTable(descriptor, SnapshotTestingUtils.getSplitKeys());

      // Start import
      System.out.println("DEBUG (import snapshot) ==>");
      DataflowPipelineOptions importPipelineOpts =
          PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      importPipelineOpts.setRunner(DataflowRunner.class);
      importPipelineOpts.setGcpTempLocation(dataflowStagingLocation);
      importPipelineOpts.setNumWorkers(1);
      importPipelineOpts.setProject(projectId);
      importPipelineOpts.setRegion(region);

      ImportJobFromHbaseSnapshot.ImportOptions importOpts =
          importPipelineOpts.as(ImportJobFromHbaseSnapshot.ImportOptions.class);
      // setup GCP and bigtable
      importOpts.setBigtableProject(StaticValueProvider.of(projectId));
      importOpts.setBigtableInstanceId(StaticValueProvider.of(instanceId));
      importOpts.setBigtableTableId(StaticValueProvider.of(tableId));
      importOpts.setBigtableAppProfileId(null);

      // setup Hbase snapshot info
      importOpts.setGcsProject(projectId);
      importOpts.setHbaseRootDir(hbaseSnapshotDir);
      importOpts.setRestoreDir(restoreDir);
      importOpts.setSnapshotName(TEST_SNAPSHOT_NAME);

      // run pipeline
      State state = ImportJobFromHbaseSnapshot.buildPipeline(importOpts).run().waitUntilFinish();
      Assert.assertEquals(State.DONE, state);

      // check data in bigtable
      BigtableTableUtils destTable = new BigtableTableUtils(connection, tableId, CF);
      Assert.assertEquals(
          100 /* There are 100 rows in test snapshot*/,
          destTable.readAllCellsFromTable().toArray().length);

      // TODO(vermas2012): Add more validations after this.
    }
  }
}
