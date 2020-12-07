/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * End to end integration test for pipeline that import HBase snapshot data into Cloud Bigtable.
 * Run with:  mvn integration-test -PhbasesnapshotsIntegrationTest \
 * -Dgoogle.bigtable.project.id=google.com:cloud-bigtable-dev \
 * -Dgoogle.bigtable.instance.id=lichng-test \
 * -Dgoogle.dataflow.stagingLocation=gs://lichng-gcs/dataflow-test/staging \
 * -Dcloud.test.data.folder=gs://lichng-gcs/hbase/ \
 * -Dhbase.snapshot.folder=gs://lichng-gcs/hbase-export/
 */
public class EndToEndIT {

  private static final String TEST_PROJECT = "test_project";
  private static final String TEST_SNAPSHOT_DIR = "gs://test-bucket/hbase-export";
  private static final String TEST_SNAPSHOT_NAME = "test_snapshot";
  private static final String TEST_RESTORE_DIR = "gs://test-bucket/hbase-restore";

  // Location of test data hosted on Google Cloud Storage, for on-cloud dataflow tests.
  private static final String CLOUD_TEST_DATA_FOLDER = "cloud.test.data.folder";
  private static final String HBASE_SNAPSHOT_FOLDER = "hbase.snapshot.folder";

  // Column family name used in all test bigtables.
  private static final String CF = "column_family";

  // Full path of the Cloud Storage folder where dataflow jars are uploaded to.
  private static final String GOOGLE_DATAFLOW_STAGING_LOCATION = "google.dataflow.stagingLocation";

  private Connection connection;
  private String projectId;
  private String instanceId;
  private String tableId;

  private GcsUtil gcsUtil;
  private String cloudTestDataFolder;
  private String dataflowStagingLocation;
  private String workDir;

  // HBase setup
  private HBaseTestingUtility testUtility;
  private String hbaseSnapshotName;
  private String hbaseSnapshotDir;
  private String restoreDir;

  @Before
  public void setup() throws Exception {
    projectId = getTestProperty(BigtableOptionsFactory.PROJECT_ID_KEY);
    instanceId = getTestProperty(BigtableOptionsFactory.INSTANCE_ID_KEY);

    dataflowStagingLocation = getTestProperty(GOOGLE_DATAFLOW_STAGING_LOCATION);

    cloudTestDataFolder = getTestProperty(CLOUD_TEST_DATA_FOLDER);
    if (!cloudTestDataFolder.endsWith(File.separator)) {
      cloudTestDataFolder = cloudTestDataFolder + File.separator;
    }
    hbaseSnapshotDir = getTestProperty(HBASE_SNAPSHOT_FOLDER);
    ;
    // Cloud Storage config
    GcpOptions gcpOptions = PipelineOptionsFactory.create().as(GcpOptions.class);
    gcpOptions.setProject(projectId);
    gcsUtil = new GcsUtil.GcsUtilFactory().create(gcpOptions);

    restoreDir = cloudTestDataFolder + "exports/" + UUID.randomUUID();

    // Bigtable config
    connection = BigtableConfiguration.connect(projectId, instanceId);
    tableId = "test_" + UUID.randomUUID().toString();

    // Set up test HBaseCluster
    // Create a HBaseMiniCluster for data preparation
    System.out.println("Setting up integration tests");
    Configuration conf =
        new HBaseSnapshotInputConfiguration(
                ValueProvider.StaticValueProvider.of(projectId),
                ValueProvider.StaticValueProvider.of(hbaseSnapshotDir),
                ValueProvider.StaticValueProvider.of(hbaseSnapshotName),
                ValueProvider.StaticValueProvider.of(restoreDir))
            .getHbaseConf();
    testUtility = new HBaseTestingUtility(conf);
    testUtility.startMiniCluster();
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
    try {
      testUtility.shutdownMiniCluster();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testHBaseSnapshotImport() throws Exception {}
}
