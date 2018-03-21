/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.beam.sequencefiles;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.beam.sequencefiles.testing.BigtableTableUtils;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class EndToEndIT {
  // Location of test data hosted on Google Cloud Storage, for on-cloud dataflow tests.
  private static final String CLOUD_TEST_DATA_FOLDER = "cloud.test.data.folder";

  // Column family name used in all test bigtables.
  private static final String CF = "column_family";

  // Full path of the gcs folder where dataflow jars are uploaded to.
  private static final String GOOGLE_DATAFLOW_STAGING_LOCATION = "google.dataflow.stagingLocation";


  private Connection connection;
  private String projectId;
  private String instanceId;
  private String tableId;

  private GcsUtil gcsUtil;
  private String cloudTestDataFolder;
  private String dataflowStagingLocation;
  private String workDir;

  @Before
  public void setup() throws Exception {
    projectId = getTestProperty(BigtableOptionsFactory.PROJECT_ID_KEY);
    instanceId = getTestProperty(BigtableOptionsFactory.INSTANCE_ID_KEY);

    dataflowStagingLocation = getTestProperty(GOOGLE_DATAFLOW_STAGING_LOCATION);

    cloudTestDataFolder = getTestProperty(CLOUD_TEST_DATA_FOLDER);
    if (!cloudTestDataFolder.endsWith(File.separator)) {
      cloudTestDataFolder = cloudTestDataFolder + File.separator;
    }

    // GCS config
    GcpOptions gcpOptions = PipelineOptionsFactory.create().as(GcpOptions.class);
    gcpOptions.setProject(projectId);
    gcsUtil = new GcsUtilFactory().create(gcpOptions);

    workDir = cloudTestDataFolder + "exports/" + UUID.randomUUID();

    // Bigtable config
    connection = BigtableConfiguration.connect(projectId, instanceId);
    tableId = "test_" + UUID.randomUUID().toString();
  }

  private static String getTestProperty(String name) {
    return checkNotNull(System.getProperty(name), "Required property missing: " + name);
  }

  @After
  public void teardown() throws IOException {
    final List<GcsPath> paths = gcsUtil.expand(GcsPath.fromUri(workDir + "/*"));

    if (!paths.isEmpty()) {
      final List<String> pathStrs = new ArrayList<>();

      for (GcsPath path : paths) {
        pathStrs.add(path.toString());
      }
      this.gcsUtil.remove(pathStrs);
    }

    connection.close();
  }

  @Test
  public void testExportImport() throws Exception {
    // Create a table, populate it & export it
    final List<Put> testData = Arrays.asList(
        new Put(Bytes.toBytes("row_key_1"))
            .addColumn(CF.getBytes(), "col1".getBytes(), 1L, "v1".getBytes())
            .addColumn(CF.getBytes(), "col1".getBytes(), 2L, "v2".getBytes()),
        new Put(Bytes.toBytes("row_key_2"))
            .addColumn(CF.getBytes(), "col2".getBytes(), 1L, "v3".getBytes())
            .addColumn(CF.getBytes(), "col2".getBytes(), 3L, "v4".getBytes())
    );

    final Set<Cell> flattenedTestData = Sets.newHashSet();
    for (Put put : testData) {
      for (List<Cell> cells : put.getFamilyCellMap().values()) {
        flattenedTestData.addAll(cells);
      }
    }

    // Create a table, populate it and export the data
    try (BigtableTableUtils srcTable = new BigtableTableUtils(connection, tableId, CF)) {
      srcTable.createEmptyTable();

      // Populate the source table
      try (BufferedMutator bufferedMutator = srcTable.getConnection()
          .getBufferedMutator(TableName.valueOf(tableId))) {
        bufferedMutator.mutate(testData);
      }

      // Export the data
      DataflowPipelineOptions pipelineOpts = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      pipelineOpts.setRunner(DataflowRunner.class);
      pipelineOpts.setGcpTempLocation(dataflowStagingLocation);
      pipelineOpts.setNumWorkers(1);
      pipelineOpts.setProject(projectId);

      ExportOptions exportOpts = pipelineOpts.as(ExportOptions.class);
      exportOpts.setBigtableInstanceId(instanceId);
      exportOpts.setBigtableTableId(tableId);
      exportOpts.setDestinationPath(StaticValueProvider.of(workDir));

      State state = ExportJob.buildPipeline(exportOpts).run().waitUntilFinish();
      Assert.assertEquals(State.DONE, state);
    }

    // Import it back into a new table
    final String destTableId = tableId + "-verify";

    try (BigtableTableUtils destTable = new BigtableTableUtils(connection, destTableId, CF)) {
      destTable.createEmptyTable();

      DataflowPipelineOptions pipelineOpts = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      pipelineOpts.setRunner(DataflowRunner.class);
      pipelineOpts.setGcpTempLocation(dataflowStagingLocation);
      pipelineOpts.setNumWorkers(1);
      pipelineOpts.setProject(projectId);

      ImportJob.ImportOptions importOpts = pipelineOpts.as(ImportJob.ImportOptions.class);
      importOpts.setBigtableInstanceId(instanceId);
      importOpts.setBigtableTableId(destTableId);
      importOpts.setSourcePattern(StaticValueProvider.of(workDir + "/part-*"));

      State state = ImportJob.buildPipeline(importOpts).run().waitUntilFinish();
      Assert.assertEquals(State.DONE, state);

      // Now make sure that it correctly imported
      Assert.assertEquals(flattenedTestData, destTable.readAllCellsFromTable());
    }
  }
}
