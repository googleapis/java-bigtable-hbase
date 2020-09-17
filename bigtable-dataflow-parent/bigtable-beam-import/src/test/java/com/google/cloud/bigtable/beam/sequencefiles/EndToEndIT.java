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
package com.google.cloud.bigtable.beam.sequencefiles;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigtable.beam.TestHelper;
import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.beam.sequencefiles.testing.BigtableTableUtils;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
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

/**
 * Integration test for export/import table.
 *
 * <p>Mandatory parameters:
 *
 * <pre>
 *  -Dgoogle.bigtable.project.id=[bigtable project]
 *  -Dgoogle.bigtable.instance.id=[bigtable instance id]
 *  -Dgoogle.dataflow.gcsPath=gs://[google storage path]
 * </pre>
 */
@RunWith(JUnit4.class)
public class EndToEndIT {
  // Column family name used in all test bigtables.
  private static final String CF = "column_family";

  private static final String projectId = getTestProperty(BigtableOptionsFactory.PROJECT_ID_KEY);
  private static final String instanceId = getTestProperty(BigtableOptionsFactory.INSTANCE_ID_KEY);
  private static final String gcsPath = getTestProperty("google.dataflow.gcsPath");
  private static final String gcsWorkDir = gcsPath + "/" + UUID.randomUUID();
  private static final String stagingLocation = gcsWorkDir + "/staging";
  private static final String tempLocation = gcsWorkDir + "/temp";

  private Connection connection;
  private String tableId;

  @Before
  public void setup() {
    connection = BigtableConfiguration.connect(projectId, instanceId);
    tableId = "test_" + UUID.randomUUID().toString();
  }

  private static String getTestProperty(String name) {
    return checkNotNull(System.getProperty(name), "Required property missing: " + name);
  }

  @After
  public void teardown() throws IOException {
    TestHelper.init(projectId);
    TestHelper.cleanUpStorageFolder(stagingLocation + "/staging"); // SIC!
    TestHelper.cleanUpStorageFolder(tempLocation);
    connection.close();
  }

  @Test
  public void testExportImport() throws Exception {
    // Create a table, populate it & export it
    final List<Put> testData =
        Arrays.asList(
            new Put(Bytes.toBytes("row_key_1"))
                .addColumn(CF.getBytes(), "col1".getBytes(), 1L, "v1".getBytes())
                .addColumn(CF.getBytes(), "col1".getBytes(), 2L, "v2".getBytes()),
            new Put(Bytes.toBytes("row_key_2"))
                .addColumn(CF.getBytes(), "col2".getBytes(), 1L, "v3".getBytes())
                .addColumn(CF.getBytes(), "col2".getBytes(), 3L, "v4".getBytes()));

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
      try (BufferedMutator bufferedMutator =
          srcTable.getConnection().getBufferedMutator(TableName.valueOf(tableId))) {
        bufferedMutator.mutate(testData);
      }

      // Export the data
      DataflowPipelineOptions pipelineOpts =
          PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      pipelineOpts.setRunner(DataflowRunner.class);
      pipelineOpts.setGcpTempLocation(stagingLocation);
      pipelineOpts.setNumWorkers(1);
      pipelineOpts.setProject(projectId);

      ExportOptions exportOpts = pipelineOpts.as(ExportOptions.class);
      exportOpts.setBigtableInstanceId(StaticValueProvider.of(instanceId));
      exportOpts.setBigtableTableId(StaticValueProvider.of(tableId));
      exportOpts.setDestinationPath(StaticValueProvider.of(tempLocation));

      State state = ExportJob.buildPipeline(exportOpts).run().waitUntilFinish();
      Assert.assertEquals(State.DONE, state);
    }

    // Import it back into a new table
    final String destTableId = tableId + "-verify";

    try (BigtableTableUtils destTable = new BigtableTableUtils(connection, destTableId, CF)) {
      destTable.deleteTable();

      DataflowPipelineOptions createTablePipelineOpts =
          PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      createTablePipelineOpts.setRunner(DataflowRunner.class);
      createTablePipelineOpts.setProject(projectId);

      CreateTableHelper.CreateTableOpts createOpts =
          createTablePipelineOpts.as(CreateTableHelper.CreateTableOpts.class);
      createOpts.setBigtableProject(projectId);
      createOpts.setBigtableInstanceId(instanceId);
      createOpts.setBigtableTableId(destTableId);
      createOpts.setSourcePattern(tempLocation + "/part-*");
      createOpts.setFamilies(ImmutableList.of(CF));
      CreateTableHelper.createTable(createOpts);

      DataflowPipelineOptions importPipelineOpts =
          PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      importPipelineOpts.setRunner(DataflowRunner.class);
      importPipelineOpts.setGcpTempLocation(stagingLocation);
      importPipelineOpts.setNumWorkers(1);
      importPipelineOpts.setProject(projectId);

      ImportJob.ImportOptions importOpts = importPipelineOpts.as(ImportJob.ImportOptions.class);
      importOpts.setBigtableProject(StaticValueProvider.of(projectId));
      importOpts.setBigtableInstanceId(StaticValueProvider.of(instanceId));
      importOpts.setBigtableTableId(StaticValueProvider.of(destTableId));
      // Have to set bigtableAppProfileId to null, otherwise importOpts will return a non-null
      // value.
      importOpts.setBigtableAppProfileId(null);
      importOpts.setSourcePattern(StaticValueProvider.of(tempLocation + "/part-*"));
      State state = ImportJob.buildPipeline(importOpts).run().waitUntilFinish();
      Assert.assertEquals(State.DONE, state);

      // Now make sure that it correctly imported
      Assert.assertEquals(flattenedTestData, destTable.readAllCellsFromTable());
    }
  }
}
