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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.beam.sequencefiles.testing.BigtableTableUtils;
import com.google.cloud.bigtable.beam.test_env.EnvSetup;
import com.google.cloud.bigtable.beam.test_env.TestProperties;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.truth.Correspondence;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EndToEndIT {
  /**
   * Correspondence helper to ensure that the cell values match as well. Byt default KeyValue#equals
   * only considers keys.
   */
  public static final Correspondence<Cell, Cell> CELL_EQUALITY =
      Correspondence.from(
          (Cell actual, Cell expected) ->
              CellUtil.equals(actual, expected) && CellUtil.matchingValue(actual, expected),
          "Cell equality");

  private static final String CF = "column_family";

  private TestProperties properties;
  private String workDir;
  private String outputDir;
  private String tableId;

  private Connection connection;

  private GcsUtil gcsUtil;

  @Before
  public void setup() throws Exception {
    EnvSetup.initialize();
    properties = TestProperties.fromSystem();
    workDir = properties.getTestWorkdir(UUID.randomUUID());
    outputDir = workDir + "output/";

    // TODO: use time based names to allow for GC
    tableId = "test_" + UUID.randomUUID();

    // Cloud Storage config
    GcpOptions gcpOptions = PipelineOptionsFactory.create().as(GcpOptions.class);
    properties.applyTo(gcpOptions);
    gcsUtil = new GcsUtil.GcsUtilFactory().create(gcpOptions);

    // Disable CSM to reduce noise in the test output
    Configuration config =
        BigtableConfiguration.configure(properties.getProjectId(), properties.getInstanceId());
    config.set(BigtableOptionsFactory.BIGTABLE_ENABLE_CLIENT_SIDE_METRICS, "false");

    // Bigtable config
    connection = BigtableConfiguration.connect(config);
    // TODO: support endpoints
  }

  @After
  public void teardown() throws IOException {
    final List<GcsPath> paths = gcsUtil.expand(GcsPath.fromUri(workDir + "*"));

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
      properties.applyTo(pipelineOpts);

      ExportOptions exportOpts = pipelineOpts.as(ExportOptions.class);
      exportOpts.setBigtableInstanceId(StaticValueProvider.of(properties.getInstanceId()));
      exportOpts.setBigtableTableId(StaticValueProvider.of(tableId));
      exportOpts.setDestinationPath(StaticValueProvider.of(outputDir));

      State state = ExportJob.buildPipeline(exportOpts).run().waitUntilFinish();
      assertThat(state).isEqualTo(State.DONE);
    }

    // Import it back into a new table
    final String destTableId = tableId + "-verify";

    try (BigtableTableUtils destTable = new BigtableTableUtils(connection, destTableId, CF)) {
      destTable.deleteTable();

      DataflowPipelineOptions createTablePipelineOpts =
          PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      properties.applyTo(createTablePipelineOpts);

      CreateTableHelper.CreateTableOpts createOpts =
          createTablePipelineOpts.as(CreateTableHelper.CreateTableOpts.class);
      createOpts.setBigtableProject(properties.getProjectId());
      createOpts.setBigtableInstanceId(properties.getInstanceId());
      createOpts.setBigtableTableId(destTableId);
      createOpts.setSourcePattern(outputDir + "part-*");
      createOpts.setFamilies(ImmutableList.of(CF));
      CreateTableHelper.createTable(createOpts);

      DataflowPipelineOptions importPipelineOpts =
          PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      properties.applyTo(importPipelineOpts);

      ImportJob.ImportOptions importOpts = importPipelineOpts.as(ImportJob.ImportOptions.class);
      importOpts.setBigtableProject(StaticValueProvider.of(properties.getProjectId()));
      importOpts.setBigtableInstanceId(StaticValueProvider.of(properties.getInstanceId()));
      importOpts.setBigtableTableId(StaticValueProvider.of(destTableId));
      // Have to set bigtableAppProfileId to null, otherwise importOpts will return a non-null
      // value.
      importOpts.setBigtableAppProfileId(null);
      importOpts.setSourcePattern(StaticValueProvider.of(outputDir + "part-*"));
      State state = ImportJob.buildPipeline(importOpts).run().waitUntilFinish();
      assertThat(state).isEqualTo(State.DONE);

      // Now make sure that it correctly imported
      assertThat(destTable.readAllCellsFromTable())
          .comparingElementsUsing(CELL_EQUALITY)
          .containsExactlyElementsIn(flattenedTestData);
    }
  }
}
