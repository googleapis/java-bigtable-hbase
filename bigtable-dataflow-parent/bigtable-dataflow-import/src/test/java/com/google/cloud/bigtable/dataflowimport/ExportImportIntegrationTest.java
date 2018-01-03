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
package com.google.cloud.bigtable.dataflowimport;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.dataflowimport.testing.BigtableTableUtils;
import com.google.cloud.bigtable.dataflowimport.testing.BigtableTableUtils.BigtableTableUtilsFactory;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class ExportImportIntegrationTest {

  // Location of test data hosted on Google Cloud Storage, for on-cloud dataflow tests.
  private static final String CLOUD_TEST_DATA_FOLDER = "cloud.test.data.folder";

  // Column family name used in all test bigtables.
  private static final byte[] CF = Bytes.toBytes("column_family");

  // Full path of the gcs folder where dataflow jars are uploaded to.
  public static final String GOOGLE_DATAFLOW_STAGING_LOCATION = "google.dataflow.stagingLocation";

  private HBaseExportOptions exportOptions;
  private BigtableTableUtilsFactory tableUtilsFactory;
  private GcsUtil gcsUtil;

  @Before
  public void setup() throws Exception {
    exportOptions = PipelineOptionsFactory.as(HBaseExportOptions.class);

    // TODO(igorbernstein2): Switch to TestPipeline and allow this test to run locally
    exportOptions.setRunner(BlockingDataflowPipelineRunner.class);
    exportOptions.setStagingLocation(getTestProperty(GOOGLE_DATAFLOW_STAGING_LOCATION));
    exportOptions.setProject(getTestProperty(BigtableOptionsFactory.PROJECT_ID_KEY));
    exportOptions.setMaxNumWorkers(1);

    exportOptions.setBigtableProjectId(getTestProperty(BigtableOptionsFactory.PROJECT_ID_KEY));
    exportOptions.setBigtableInstanceId(getTestProperty(BigtableOptionsFactory.INSTANCE_ID_KEY));
    exportOptions.setBigtableTableId("test_" + UUID.randomUUID().toString());

    String cloudTestDataFolder = getTestProperty(CLOUD_TEST_DATA_FOLDER);
    if (!cloudTestDataFolder.endsWith(File.separator)) {
      cloudTestDataFolder = cloudTestDataFolder + File.separator;
    }
    exportOptions.setDestination(cloudTestDataFolder + "exports/" + UUID.randomUUID());

    tableUtilsFactory = BigtableTableUtilsFactory.from(exportOptions);
    gcsUtil = new GcsUtilFactory().create(exportOptions);
  }

  private static String getTestProperty(String name) {
    return checkNotNull(System.getProperty(name), "Required property missing: " + name);
  }

  @After
  public void teardown() throws IOException {
    if (tableUtilsFactory != null) {
      tableUtilsFactory.close();
      tableUtilsFactory = null;
    }

    final List<GcsPath> paths = gcsUtil.expand(GcsPath.fromUri(exportOptions.getDestination() + "/*"));

    if (!paths.isEmpty()) {
      final List<String> pathStrs = new ArrayList<>();

      for (GcsPath path : paths) {
        pathStrs.add(path.toString());
      }
      // delete the folder as well
      pathStrs.add(exportOptions.getDestination() + "/");

      gcsUtil.remove(pathStrs);
    }
  }


  @Test
  public void testExportImport() throws IOException {
    // Create a table, populate it & export it
    final List<Put> testData = Arrays.asList(
        new Put(Bytes.toBytes("row_key_1"))
            .addColumn(CF, "col1".getBytes(), 1L, "v1".getBytes())
            .addColumn(CF, "col1".getBytes(), 2L, "v2".getBytes()),
        new Put(Bytes.toBytes("row_key_2"))
            .addColumn(CF, "col2".getBytes(), 1L, "v3".getBytes())
            .addColumn(CF, "col2".getBytes(), 3L, "v4".getBytes())
    );

    final Set<Cell> flattenedTestData = Sets.newHashSet();
    for (Put put : testData) {
      for (List<Cell> cells : put.getFamilyCellMap().values()) {
        flattenedTestData.addAll(cells);
      }
    }

    try (BigtableTableUtils srcTable = tableUtilsFactory
        .createBigtableTableUtils(exportOptions.getBigtableTableId(), Bytes.toString(CF))) {
      srcTable.createEmptyTable();

      // Populate the source table
      try (BufferedMutator bufferedMutator = srcTable.getConnection()
          .getBufferedMutator(TableName.valueOf(exportOptions.getBigtableTableId()))) {
        bufferedMutator.mutate(testData);
      }

      // Export the data
      HBaseExportJob.buildPipeline(exportOptions).run();
    }

    // Import it back into a new table
    final String destTableId = exportOptions.getBigtableTableId() + "-verify";

    try (BigtableTableUtils destTables = tableUtilsFactory
        .createBigtableTableUtils(destTableId, Bytes.toString(CF))) {
      destTables.createEmptyTable();

      final HBaseImportOptions importOptions = exportOptions.as(HBaseImportOptions.class);
      importOptions.setBigtableTableId(destTableId);
      importOptions.setFilePattern(exportOptions.getDestination());

      Pipeline p = CloudBigtableIO.initializeForWrite(Pipeline.create(importOptions));
      p
          .apply("ReadSequenceFile", Read.from(HBaseImportIO.createSource(importOptions)))
          .apply("ConvertResultToMutations", HBaseImportIO.transformToMutations())
          .apply("WriteToTable", CloudBigtableIO.writeToTable(
              CloudBigtableTableConfiguration.fromCBTOptions(importOptions)));
      p.run();

      // Now make sure that it correctly imported
      Assert.assertEquals(flattenedTestData, destTables.readAllCellsFromTable());
    }
  }
}
