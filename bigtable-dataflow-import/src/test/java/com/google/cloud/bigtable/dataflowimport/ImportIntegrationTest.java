/*
 * Copyright (C) 2015 Google Inc.
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.dataflowimport.testing.BigtableTableUtils.BigtableTableUtilsFactory;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.dataflowimport.testing.BigtableTableUtils;
import com.google.cloud.bigtable.dataflowimport.testing.SequenceFileIoUtils;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for importing HBase Sequence Files into Cloud Bigtable.
 *
 * <p>Three test cases are included:
 * <ul>
 *   <li>Sequence File in latest format on local file system.
 *   <li>Sequence File in latest format on Google Cloud Storage (GCS).
 *   <li>Sequence File in HBase 0.94 format on GCS.
 * </ul>
 *
 * <p>To run this test, the following system properties must be defined:
 * <ul>
 *   <li>google.bigtable.project.id
 *   <li>google.bigtable.cluster.name
 *   <li>google.bigtable.zone.name
 *   <li>google.dataflow.stagingLocation
 *   <li>google.cloud.auth.service.account.email (for on-cloud tests)
 *   <li>cloud.test.data.folder (on-cloud test data location)
 * </ul>
 *
 * <p>Because {@link TestPipeline} is used for both local and on-cloud runs, tests
 * in this classes must run sequentially (in arbitrary order). This is because {@code TestPipeline}
 * use the {@code "runIntegrationTestOnService"} system property to choose the type of
 * pipeline to create.
 *
 * <p>It is possible to use regular pipelines in these tests, e.g.,
 * {@link com.google.cloud.dataflow.sdk.runners.DirectPipeline} or
 * {@link BlockingDataflowPipelineRunner}, but
 * <a href="https://cloud.google.com/dataflow/pipelines/testing-your-pipeline">Dataflow Testing
 * Guide</a> recommends {@code TestPipeline}.
 */
@RunWith(JUnit4.class)
public class ImportIntegrationTest {

  // Location of test data hosted on Google Cloud Storage, for on-cloud dataflow tests.
  private static final String CLOUD_TEST_DATA_FOLDER = "cloud.test.data.folder";
  private static final String HBASE_094_SUBFOLDER = "hbase094";
  private static final String HBASE_CURRENT_SUBFOLDER = "post-hbase094";

  // Column family name used in all test bigtables.
  private static final byte[] CF = Bytes.toBytes("column_family");

  /**
   * Specifies if test pipeline runs locally or on cloud. See {@link TestPipeline} for more
   * information.
   */
  private static final String RUN_INTEGRATION_TEST_ON_SERVICE = "runIntegrationTestOnService";
  /**
   * A json string containg configurations for the test pipeline. See {@link TestPipeline} for more
   * information.
   */
  private static final String TEST_PIPELINE_OPTIONS = "dataflowOptions";
  // Full path of the gcs folder where dataflow jars are uploaded to.
  public static final String GOOGLE_DATAFLOW_STAGING_LOCATION = "google.dataflow.stagingLocation";

  private HBaseImportOptions commonOptions;
  private String cloudTestDataFolder;
  private BigtableTableUtilsFactory tableUtilsFactory;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    commonOptions = PipelineOptionsFactory.as(HBaseImportOptions.class);
    String projectId = checkNotNull(
        System.getProperty(BigtableOptionsFactory.PROJECT_ID_KEY),
        "Required property missing: " + BigtableOptionsFactory.PROJECT_ID_KEY);
    commonOptions.setBigtableProjectId(projectId);
    commonOptions.setProject(projectId); // Use Bigtable project as dataflow project.
    commonOptions.setBigtableClusterId(
        checkNotNull(System.getProperty(BigtableOptionsFactory.CLUSTER_KEY),
            "Required property missing: " + BigtableOptionsFactory.CLUSTER_KEY));
    commonOptions.setBigtableZoneId(
        checkNotNull(System.getProperty(BigtableOptionsFactory.ZONE_KEY),
            "Required property missing: " + BigtableOptionsFactory.ZONE_KEY));
    commonOptions.setStagingLocation(
        checkNotNull(System.getProperty(GOOGLE_DATAFLOW_STAGING_LOCATION),
            "Required property missing: " + GOOGLE_DATAFLOW_STAGING_LOCATION));
    commonOptions.setMaxNumWorkers(1);

    cloudTestDataFolder =
        checkNotNull(System.getProperty(CLOUD_TEST_DATA_FOLDER), CLOUD_TEST_DATA_FOLDER);
    if (!cloudTestDataFolder.endsWith(File.separator)) {
      cloudTestDataFolder = cloudTestDataFolder + File.separator;
    }
    tableUtilsFactory = BigtableTableUtilsFactory.from(commonOptions);
  }

  @After
  public void teardown() throws IOException {
    if (tableUtilsFactory != null) {
      tableUtilsFactory.close();
    }
  }

  private HBaseImportOptions createImportOptions(
      String importFilePath, String tableName, boolean isHBase094Format) {
    HBaseImportOptions importOptions = commonOptions.as(HBaseImportOptions.class);
    importOptions.setFilePattern(importFilePath);
    importOptions.setBigtableTableId(tableName);
    importOptions.setHBase094DataFormat(isHBase094Format);
    if (importFilePath.startsWith("gs://")) {
      importOptions.setRunner(BlockingDataflowPipelineRunner.class);
    } else {
      importOptions.setRunner(DirectPipelineRunner.class);
    }
    return importOptions;
  }

  @Test
  public void testImportSequenceFile_fromLocalFs() throws Exception {
    String tableId = "test_" + UUID.randomUUID().toString();
    File sequenceFile = tmpFolder.newFile(tableId);
    Set<? extends Cell> expected = getTestData();
    SequenceFileIoUtils.createFileWithData(sequenceFile, expected);

    HBaseImportOptions options = createImportOptions(
        sequenceFile.getPath(), tableId, false /** isHBase094Format **/);
    try (BigtableTableUtils tableUtils = tableUtilsFactory.createBigtableTableUtils(
        tableId, Bytes.toString(CF))) {
      tableUtils.createEmptyTable();
      assertEquals(Collections.EMPTY_SET, tableUtils.readAllCellsFromTable());
      doImport(options);
      assertCellSetsEqual(expected, tableUtils.readAllCellsFromTable());
    }
  }

  /**
   * Verifies that a Sequence File on Google Cloud Storage (GCS) can be imported
   * into Google Cloud Bigtable.
   *
   * <p>This method creates a local Sequence File, copies it to the stagingLocation folder
   * on GCS, then imports the GCS file. The GCS file is not removed by post-test cleanup.
   */
  @Test
  public void testImportSequenceFile_fromGcs() throws Exception {
    runImportFileTestFromGcs(false /** isHBase094Format **/);
  }

  @Test
  public void testImportSequenceFile_fromGcsWithHBase094Format() throws Exception {
    runImportFileTestFromGcs(true /** isHBase094Format **/);
  }

  private void runImportFileTestFromGcs(boolean isHBase094Format) throws Exception {
    String tableId = "test_" + UUID.randomUUID().toString();
    // For this test input files already exist on GCS. getTestData() returns
    // all data that we know are in the file. See java doc for that method for more information.
    // TODO Generate and upload input sequence files for on-cloud tests and remove once done.
    Set<? extends Cell> expected = getTestData();

    HBaseImportOptions options = createImportOptions(
        getCloudTestDataFolder(isHBase094Format), tableId, isHBase094Format);
    options.setFilePattern(options.getFilePattern());
    try (BigtableTableUtils tableUtils = tableUtilsFactory.createBigtableTableUtils(
        tableId, Bytes.toString(CF))) {
      tableUtils.createEmptyTable();
      assertEquals(Collections.EMPTY_SET, tableUtils.readAllCellsFromTable());
      doImport(options);
      assertCellSetsEqual(expected, tableUtils.readAllCellsFromTable());
    }
  }

  @Test
  public void testImportSequenceFile_currentFormatWithHBase094Deserializer() throws Exception {
    String tableId = "test_" + UUID.randomUUID().toString();
    File sequenceFile = tmpFolder.newFile(tableId);
    SequenceFileIoUtils.createFileWithData(sequenceFile, getTestData());

    HBaseImportOptions options = createImportOptions(
        sequenceFile.getPath(), tableId, true /** isHBase094Format **/);
    options.setFilePattern(options.getFilePattern());
    try (BigtableTableUtils tableUtils = tableUtilsFactory.createBigtableTableUtils(
        tableId, Bytes.toString(CF))) {
      assertFalse(tableUtils.isTableExists());
      tableUtils.createEmptyTable();
      try {
        doImport(options);
        fail("Expecting failure.");
      } catch (Throwable expected) {
        // In HBase 0.94 and earlier row data was serialized using length-value encoding.
        // When a 0.94 deserializer is used on a newer file, the error dependends on the data
        // in file. So far we've seen EOFException and OutOfMemoryError.
      }
    }
    // Verify table is removed.
    assertFalse(tableUtilsFactory.createBigtableTableUtils(
        tableId, Bytes.toString(CF)).isTableExists());
  }

  @Test
  public void testImportSequenceFile_HBase094FormatWithCurrentDeserializer() throws Exception {
    String tableId = "test_" + UUID.randomUUID().toString();

    // Use data on GCS because we cannot programmatically create input in old format.
    HBaseImportOptions options = createImportOptions(
        getCloudTestDataFolder(true), tableId, false  /** isHBase094Format **/);
    options.setFilePattern(options.getFilePattern());
    try (BigtableTableUtils tableUtils = tableUtilsFactory.createBigtableTableUtils(
        tableId, Bytes.toString(CF))) {
      tableUtils.createEmptyTable();
      doImport(options);
      assertCellSetsEqual(Collections.EMPTY_SET, tableUtils.readAllCellsFromTable());
    }
  }

  private String getCloudTestDataFolder(boolean isHBase094Format) {
    return cloudTestDataFolder + (isHBase094Format ? HBASE_094_SUBFOLDER : HBASE_CURRENT_SUBFOLDER);
  }

  /**
   * Returns test data for import.
   *
   * <p>Currently for on-cloud tests, test sequence files are manually created and uploaded
   * to Google Cloud Storage. If this method changes, the test files must be updated. The files
   * currently in use are created as follows:
   *
   *<p>First, run the following commands in HBase shell:
   * <pre>
   *   create 'integration-test', { NAME => 'column_family', VERSIONS=>5}
   *   put 'integration-test', 'row_key_1', 'column_family:col1', 'v1', 1
   *   put 'integration-test', 'row_key_1', 'column_family:col1', 'v2', 2
   *   put 'integration-test', 'row_key_2', 'column_family:col2', 'v3', 1
   *   put 'integration-test', 'row_key_2', 'column_family:col2', 'v4', 3
   * </pre>
   *
   * <p>Next, run the export command in the HBase home directory:
   * <pre>
   *   bin/hbase org.apache.hadoop.hbase.mapreduce.Export integration-test TARGET_DIR 5
   * </pre>
   */
  private Set<? extends Cell> getTestData() throws Exception {
    final byte[] rowKey1 = Bytes.toBytes("row_key_1");
    final byte[] rowKey2 = Bytes.toBytes("row_key_2");
    final byte[] col1 = Bytes.toBytes("col1");
    final byte[] col2 = Bytes.toBytes("col2");
    Set<? extends Cell> keyValues = Sets.newHashSet(
        new KeyValue(rowKey1, CF, col1, 1L, Bytes.toBytes("v1")),
        new KeyValue(rowKey1, CF, col1, 2L, Bytes.toBytes("v2")),
        new KeyValue(rowKey2, CF, col2, 1L, Bytes.toBytes("v3")),
        new KeyValue(rowKey2, CF, col2, 3L, Bytes.toBytes("v4")));
    return keyValues;
  }

  private void doImport(HBaseImportOptions options) {
    // Configure the TestPipeline for on- or off-cloud service.
    if (options.getFilePattern().startsWith("gs://")) {
      // Pipeline should be staged on cloud.
      System.setProperty(RUN_INTEGRATION_TEST_ON_SERVICE, "true");
      System.setProperty(TEST_PIPELINE_OPTIONS,
          String.format("[ \"%s=%s\", \"%s=%s\", \"%s=%s\" ]",
              "--runner", BlockingDataflowPipelineRunner.class.getSimpleName(),
              "--project", options.getProject(),
              "--stagingLocation", options.getStagingLocation()));
    } else {
      // Pipeline should run locally.
      System.clearProperty(RUN_INTEGRATION_TEST_ON_SERVICE);
    }

    Pipeline p = CloudBigtableIO.initializeForWrite(TestPipeline.create());
    p
        .apply("ReadSequenceFile", Read.from(HBaseImportIO.createSource(options)))
        .apply("ConvertResultToMutations", HBaseImportIO.transformToMutations())
        .apply("WriteToTable", CloudBigtableIO.writeToTable(
            CloudBigtableTableConfiguration.fromCBTOptions(options)));

    p.run();
  }

  /**
   * Compare two {@link Set}s of cells. Cells are converted to {@link KeyValue} if necessary
   * to take advantage the {@link KeyValue#equals} method.
   */
  private static void assertCellSetsEqual(
      Set<? extends Cell> expected, Set<? extends Cell> actual) {
    Function<Cell, KeyValue> keyValueFunc = new Function<Cell, KeyValue>() {
          @Override
          public KeyValue apply(Cell cell) {
            return cell instanceof KeyValue ? (KeyValue) cell : new KeyValue(cell);
          }
        };
    assertEquals(Sets.newHashSet(Iterables.transform(expected, keyValueFunc)),
        Sets.newHashSet(Iterables.transform(actual, keyValueFunc)));
  }
}
