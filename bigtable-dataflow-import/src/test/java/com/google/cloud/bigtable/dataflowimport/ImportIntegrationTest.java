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
import java.util.Arrays;
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
  private static final String DELETE_MARKER_SUBFOLDER = "post-hbase094-delete-marker";

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
        checkNotNull(System.getProperty(BigtableOptionsFactory.INSTANCE_ID_KEY),
            "Required property missing: " + BigtableOptionsFactory.INSTANCE_ID_KEY));
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
    runImportFileTestFromGcs(CloudTestDataType.CURRENT_FORMAT_NO_DELETE_MARKER, getTestData());
  }

  @Test
  public void testImportSequenceFile_fromGcsWithHBase094Format() throws Exception {
    runImportFileTestFromGcs(CloudTestDataType.HBASE_094_FORMAT_NO_DELETE_MARKER, getTestData());
  }

  // For on-cloud tests input files already exist on GCS. {@code expected} returns
  // all data that we know are in the file.
  // TODO Generate and upload input sequence files for on-cloud tests and remove once done.
  private void runImportFileTestFromGcs(CloudTestDataType dataType, Set<? extends Cell> expected)
      throws Exception {
    String tableId = "test_" + UUID.randomUUID().toString();
    HBaseImportOptions options = createImportOptions(
        getCloudTestDataFolder(dataType), tableId, dataType.isHBase094Format());
    options.setFilePattern(options.getFilePattern());
    try (BigtableTableUtils tableUtils = tableUtilsFactory.createBigtableTableUtils(
        tableId, dataType.getColumnFamilies())) {
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
        getCloudTestDataFolder(CloudTestDataType.HBASE_094_FORMAT_NO_DELETE_MARKER),
        tableId, false  /** isHBase094Format **/);
    options.setFilePattern(options.getFilePattern());
    try (BigtableTableUtils tableUtils = tableUtilsFactory.createBigtableTableUtils(
        tableId, Bytes.toString(CF))) {
      tableUtils.createEmptyTable();
      doImport(options);
      assertCellSetsEqual(Collections.<Cell> emptySet(), tableUtils.readAllCellsFromTable());
    }
  }

  /**
   * Verifies that a Sequence File with Delete Markers may be imported into Cloud Bigtable.
   *
   * <p>As of HBase 1.1.2, the delete-all-cells-in-family-by-exact-timestamp marker cannot be
   * exported to sequence file because the Serializer used by HBase does not recognize this
   * cell type. Therefore our test data file does not contain this marker.
   * This marker is still tested in unit tests.
   */
  @Test
  public void testImportSequeneFile_fromGcsWithDeleteMarker() throws Exception {
    final byte[] r1 = Bytes.toBytes("r1");
    final byte[] r2 = Bytes.toBytes("r2");
    final byte[] cf1 = Bytes.toBytes("cf1");
    final byte[] cf2 = Bytes.toBytes("cf2");
    final byte[] c1 = Bytes.toBytes("c1");
    final byte[] c2 = Bytes.toBytes("c2");
    final byte[] val = Bytes.toBytes("1");

    /*
     * The input sequence file consists of two rows.
     *
     * Data cells in the first row are added this way:
     *    Put put = new Put(r1);
     *    put.addColumn(cf1, c1, 1000, val);
     *    put.addColumn(cf1, c1, 1010, val);
     *    put.addColumn(cf1, c1, 1020, val);
     *    put.addColumn(cf1, c1, 1030, val);
     *    put.addColumn(cf1, c1, 1040, val);
     *    put.addColumn(cf1, c1, 1050, val);
     *    put.addColumn(cf1, c1, 980, val);
     *    put.addColumn(cf1, c1, 970, val);
     *    put.addColumn(cf1, c1, 990, val);
     *    put.addColumn(cf1, c2, 100, val);
     *    put.addColumn(cf2, c1, 990, val);
     *    put.addColumn(cf2, c2, 990, val);
     *    put.addColumn(cf2, c1, 1000, val);
     *    put.addColumn(cf2, c2, 1000, val);
     *    put.addColumn(cf2, c1, 1010, val);
     *    put.addColumn(cf2, c2, 1010, val);
     *    put.addColumn(cf2, c1, 1020, val);
     *    put.addColumn(cf2, c2, 1020, val);
     *    put.addColumn(cf2, c1, 1030, val);
     *    put.addColumn(cf2, c2, 1030, val);
     *    put.addColumn(cf2, c1, 1040, val);
     *    put.addColumn(cf2, c2, 1040, val);
     *    put.addColumn(cf2, c1, 1050, val);
     *    put.addColumn(cf2, c2, 1050, val);
     *
     * The Data cells in the second row are added this way:
     *     Put put2 = new Put(r2);
     *     put2.addColumn(cf1, c1, 1000, val);
     *
     * Three types of delete Markers are added to row 'r1' after all data cells are inserted.
     * The DeleteFamilyVersion marker is omitted.
     *     Delete delete = new Delete(r1)
     *         .addColumn(cf1, c1, 1045))    // No effect
     *         .addColumn(cf1, c1, 1040))    // Deletes one cell
     *         .addColumns(cf1, c1, 1000))   // Deletes all cells with ts <= 1000
     *         .addFamilyVersion(cf1, 1045)) // No effect
     *         .addFamilyVersion(cf2, 1040)) // Deletes two cells in cf2 with ts=1040
     *         .addFamily(cf2, 1000));       // Deletes everything with ts<= 1000 in cf2
     */
    Set<? extends Cell> expected = Sets.newHashSet(
        new KeyValue(r1, cf1, c1, 1050, val),
        new KeyValue(r1, cf1, c1, 1030, val),
        new KeyValue(r1, cf1, c1, 1020, val),
        new KeyValue(r1, cf1, c1, 1010, val),
        new KeyValue(r1, cf1, c2, 100, val),
        new KeyValue(r1, cf2, c1, 1050, val),
        new KeyValue(r1, cf2, c1, 1040, val),
        new KeyValue(r1, cf2, c1, 1030, val),
        new KeyValue(r1, cf2, c1, 1020, val),
        new KeyValue(r1, cf2, c1, 1010, val),
        new KeyValue(r1, cf2, c2, 1050, val),
        new KeyValue(r1, cf2, c2, 1040, val),
        new KeyValue(r1, cf2, c2, 1030, val),
        new KeyValue(r1, cf2, c2, 1020, val),
        new KeyValue(r1, cf2, c2, 1010, val),
        new KeyValue(r2, cf1, c1, 1000, val));
    runImportFileTestFromGcs(CloudTestDataType.CURRENT_FORMAT_WITH_DELETE_MARKER, expected);
  }

  private String getCloudTestDataFolder(CloudTestDataType dataType) {
    return cloudTestDataFolder + dataType.getDataSubfoler();
  }

  /**
   * Returns test data (that do not contain delete markers) for import. Delete marker test data are
   * created elsewhere.
   *
   * <p>Currently for on-cloud tests, test sequence files are manually created and uploaded
   * to Google Cloud Storage. If this method changes, the test files must be updated. The files
   * currently in use are created as follows:
   *
   *<p>First, run the following commands in HBase shell:
   * <pre>
   *   create 'integration-test', { NAME => 'column_family', VERSIONS=>5}
   *   put 'integration-test', 'row_key_1', 'column_family:col1', 'v2', 1
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
        new KeyValue(rowKey1, CF, col1, 1L, Bytes.toBytes("v2")),
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

  private enum CloudTestDataType {
    CURRENT_FORMAT_NO_DELETE_MARKER(HBASE_CURRENT_SUBFOLDER, false, Bytes.toString(CF)),
    HBASE_094_FORMAT_NO_DELETE_MARKER(HBASE_094_SUBFOLDER, true, Bytes.toString(CF)),
    CURRENT_FORMAT_WITH_DELETE_MARKER(DELETE_MARKER_SUBFOLDER, false, "cf1", "cf2");

    private final String dataSubfoler;
    private final boolean isHBase094Format;
    private final String[] columnFamilies;

    public String[] getColumnFamilies() {
      return Arrays.copyOf(columnFamilies, columnFamilies.length);
    }

    private CloudTestDataType(
        String dataSubfoler, boolean isHBase094Format, String ...columnFamilies) {
      this.dataSubfoler = dataSubfoler;
      this.isHBase094Format = isHBase094Format;
      this.columnFamilies = columnFamilies;
    }

    public String getDataSubfoler() {
      return dataSubfoler;
    }

    public boolean isHBase094Format() {
      return isHBase094Format;
    }
  }
}
