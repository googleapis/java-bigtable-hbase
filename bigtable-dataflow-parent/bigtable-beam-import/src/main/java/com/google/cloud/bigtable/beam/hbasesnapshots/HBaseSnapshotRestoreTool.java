/*
 * Copyright 2024 Google LLC
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

import com.google.api.core.InternalExtensionOnly;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.ImportConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;

/**
 * Tool to restore HBase snapshots in GCS for scanning. This tool runs locally (without Dataflow)
 * and copies snapshot files to a restore path, resolving HLinks and References so that they can be
 * read by a scanner.
 *
 * <p>Execute the following command to run the tool directly using system properties:
 *
 * <pre>
 * {@code mvn compile exec:java \
 *    -Dexec.mainClass=com.google.cloud.bigtable.beam.hbasesnapshots.HBaseSnapshotRestoreTool \
 *    -Dproject=[PROJECT_ID] \
 *    -DhbaseSnapshotSourceDir=gs://[BUCKET]/[HBASE_EXPORT_ROOT_PATH]/data \
 *    -Dsnapshots=[SNAPSHOT_NAMES] \
 *    -DrestorePath=gs://[BUCKET]/[HBASE_EXPORT_ROOT_PATH]/restore
 * }
 * </pre>
 *
 * <p>Alternatively, you can provide a path to a JSON configuration file:
 *
 * <pre>
 * {@code mvn compile exec:java \
 *    -Dexec.mainClass=com.google.cloud.bigtable.beam.hbasesnapshots.HBaseSnapshotRestoreTool \
 *    -Dproject=[PROJECT_ID] \
 *    -DimportConfigFilePath=[PATH_TO_JSON_CONFIG]
 * }
 * </pre>
 *
 * <p>The JSON configuration file should have the following format:
 *
 * <pre>
 * {
 *   "sourcepath": "gs://[BUCKET]/[HBASE_EXPORT_ROOT_PATH]/data",
 *   "restorepath": "gs://[BUCKET]/[HBASE_EXPORT_ROOT_PATH]/restore",
 *   "snapshots": {
 *     "snapshot1": "table1",
 *     "snapshot2": "table2"
 *   }
 * }
 * </pre>
 */
@InternalExtensionOnly
public class HBaseSnapshotRestoreTool {
  private static final Log LOG = LogFactory.getLog(HBaseSnapshotRestoreTool.class);

  private static final String PROJECT_PROPERTY = "project";
  private static final String IMPORT_CONFIG_FILE_PATH_PROPERTY = "importConfigFilePath";
  private static final String HBASE_SNAPSHOT_SOURCE_DIR_PROPERTY = "hbaseSnapshotSourceDir";
  private static final String SNAPSHOTS_PROPERTY = "snapshots";
  private static final String RESTORE_PATH_PROPERTY = "restorePath";

  public static void main(String[] args) throws Exception {
    GcsOptions options = PipelineOptionsFactory.create().as(GcsOptions.class);
    String project = System.getProperty(PROJECT_PROPERTY);
    if (project != null) {
      options.setProject(project);
    }

    ImportConfig importConfig =
        System.getProperty(IMPORT_CONFIG_FILE_PATH_PROPERTY) != null
            ? buildImportConfigFromConfigFile(System.getProperty(IMPORT_CONFIG_FILE_PATH_PROPERTY))
            : buildImportConfigFromArgs(options);

    LOG.info(
        String.format(
            "SourcePath:%s, RestorePath:%s",
            importConfig.getSourcepath(), importConfig.getRestorepath()));

    Map<String, String> configurations =
        SnapshotUtils.getConfiguration(
            null, // invoke from a DirectRunner without using dataflow
            options.getProject(),
            importConfig.getSourcepath(),
            importConfig.getHbaseConfiguration());

    List<SnapshotConfig> snapshotConfigs =
        SnapshotUtils.buildSnapshotConfigs(
            importConfig.getSnapshots(),
            configurations,
            options.getProject(),
            importConfig.getSourcepath(),
            importConfig.getRestorepath());

    for (SnapshotConfig config : snapshotConfigs) {
      restoreSnapshot(config);
    }
  }

  @VisibleForTesting
  static ImportConfig buildImportConfigFromArgs(GcsOptions gcsOptions) throws IOException {
    String sourceDir = System.getProperty(HBASE_SNAPSHOT_SOURCE_DIR_PROPERTY);
    String snapshotsProperty = System.getProperty(SNAPSHOTS_PROPERTY);
    Map<String, String> snapshots = null;
    if (snapshotsProperty != null) {
      snapshots =
          (sourceDir != null && SnapshotUtils.isRegex(snapshotsProperty))
              ? SnapshotUtils.getSnapshotsFromSnapshotPath(
                  sourceDir, gcsOptions.getGcsUtil(), snapshotsProperty)
              : SnapshotUtils.getSnapshotsFromString(snapshotsProperty);
    }

    ImportConfig importConfig = new ImportConfig();
    importConfig.setSourcepath(sourceDir);
    if (snapshots != null) {
      importConfig.setSnapshotsFromMap(snapshots);
    }
    importConfig.validate();
    SnapshotUtils.setRestorePath(System.getProperty(RESTORE_PATH_PROPERTY), importConfig);

    return importConfig;
  }

  @VisibleForTesting
  static ImportConfig buildImportConfigFromConfigFile(String configFilePath) throws Exception {
    Gson gson = new GsonBuilder().create();
    ImportConfig importConfig =
        gson.fromJson(SnapshotUtils.readFileContents(configFilePath), ImportConfig.class);
    Preconditions.checkNotNull(importConfig, "ImportConfig parsed from file cannot be null.");
    importConfig.validate();
    SnapshotUtils.setRestorePath(importConfig.getRestorepath(), importConfig);
    return importConfig;
  }

  @VisibleForTesting
  /**
   * Creates a copy of Snasphsot from the source path into restore path.
   *
   * @param snapshotConfig - Snapshot Configuration
   * @throws IOException
   */
  static void restoreSnapshot(SnapshotConfig snapshotConfig) throws IOException {
    Path sourcePath = snapshotConfig.getSourcePath();
    Path restorePath = snapshotConfig.getRestorePath();
    Configuration configuration = snapshotConfig.getConfiguration();
    LOG.info(
        String.format("RestoreSnapshot - sourcePath:%s restorePath: %s", sourcePath, restorePath));
    FileSystem fileSystem = sourcePath.getFileSystem(configuration);
    if (fileSystem.exists(restorePath)) {
      LOG.info(
          String.format(
              "Restore path %s already exists, deleting it for idempotency", restorePath));
      fileSystem.delete(restorePath, true);
    }
    RestoreSnapshotHelper.copySnapshotForScanner(
        configuration, fileSystem, sourcePath, restorePath, snapshotConfig.getSnapshotName());
  }
}
