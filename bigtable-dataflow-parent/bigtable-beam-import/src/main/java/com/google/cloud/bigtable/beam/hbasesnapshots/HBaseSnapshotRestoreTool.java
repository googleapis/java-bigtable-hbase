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
 *
 *
 * <pre>
 *    java  -Dproject=$PROJECT \
 *    -DhbaseSnapshotSourceDir=gs://HBASE_EXPORT_ROOT_PATH/data \
 *    -Dsnapshots=$SNAPSHOT \
 *    -Dregion=$REGION \
 *    -DrestorePath=gs://HBASE_EXPORT_ROOT_PATH/restore \
 *     -jar bigtable-dataflow-parent/bigtable-beam-import/target/bigtable-beam-import-2.12.1-shaded.jar  \
 *     restoresnapshot
 * </pre>
 */
@InternalExtensionOnly
public class HBaseSnapshotRestoreTool {
  private static final Log LOG = LogFactory.getLog(HBaseSnapshotRestoreTool.class);

  @VisibleForTesting
  static final String MISSING_SNAPSHOT_SOURCEPATH =
      "Source Path containing hbase snapshots must be specified.";

  @VisibleForTesting
  static final String MISSING_SNAPSHOT_NAMES =
      "Snapshots must be specified. Allowed values are '*' (indicating all snapshots under source path) or "
          + "'prefix*' (snapshots matching certain prefix) or 'snapshotname1:tablename1,snapshotname2:tablename2' "
          + "(comma seperated list of snapshots)";

  public static void main(String[] args) throws Exception {
    GcsOptions options = PipelineOptionsFactory.create().as(GcsOptions.class);
    options.setProject(System.getProperty("project"));

    ImportConfig importConfig =
        System.getProperty("importConfigFilePath") != null
            ? buildImportConfigFromConfigFile(System.getProperty("importConfigFilePath"))
            : buildImportConfigFromArgs(options);

    LOG.info(
        String.format(
            "SourcePath:%s, RestorePath:%s",
            importConfig.getSourcepath(), importConfig.getRestorepath()));

    Map<String, String> configurations =
        SnapshotUtils.getConfiguration(
            null, // invoke from a local machine without using dataflow
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
  static ImportConfig buildImportConfigFromArgs(GcsOptions gcsOptions)
      throws IOException {
    Preconditions.checkArgument(
        System.getProperty("hbaseSnapshotSourceDir") != null, MISSING_SNAPSHOT_SOURCEPATH);
    Preconditions.checkArgument(System.getProperty("snapshots") != null, MISSING_SNAPSHOT_NAMES);

    Map<String, String> snapshots =
        SnapshotUtils.isRegex(System.getProperty("snapshots"))
            ? SnapshotUtils.getSnapshotsFromSnapshotPath(
                System.getProperty("hbaseSnapshotSourceDir"),
                gcsOptions.getGcsUtil(),
                System.getProperty("snapshots"))
            : SnapshotUtils.getSnapshotsFromString(System.getProperty("snapshots"));

    ImportConfig importConfig = new ImportConfig();
    importConfig.setSourcepath(System.getProperty("hbaseSnapshotSourceDir"));
    importConfig.setSnapshotsFromMap(snapshots);
    SnapshotUtils.setRestorePath(System.getProperty("restorePath"), importConfig);

    return importConfig;
  }

  @VisibleForTesting
  static ImportConfig buildImportConfigFromConfigFile(String configFilePath) throws Exception {
    Gson gson = new GsonBuilder().create();
    ImportConfig importConfig =
        gson.fromJson(SnapshotUtils.readFileContents(configFilePath), ImportConfig.class);
    Preconditions.checkNotNull(importConfig.getSourcepath(), MISSING_SNAPSHOT_SOURCEPATH);
    Preconditions.checkNotNull(importConfig.getSnapshots(), MISSING_SNAPSHOT_NAMES);
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
        "RestoreSnapshot - sourcePath:{" + sourcePath + "} restorePath: {" + restorePath + "}");
    FileSystem fileSystem = sourcePath.getFileSystem(configuration);
    RestoreSnapshotHelper.copySnapshotForScanner(
        configuration, fileSystem, sourcePath, restorePath, snapshotConfig.getSnapshotName());
  }
}
