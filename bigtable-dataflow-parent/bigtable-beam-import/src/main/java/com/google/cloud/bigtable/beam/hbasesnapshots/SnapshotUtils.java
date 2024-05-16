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

import com.google.api.core.InternalApi;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.ImportConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains various helper methods to handle different tasks associated with importing of hbase
 * snapshots
 */
@InternalApi("For internal usage only")
public class SnapshotUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotUtils.class);
  private static final String DIRECTRUNNER = "DirectRunner";
  private static final String SNAPSHOT_MANIFEST_DIRECTORY = ".hbase-snapshot";
  private static final String GCS_SCHEME = "gs";
  private static final Sleeper sleeper = Sleeper.DEFAULT;
  private static final Object lock = new Object();
  private static Configuration hbaseConfiguration;

  private SnapshotUtils() {}

  private static String getParentDirectory(String hbaseSnapshotSourceDirectory) {
    URI hbaseSnapshotSourceUri;
    try {
      hbaseSnapshotSourceUri = new URI(hbaseSnapshotSourceDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format(
              "Invalid file path format for snapshot source directory: %s. Valid paths should have file scheme (gs://, file://)",
              hbaseSnapshotSourceDirectory));
    }

    if (hbaseSnapshotSourceUri.getScheme() != null
        && hbaseSnapshotSourceUri.getScheme().equals("gs")) // i.e Cloud Storage file system
    {
      return GcsPath.fromUri(hbaseSnapshotSourceUri).getParent().toString();
    }

    return new File(hbaseSnapshotSourceDirectory).getParent();
  }

  static String removeSuffixSlashIfExists(String directory) {
    return directory.endsWith("/") ? directory.substring(0, directory.length() - 1) : directory;
  }

  static String appendCurrentTimestamp(String directory) {
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneId.of("UTC"));
    return String.join("/", removeSuffixSlashIfExists(directory), formatter.format(Instant.now()));
  }

  static String getNamedDirectory(String sourceDirectory, String subFoldername) {
    String parentDirectory = removeSuffixSlashIfExists(getParentDirectory(sourceDirectory));
    return appendCurrentTimestamp(String.join("/", parentDirectory, subFoldername));
  }

  /** Builds the configuration combining default and user provided values. */
  static Map<String, String> getConfiguration(
      String runner,
      String project,
      String sourcedir,
      @Nullable Map<String, String> hbaseConfiguration) {
    Map<String, String> configurations = new HashMap<>();

    configurations.put(HConstants.HBASE_DIR, sourcedir);
    configurations.put(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configurations.put("fs.gs.project.id", project);
    configurations.put("google.cloud.auth.service.account.enable", "true");

    if (runner.equals(DIRECTRUNNER)) {
      // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication
      configurations.put("fs.gs.auth.type", "APPLICATION_DEFAULT");
    }

    // Update the default configurations with user supplied configuration values
    if (hbaseConfiguration != null) {
      configurations.putAll(hbaseConfiguration);
    }
    return configurations;
  }

  public static Configuration getHBaseConfiguration(Map<String, String> configurations) {
    if (hbaseConfiguration == null) {
      synchronized (lock) {
        if (hbaseConfiguration == null)
          hbaseConfiguration = createHbaseConfiguration(configurations);
      }
    }
    return hbaseConfiguration;
  }

  private static Configuration createHbaseConfiguration(Map<String, String> configurations) {
    LOG.info("Create HBase Configuration instance");
    Configuration hbaseConfiguration = HBaseConfiguration.create();
    for (Map.Entry<String, String> entry : configurations.entrySet())
      hbaseConfiguration.set(entry.getKey(), entry.getValue());
    return hbaseConfiguration;
  }

  /**
   * Build Snapshot Configurations.
   *
   * @param snapshotdetails - Snapshot details representing hbase snapshot name and corresponding
   *     bigtable table name.
   * @param configurations - BigTable Configurations
   * @param projectId - Google Cloud Project Id
   * @param sourcePath - Source path containing snapshot files
   * @param restorePath - Path snapshot files gets stored during job runs.
   * @return
   */
  static List<SnapshotConfig> buildSnapshotConfigs(
      List<ImportConfig.SnapshotInfo> snapshotdetails,
      Map<String, String> configurations,
      String projectId,
      String sourcePath,
      String restorePath) {

    return snapshotdetails.stream()
        .map(
            snapshotInfo ->
                SnapshotConfig.builder()
                    .setProjectId(projectId)
                    .setSourceLocation(sourcePath)
                    .setRestoreLocation(restorePath)
                    .setSnapshotName(snapshotInfo.getSnapshotName())
                    .setTableName(snapshotInfo.getbigtableTableName())
                    .setConfigurationDetails(configurations)
                    .build())
        .collect(Collectors.toList());
  }

  public static BackOff createBackOff(
      long backoffInitialIntervalInMillis, long backoffMaxIntervalInMillis, int maxRetries) {
    return FluentBackoff.DEFAULT
        .withInitialBackoff(Duration.millis(backoffInitialIntervalInMillis))
        .withMaxRetries(maxRetries)
        .withMaxBackoff(Duration.millis(backoffMaxIntervalInMillis))
        .backoff();
  }

  /**
   * Creates restore path based on the input configuration
   *
   * @param importConfig - Job Configuration
   */
  public static void setRestorePath(ImportConfig importConfig) {
    importConfig.setRestorepath(
        formatRestorePath(importConfig.getRestorepath(), importConfig.getSourcepath()));
  }

  /**
   * Parses the provided input to generate snapshot names and corresponding bigtable names. For
   * single snapshot names the following are valid formats: If both Snapshotname and bigtablename
   * are same then only snapshotname can be provided If bigtablename is different then should be
   * provided in the format snapshotname:bigtablename Multiple snapshots can be provided in
   * snapshot1:table1,snapshot2:table2 format or snapshot1,snapshot2 format
   *
   * @param snapshotNames - Snapshot names and corresponding bigtable table names.
   */
  public static Map<String, String> getSnapshotsFromString(String snapshotNames) {
    Map<String, String> snapshots = new HashMap<>();
    for (String snapshotInfo : snapshotNames.split(",")) {
      String[] snapshotWithTableName = snapshotInfo.split(":");
      if (snapshotWithTableName.length == 2)
        snapshots.put(snapshotWithTableName[0], snapshotWithTableName[1]);
      else if (snapshotWithTableName.length == 1)
        snapshots.put(snapshotWithTableName[0], snapshotWithTableName[0]);
      else
        throw new IllegalArgumentException(
            "Invalid specification format for snapshots. Expected format is snapshot1:table1,snapshot2:table2");
    }
    return snapshots;
  }

  public static String formatRestorePath(String providedPath, String hbaseSnapshotsPath) {
    return providedPath == null
        ? SnapshotUtils.getNamedDirectory(hbaseSnapshotsPath, "restore")
        : SnapshotUtils.appendCurrentTimestamp(providedPath);
  }

  /**
   * Read list of Snapshot names from Snapshot Source Path
   *
   * @param importSnapshotpath - Path representing the snapshot source directory
   * @param gcsUtil - GCS Instance
   * @param prefix - Specific prefix to be matched or '*' for all files.
   * @return
   * @throws IOException
   */
  public static Map<String, String> getSnapshotsFromSnapshotPath(
      String importSnapshotpath, GcsUtil gcsUtil, String prefix) throws IOException {

    importSnapshotpath =
        Joiner.on("/")
            .join(removeSuffixSlashIfExists(importSnapshotpath), SNAPSHOT_MANIFEST_DIRECTORY);
    // Build GCS path from given string e.g:
    // gs://sym-bucket/snapshots/20220309230526/.hbase-snapshot
    GcsPath gcsPath = GcsPath.fromUri(importSnapshotpath);
    // LOG.info("GCS Path:" + gcsPath + ";Object:" + gcsPath.getObject());
    Map<String, String> snapshots = new HashMap<>();

    List<StorageObject> objects =
        gcsUtil.listObjects(gcsPath.getBucket(), gcsPath.getObject(), null).getItems();
    if (objects == null)
      throw new IllegalStateException(
          String.format("Snapshot path %s does not contain any snapshots", importSnapshotpath));

    // Build a pattern for object portion e.g if path is
    // gs://sym-bucket/snapshots/20220309230526/.hbase-snapshot
    // the object portion would be snapshots/60G/20220309230526/.hbase-snapshot
    Pattern pathPattern = Pattern.compile(String.format("%s/(.+?/)", gcsPath.getObject()));
    Pattern prefixPattern = prefix.equals("*") ? null : Pattern.compile(prefix);
    Matcher pathMatcher = null;
    String snapshotName = null;
    for (StorageObject object : objects) {
      pathMatcher = pathPattern.matcher(object.getId());
      if (pathMatcher.find()) {
        // Group 1 represents the snapshot directory name along with suffix slash (e.g: snapshot1/)
        snapshotName = pathMatcher.group(1).replace("/", "");
        if (prefix.equals("*") || prefixPattern.matcher(snapshotName).find())
          snapshots.put(snapshotName, snapshotName);
      }
    }

    return snapshots;
  }

  /**
   * Reads the contents of file
   *
   * @param filePath - Path of the file.
   * @return
   * @throws IOException
   */
  public static String readFileContents(String filePath) throws IOException {
    try (Reader reader =
        Channels.newReader(
            FileSystems.open(FileSystems.matchSingleFileSpec(filePath).resourceId()),
            StandardCharsets.UTF_8.name())) {
      return CharStreams.toString(reader);
    }
  }

  /**
   * Check if the given value contains any character in given meta characters list
   *
   * @param data - text value
   * @return
   */
  public static boolean isRegex(String data) {
    String[] metaChars = {"*", "+", "?"};
    return Arrays.stream(metaChars).anyMatch(data::contains);
  }

  public static Sleeper getSleeper() {
    return sleeper;
  }
}
