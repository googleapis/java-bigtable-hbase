/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class representing the job configuration loading the different input values and combinations of
 * snapshot names (such as all snapshots or matching prefix or explicit names) provided.
 */
@InternalApi("For internal usage only")
public final class ImportConfig implements Serializable {
  private final long DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS = 5000; // 5 seconds
  private final long DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS = 3 * 60 * 1000; // 180 seconds
  private final int DEFAULT_BACKOFF_MAX_RETRIES = 3;

  @JsonAdapter(SnapshotInfoJsonAdapter.class)
  @SerializedName("snapshots")
  public List<SnapshotInfo> snapshotInfos;

  private String sourcepath;
  private String restorepath;
  private String runstatuspath;
  private long backoffInitialIntervalInMillis =
      DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS; // Defaults to 5 seconds
  private long backoffMaxIntervalInMillis = DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS; // 60 seconds
  private int backoffMaxretries = DEFAULT_BACKOFF_MAX_RETRIES;
  private Map<String, String> hbaseConfiguration;
  private Map<String, String> bigtableConfiguration;

  public void setSnapshotsFromMap(Map<String, String> snapshots) {
    this.snapshotInfos =
        snapshots.entrySet().stream()
            .map(entry -> new SnapshotInfo(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
  }

  public String getSourcepath() {
    return this.sourcepath;
  }

  public void setSourcepath(String sourcepath) {
    this.sourcepath = sourcepath;
  }

  public String getRestorepath() {
    return restorepath;
  }

  public void setRestorepath(String restorepath) {
    this.restorepath = restorepath;
  }

  public String getRunstatuspath() {
    return runstatuspath;
  }

  public void setRunstatuspath(String runstatuspath) {
    this.runstatuspath = runstatuspath;
  }

  public long getBackoffInitialIntervalInMillis() {
    return backoffInitialIntervalInMillis;
  }

  public void setBackoffInitialIntervalInMillis(long backoffInitialIntervalInMillis) {
    Preconditions.checkArgument(
        backoffInitialIntervalInMillis > 0,
        "backoffInitialIntervalInMillis must be greater than 0: %s",
        backoffInitialIntervalInMillis);
    Preconditions.checkArgument(
        backoffInitialIntervalInMillis <= this.backoffMaxIntervalInMillis,
        "backoffInitialIntervalInMillis (%s) must be less than or equal to"
            + " backoffMaxIntervalInMillis (%s)",
        backoffInitialIntervalInMillis,
        this.backoffMaxIntervalInMillis);
    this.backoffInitialIntervalInMillis = backoffInitialIntervalInMillis;
  }

  public long getBackoffMaxIntervalInMillis() {
    return this.backoffMaxIntervalInMillis;
  }

  public void setBackoffMaxIntervalInMillis(long backoffMaxIntervalInMillis) {
    Preconditions.checkArgument(
        backoffMaxIntervalInMillis > 0,
        "backoffMaxIntervalInMillis must be greater than 0: %s",
        backoffMaxIntervalInMillis);
    Preconditions.checkArgument(
        this.backoffInitialIntervalInMillis <= backoffMaxIntervalInMillis,
        "backoffInitialIntervalInMillis (%s) must be less than or equal to"
            + " backoffMaxIntervalInMillis (%s)",
        this.backoffInitialIntervalInMillis,
        backoffMaxIntervalInMillis);
    this.backoffMaxIntervalInMillis = backoffMaxIntervalInMillis;
  }

  public int getBackoffMaxretries() {
    return this.backoffMaxretries;
  }

  public void setBackoffMaxretries(int backoffMaxretries) {
    Preconditions.checkArgument(
        backoffMaxretries >= 0, "backoffMaxretries must be non-negative: %s", backoffMaxretries);
    this.backoffMaxretries = backoffMaxretries;
  }

  public void validate() {
    Preconditions.checkNotNull(
        sourcepath, "Source Path containing hbase snapshots must be specified.");
    Preconditions.checkArgument(
        !sourcepath.trim().isEmpty(), "Source Path containing hbase snapshots must be specified.");
    Preconditions.checkArgument(
        restorepath == null || !restorepath.trim().isEmpty(),
        "restorepath cannot be empty if specified");
    Preconditions.checkArgument(
        runstatuspath == null || !runstatuspath.trim().isEmpty(),
        "runstatuspath cannot be empty if specified");

    java.net.URI sourceUri;
    try {
      sourceUri = new java.net.URI(sourcepath);
    } catch (java.net.URISyntaxException e) {
      throw new IllegalArgumentException("sourcepath is not a valid URI: " + sourcepath, e);
    }

    if (restorepath != null) {
      try {
        new java.net.URI(restorepath);
      } catch (java.net.URISyntaxException e) {
        throw new IllegalArgumentException("restorepath is not a valid URI: " + restorepath, e);
      }
    }

    if (runstatuspath != null) {
      try {
        new java.net.URI(runstatuspath);
      } catch (java.net.URISyntaxException e) {
        throw new IllegalArgumentException("runstatuspath is not a valid URI: " + runstatuspath, e);
      }
    }

    if (restorepath == null) {
      if (sourceUri.getScheme() != null && sourceUri.getScheme().equalsIgnoreCase("gs")) {
        String path = sourceUri.getPath();
        Preconditions.checkArgument(
            path != null && !path.isEmpty() && !path.equals("/"),
            "sourcepath must have a parent directory to auto-generate restorepath: %s",
            sourcepath);
      } else {
        Preconditions.checkArgument(
            new java.io.File(sourcepath).getParent() != null,
            "sourcepath must have a parent directory to auto-generate restorepath: %s",
            sourcepath);
      }
    }

    Preconditions.checkNotNull(
        snapshotInfos,
        "Snapshots must be specified. Allowed values are '*' (indicating all snapshots under source"
            + " path) or 'prefix*' (snapshots matching certain prefix) or"
            + " 'snapshotname1:tablename1,snapshotname2:tablename2' (comma seperated list of"
            + " snapshots)");
    Preconditions.checkArgument(
        !snapshotInfos.isEmpty(),
        "Snapshots must be specified. Allowed values are '*' (indicating all snapshots under source"
            + " path) or 'prefix*' (snapshots matching certain prefix) or"
            + " 'snapshotname1:tablename1,snapshotname2:tablename2' (comma seperated list of"
            + " snapshots)");
    java.util.Set<String> snapshotNames = new java.util.HashSet<>();
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      Preconditions.checkArgument(
          snapshotInfo.getSnapshotName() != null
              && !snapshotInfo.getSnapshotName().trim().isEmpty(),
          "snapshotName inside snapshots cannot be null or empty");
      Preconditions.checkArgument(
          snapshotNames.add(snapshotInfo.getSnapshotName()),
          "Duplicate snapshot name detected: %s",
          snapshotInfo.getSnapshotName());
      Preconditions.checkArgument(
          snapshotInfo.getbigtableTableName() != null
              && !snapshotInfo.getbigtableTableName().trim().isEmpty(),
          "bigtableTableName inside snapshots cannot be null or empty");
    }
    Preconditions.checkArgument(
        backoffInitialIntervalInMillis > 0,
        "backoffInitialIntervalInMillis must be greater than 0: %s",
        backoffInitialIntervalInMillis);
    Preconditions.checkArgument(
        backoffMaxIntervalInMillis > 0,
        "backoffMaxIntervalInMillis must be greater than 0: %s",
        backoffMaxIntervalInMillis);
    Preconditions.checkArgument(
        backoffInitialIntervalInMillis <= backoffMaxIntervalInMillis,
        "backoffInitialIntervalInMillis (%s) must be less than or equal to"
            + " backoffMaxIntervalInMillis (%s)",
        backoffInitialIntervalInMillis,
        backoffMaxIntervalInMillis);
    Preconditions.checkArgument(
        backoffMaxretries >= 0, "backoffMaxretries must be non-negative: %s", backoffMaxretries);

    if (hbaseConfiguration != null) {
      for (Map.Entry<String, String> entry : hbaseConfiguration.entrySet()) {
        Preconditions.checkArgument(
            entry.getKey() != null && !entry.getKey().trim().isEmpty(),
            "hbaseConfiguration keys cannot be null or empty");
        Preconditions.checkArgument(
            entry.getValue() != null, "hbaseConfiguration values cannot be null");
      }
    }
    if (bigtableConfiguration != null) {
      for (Map.Entry<String, String> entry : bigtableConfiguration.entrySet()) {
        Preconditions.checkArgument(
            entry.getKey() != null && !entry.getKey().trim().isEmpty(),
            "bigtableConfiguration keys cannot be null or empty");
        Preconditions.checkArgument(
            entry.getValue() != null, "bigtableConfiguration values cannot be null");
      }
    }
  }

  public List<SnapshotInfo> getSnapshots() {
    return this.snapshotInfos;
  }

  public void setSnapshots(List<SnapshotInfo> snapshots) {
    this.snapshotInfos = snapshots;
  }

  public Map<String, String> getHbaseConfiguration() {
    return this.hbaseConfiguration;
  }

  public void setHbaseConfiguration(Map<String, String> hbaseConfiguration) {
    this.hbaseConfiguration = hbaseConfiguration;
  }

  public Map<String, String> getBigtableConfiguration() {
    return bigtableConfiguration;
  }

  public void setBigtableConfiguration(Map<String, String> bigtableConfiguration) {
    this.bigtableConfiguration = bigtableConfiguration;
  }

  public static class SnapshotInfo implements Serializable {
    private final String snapshotName;
    private final String bigtableTableName;

    public SnapshotInfo(String snapshotName, String tableName) {
      this.snapshotName = snapshotName;
      this.bigtableTableName = tableName;
    }

    public String getSnapshotName() {
      return snapshotName;
    }

    public String getbigtableTableName() {
      return bigtableTableName;
    }
  }

  static class SnapshotInfoJsonAdapter extends TypeAdapter<List<SnapshotInfo>> {

    @Override
    public void write(JsonWriter jsonWriter, List<SnapshotInfo> snapshotInfos) throws IOException {
      jsonWriter.beginObject();
      if (snapshotInfos != null) {
        snapshotInfos.forEach(
            snapshotInfo -> {
              try {
                jsonWriter.name(snapshotInfo.getSnapshotName());
                jsonWriter.value(snapshotInfo.getbigtableTableName());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
      jsonWriter.endObject();
    }

    @Override
    public List<SnapshotInfo> read(JsonReader jsonReader) throws IOException {
      List<SnapshotInfo> snapshotInfoList = new ArrayList<>();
      jsonReader.beginObject();
      while (jsonReader.hasNext()) {
        snapshotInfoList.add(new SnapshotInfo(jsonReader.nextName(), jsonReader.nextString()));
      }
      jsonReader.endObject();
      return snapshotInfoList;
    }
  }
}
