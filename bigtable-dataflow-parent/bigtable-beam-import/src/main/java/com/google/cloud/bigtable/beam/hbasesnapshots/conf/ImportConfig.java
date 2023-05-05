package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class representing the job configuration loading the different input values and combinations of
 * snapshot names (such as all snapshots or matching prefix or explicit names) provided.
 */
public final class ImportConfig implements Serializable {
  private final long DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS = 5000; // 5 seconds
  private final long DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS = 3 * 60 * 1000; // 180 seconds
  private final int DEFAULT_BACKOFF_MAX_RETRIES = 3;

  @JsonAdapter(SnapshotInfoJsonAdapter.class)
  public List<SnapshotInfo> snapshots;

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
    this.snapshots = new ArrayList<>();
    snapshots.forEach(
        (snapshotName, bigtableName) ->
            this.snapshots.add(new SnapshotInfo(snapshotName, bigtableName)));
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
    this.backoffInitialIntervalInMillis = backoffInitialIntervalInMillis;
  }

  public long getBackoffMaxIntervalInMillis() {
    return this.backoffMaxIntervalInMillis;
  }

  public void setBackoffMaxIntervalInMillis(long backoffMaxIntervalInMillis) {
    this.backoffMaxIntervalInMillis = backoffMaxIntervalInMillis;
  }

  public int getBackoffMaxretries() {
    return this.backoffMaxretries;
  }

  public void setBackoffMaxretries(int backoffMaxretries) {
    this.backoffMaxretries = backoffMaxretries;
  }

  public List<SnapshotInfo> getSnapshots() {
    return this.snapshots;
  }

  public void setSnapshots(List<SnapshotInfo> snapshots) {
    this.snapshots = snapshots;
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
      snapshotInfos.forEach(
          snapshotInfo -> {
            try {
              jsonWriter.name(snapshotInfo.getSnapshotName());
              jsonWriter.value(snapshotInfo.getbigtableTableName());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
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
