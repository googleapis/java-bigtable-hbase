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
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.beam.hbasesnapshots.SnapshotUtils;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** A {@link AutoValue} class representing the configuration associated with each snapshot. */
@InternalApi("For internal usage only")
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SnapshotConfig implements Serializable {

  /** Creates a new builder for {@link SnapshotConfig}. */
  public static Builder builder() {
    return new AutoValue_SnapshotConfig.Builder();
  }

  /** Returns the Google Cloud project ID. */
  public abstract String getProjectId();

  /** Returns the GCS source location of the HBase snapshot. */
  public abstract String getSourceLocation();

  /** Returns the GCS source path as a Hadoop {@link Path}. */
  public Path getSourcePath() {
    return new Path(getSourceLocation());
  }

  /** Returns the GCS restore path as a Hadoop {@link Path}. */
  public Path getRestorePath() {
    return new Path(getRestoreLocation());
  }

  /** Returns the name of the HBase snapshot. */
  public abstract String getSnapshotName();

  /** Returns the name of the Bigtable table to import into. */
  public abstract String getTableName();

  /** Returns the GCS location where the snapshot is restored temporarily. */
  public abstract String getRestoreLocation();

  /** Returns the additional configuration details as a map. */
  public abstract Map<String, String> getConfigurationDetails();

  /** Returns the Hadoop {@link Configuration} derived from configuration details. */
  public Configuration getConfiguration() {
    return SnapshotUtils.getHBaseConfiguration(getConfigurationDetails());
  }

  /** Returns a builder initialized with the current values of this instance. */
  public abstract Builder toBuilder();

  /** Builder for {@link SnapshotConfig}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the Google Cloud project ID. */
    public abstract Builder setProjectId(String projectId);

    /** Sets the GCS source location of the HBase snapshot. */
    public abstract Builder setSourceLocation(String value);

    /** Sets the name of the HBase snapshot. */
    public abstract Builder setSnapshotName(String value);

    /** Sets the name of the Bigtable table to import into. */
    public abstract Builder setTableName(String value);

    /** Sets the GCS location where the snapshot is restored temporarily. */
    public abstract Builder setRestoreLocation(String value);

    /** Sets the additional configuration details. */
    public abstract Builder setConfigurationDetails(Map<String, String> configuration);

    /** Builds the {@link SnapshotConfig} instance. */
    public abstract SnapshotConfig build();
  }
}
