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
package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.bigtable.beam.hbasesnapshots.SnapshotUtils;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** A {@link AutoValue} class representing the configuration associated with each snapshot. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SnapshotConfig implements Serializable {

  public static Builder builder() {
    return new AutoValue_SnapshotConfig.Builder();
  }

  public abstract String getProjectId();

  public abstract String getSourceLocation();

  //  public abstract Path getSourcePath();
  @Memoized
  public Path getSourcePath() {
    return new Path(getSourceLocation());
  }

  @Memoized
  public Path getRestorePath() {
    return new Path(getRestoreLocation());
  }

  public abstract String getSnapshotName();

  public abstract String getTableName();

  public abstract String getRestoreLocation();

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  abstract Map<String, String> getConfigurationDetails();

  public Configuration getConfiguration() {
    return SnapshotUtils.getHBaseConfiguration(getConfigurationDetails());
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setSourceLocation(String value);

    public abstract Builder setSnapshotName(String value);

    public abstract Builder setTableName(String value);

    public abstract Builder setRestoreLocation(String value);

    public abstract Builder setConfigurationDetails(Map<String, String> configuration);

    public abstract SnapshotConfig build();
  }
}
