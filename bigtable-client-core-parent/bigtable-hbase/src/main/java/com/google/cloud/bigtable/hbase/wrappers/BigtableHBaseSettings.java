/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.APP_PROFILE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public abstract class BigtableHBaseSettings {

  protected static final Logger LOG = new Logger(BigtableOptionsFactory.class);

  private final Configuration configuration;
  private final String projectId;
  private final String instanceId;
  private final String appProfileId;
  private final int ttlSecondsForBackup;

  @VisibleForTesting
  // This is used to override the default closing timeout of 10 minutes in the test so the test can
  // run faster
  public static final String BULK_MUTATION_CLOSE_TIMEOUT_MILLISECONDS =
      "bulk.mutation.close.timeout.milliseconds";

  // Must be non-negative. Set to 0 to disable timeout.
  private final long bulkMutationCloseTimeoutMilliseconds;

  public static BigtableHBaseSettings create(Configuration configuration) throws IOException {
    return BigtableHBaseVeneerSettings.create(configuration);
  }

  public BigtableHBaseSettings(Configuration configuration) {
    this.configuration = new Configuration(configuration);
    this.projectId = getRequiredValue(PROJECT_ID_KEY, "Project ID");
    this.instanceId = getRequiredValue(INSTANCE_ID_KEY, "Instance ID");
    this.appProfileId = configuration.get(APP_PROFILE_ID_KEY);

    this.ttlSecondsForBackup =
        configuration.getInt(
            BigtableOptionsFactory.BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY,
            BigtableOptionsFactory.BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_VALUE);

    this.bulkMutationCloseTimeoutMilliseconds =
        configuration.getLong(BigtableHBaseSettings.BULK_MUTATION_CLOSE_TIMEOUT_MILLISECONDS, 0);
    Preconditions.checkArgument(this.bulkMutationCloseTimeoutMilliseconds >= 0);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getAppProfileId() {
    return appProfileId;
  }

  public int getTtlSecondsForBackup() {
    return ttlSecondsForBackup;
  }

  public abstract String getDataHost();

  public abstract String getAdminHost();

  public abstract int getPort();

  public abstract int getBulkMaxRowCount();

  public abstract long getBatchingMaxRequestSize();

  // This is equivalent to allow server-side timestamp.
  public abstract boolean isRetriesWithoutTimestampAllowed();

  public long getBulkMutationCloseTimeoutMilliseconds() {
    return bulkMutationCloseTimeoutMilliseconds;
  }

  protected String getRequiredValue(String key, String displayName) {
    String value = configuration.get(key);
    Preconditions.checkArgument(
        !isNullOrEmpty(value), String.format("%s must be supplied via %s", displayName, key));
    return value;
  }

  // Prints the full set of settings.
  public abstract String toDebugString();

  // Each element prints part of the settings, in case the full set is too long for logging.
  public abstract Map<String, String> toDebugStrings();
}
