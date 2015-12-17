/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable cluster, including a table to connect to in the cluster.
 */
public class CloudBigtableTableConfiguration extends CloudBigtableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  /**
   * Converts a {@link CloudBigtableOptions} object to a {@link CloudBigtableTableConfiguration}
   * object.
   * @param options The {@link CloudBigtableOptions} object.
   * @return The new {@link CloudBigtableTableConfiguration}.
   */
  public static CloudBigtableTableConfiguration fromCBTOptions(CloudBigtableOptions options){
    return new CloudBigtableTableConfiguration(
        options.getBigtableProjectId(),
        options.getBigtableZoneId(),
        options.getBigtableClusterId(),
        options.getBigtableTableId(),
        Collections.<String, String> emptyMap());
  }

  /**
   * Builds a {@link CloudBigtableTableConfiguration}.
   */
  public static class Builder extends CloudBigtableConfiguration.Builder {
    protected String tableId;

    public Builder() {
    }

    protected Builder(Map<String, String> configuration) {
      super(configuration);
    }

    /**
     * Specifies the table to connect to.
     * @param tableId The table to connect to.
     * @return The {@link CloudBigtableTableConfiguration.Builder} for chaining convenience.
     */
    public Builder withTableId(String tableId) {
      this.tableId = tableId;
      return this;
    }

    /**
     * {@inheritDoc}
     * 
     * Overrides {@link CloudBigtableScanConfiguration.Builder#withProjectId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withProjectId(String projectId) {
      super.withProjectId(projectId);
      return this;
    }

    /**
     * {@inheritDoc}
     * 
     * Overrides {@link CloudBigtableScanConfiguration.Builder#withZoneId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withZoneId(String zoneId) {
      super.withZoneId(zoneId);
      return this;
    }

    /**
     * {@inheritDoc}
     * 
     * Overrides {@link CloudBigtableScanConfiguration.Builder#withClusterId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withClusterId(String clusterId) {
      super.withClusterId(clusterId);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * Overrides {@link CloudBigtableScanConfiguration.Builder#withConfiguration(String, String)} so
     * that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withConfiguration(String key, String value) {
      super.withConfiguration(key, value);
      return this;
    }

    /**
     * Builds the {@link CloudBigtableTableConfiguration}.
     * @return The new {@link CloudBigtableTableConfiguration}.
     */
    @Override
    public CloudBigtableTableConfiguration build() {
      return new CloudBigtableTableConfiguration(projectId, zoneId, clusterId, tableId,
          additionalConfiguration);
    }
  }

  protected String tableId;

  // This is required for serialization of CloudBigtableScanConfiguration.
  CloudBigtableTableConfiguration() {
  }

  /**
   * Creates a {@link CloudBigtableTableConfiguration} using the specified configuration.
   * @param projectId The project ID for the cluster.
   * @param zoneId The zone where the cluster is located.
   * @param clusterId The cluster ID for the cluster.
   * @param tableId The table to connect to in the cluster.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  public CloudBigtableTableConfiguration(String projectId, String zoneId, String clusterId,
      String tableId, Map<String, String> additionalConfiguration) {
    super(projectId, zoneId, clusterId, additionalConfiguration);
    this.tableId = tableId;
  }

  /**
   * Gets the table specified by the configuration.
   * @return The table ID.
   */
  public String getTableId() {
    return tableId;
  }

  @Override
  public Builder toBuilder() {
    return new Builder(getConfiguration())
        .withTableId(tableId);
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects.equals(tableId, ((CloudBigtableTableConfiguration) obj).tableId);
  }
}
