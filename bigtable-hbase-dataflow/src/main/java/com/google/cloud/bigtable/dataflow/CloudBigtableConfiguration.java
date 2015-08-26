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

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

/**
 * This class defines information that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable cluster.
 */
public class CloudBigtableConfiguration implements Serializable {

  private static final long serialVersionUID = 1655181275627002133L;

  /**
   * Builds a {@link CloudBigtableConfiguration}.
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder<?>> {
    protected String projectId;
    protected String zoneId;
    protected String clusterId;

    /**
     * Specifies the project ID for the Cloud Bigtable cluster.
     * @param projectId The project ID for the cluster.
     * @return The original {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public T withProjectId(String projectId) {
      this.projectId = projectId;
      return (T) this;
    }

    /**
     * Specifies the zone where the Cloud Bigtable cluster is located.
     * @param zoneId The zone where the cluster is located.
     * @return The original {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public T withZoneId(String zoneId) {
      this.zoneId = zoneId;
      return (T) this;
    }

    /**
     * Specifies the cluster ID for the Cloud Bigtable cluster.
     * @param clusterId The cluster ID for the cluster.
     * @return The original {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public T withClusterId(String clusterId) {
      this.clusterId = clusterId;
      return (T) this;
    }

    /**
     * Builds the {@link CloudBigtableConfiguration}.
     * @return The new {@link CloudBigtableConfiguration}.
     */
    public CloudBigtableConfiguration build() {
      return new CloudBigtableConfiguration(projectId, zoneId, clusterId);
    }
  }

  // Not final due to serialization of CloudBigtableScanConfiguration.
  protected String projectId;
  protected String zoneId;
  protected String clusterId;

  // Used for serialization of CloudBigtableScanConfiguration.
  CloudBigtableConfiguration() {
  }

  /**
   * Creates a {@link CloudBigtableConfiguration} using the specified project ID, zone, and cluster
   * ID.
   * @param projectId The project ID for the cluster.
   * @param zoneId The zone where the cluster is located.
   * @param clusterId The cluster ID for the cluster.
   */
  public CloudBigtableConfiguration(String projectId, String zoneId, String clusterId) {
    this.projectId = projectId;
    this.zoneId = zoneId;
    this.clusterId = clusterId;
  }

  /**
   * Gets the project ID for the Cloud Bigtable cluster.
   * @return The project ID for the cluster.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Gets the zone where the Cloud Bigtable cluster is located.
   * @return The zone where the cluster is located.
   */
  public String getZoneId() {
    return zoneId;
  }

  /**
   * Gets the cluster ID for the Cloud Bigtable cluster.
   * @return The cluster ID for the cluster.
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Converts the {@link CloudBigtableConfiguration} to a {@link BigtableOptions} object.
   * @return The {@link BigtableOptions} object.
   */
  public BigtableOptions toBigtableOptions() {
    return new BigtableOptions.Builder()
      .setProjectId(getProjectId())
      .setZoneId(getZoneId())
      .setClusterId(getClusterId())
      .setUserAgent("CloudBigtableDataflow")
      .build();
  }

  /**
   * Converts the {@link CloudBigtableConfiguration} to an HBase {@link Configuration}.
   * @return The {@link Configuration}.
   */
  public Configuration toHBaseConfig() {
    Configuration config = new Configuration();
    config.set(BigtableOptionsFactory.PROJECT_ID_KEY, getProjectId());
    config.set(BigtableOptionsFactory.ZONE_KEY, getZoneId());
    config.set(BigtableOptionsFactory.CLUSTER_KEY, getClusterId());
    return config;
  }
}
