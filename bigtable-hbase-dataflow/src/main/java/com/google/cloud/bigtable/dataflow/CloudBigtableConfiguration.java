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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
    protected Map<String, String> additionalConfiguration = new HashMap<>();

    public Builder() {
    }

    protected Builder(Map<String, String> configuration) {
      this.additionalConfiguration.putAll(configuration);

      this.projectId = this.additionalConfiguration.remove(BigtableOptionsFactory.PROJECT_ID_KEY);
      this.zoneId = this.additionalConfiguration.remove(BigtableOptionsFactory.ZONE_KEY);
      this.clusterId = this.additionalConfiguration.remove(BigtableOptionsFactory.CLUSTER_KEY);
    }

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
     * Add additional connection configuration.
     * {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more information about
     * configuration options.
     */
    public T withConfiguration(String key, String value) {
      Preconditions.checkArgument(value != null, "Value cannot be null");
      this.additionalConfiguration.put(key, value);
      return (T) this;
    }

    /**
     * Builds the {@link CloudBigtableConfiguration}.
     * @return The new {@link CloudBigtableConfiguration}.
     */
    public CloudBigtableConfiguration build() {
      return new CloudBigtableConfiguration(projectId, zoneId, clusterId, additionalConfiguration);
    }

    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }
}

  // Not final due to serialization of CloudBigtableScanConfiguration.
  private Map<String, String> configuration;

  // Used for serialization of CloudBigtableScanConfiguration.
  CloudBigtableConfiguration() {
  }

  /**
   * Creates a {@link CloudBigtableConfiguration} using the specified project ID, zone, and cluster
   * ID.
   * @param projectId The project ID for the cluster.
   * @param zoneId The zone where the cluster is located.
   * @param clusterId The cluster ID for the cluster.
   * @param additionalConfiguration A Map with additional connection configuration.
   *          {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more information
   *          about configuration options.
   */
  public CloudBigtableConfiguration(String projectId, String zoneId, String clusterId,
      Map<String, String> additionalConfiguration) {
    this.configuration = new HashMap<>(additionalConfiguration);
    setValue(BigtableOptionsFactory.PROJECT_ID_KEY, projectId, "Project ID");
    setValue(BigtableOptionsFactory.ZONE_KEY, zoneId, "Zone ID");
    setValue(BigtableOptionsFactory.CLUSTER_KEY, clusterId, "Cluster ID");
  }

  private void setValue(String key, String value, String type) {
    Preconditions.checkArgument(!configuration.containsKey(key),
      String.format("%s was set twice", key));
    Preconditions.checkArgument(value != null, String.format("%s must be set.", type));
    configuration.put(key, value);
  }

  /**
   * Gets the project ID for the Cloud Bigtable cluster.
   * @return The project ID for the cluster.
   */
  public String getProjectId() {
    return configuration.get(BigtableOptionsFactory.PROJECT_ID_KEY);
  }

  /**
   * Gets the zone where the Cloud Bigtable cluster is located.
   * @return The zone where the cluster is located.
   */
  public String getZoneId() {
    return configuration.get(BigtableOptionsFactory.ZONE_KEY);
  }

  /**
   * Gets the cluster ID for the Cloud Bigtable cluster.
   * @return The cluster ID for the cluster.
   */
  public String getClusterId() {
    return configuration.get(BigtableOptionsFactory.CLUSTER_KEY);
  }

  /**
   * Converts the {@link CloudBigtableConfiguration} to a {@link BigtableOptions} object.
   * @return The {@link BigtableOptions} object.
   */
  public BigtableOptions toBigtableOptions() throws IOException {
    return BigtableOptionsFactory.fromConfiguration(toHBaseConfig());
  }

  /**
   * Converts the {@link CloudBigtableConfiguration} to an HBase {@link Configuration}.
   * @return The {@link Configuration}.
   */
  public Configuration toHBaseConfig() {
    Configuration config = new Configuration();
    for (Entry<String, String> entry : configuration.entrySet()) {
      config.set(entry.getKey(), entry.getValue());
    }
    return config;
  }

  /**
   * Creates a new Builder from the configuration
   * @return A new {@link Builder}
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Builder toBuilder() {
    return new Builder(getConfiguration());
  }

  /**
   * Returns an unmodifiable copy of the configuration map.
   */
  protected Map<String, String> getConfiguration() {
    return ImmutableMap.copyOf(configuration);
  }

  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }
}
