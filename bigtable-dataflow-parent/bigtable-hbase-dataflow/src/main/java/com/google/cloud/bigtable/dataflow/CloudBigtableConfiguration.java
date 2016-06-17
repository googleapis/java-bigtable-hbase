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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import com.google.bigtable.repackaged.com.google.cloud.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a Cloud
 * Bigtable cluster.
 */
public class CloudBigtableConfiguration implements Serializable {

  private static final long serialVersionUID = 1655181275627002133L;

  /**
   * Builds a {@link CloudBigtableConfiguration}.
   */
  public static class Builder {
    protected String projectId;
    protected String instanceId;
    protected Map<String, String> additionalConfiguration = new HashMap<>();

    public Builder() {
    }

    protected void copyFrom(Map<String, String> configuration) {
      this.additionalConfiguration.putAll(configuration);

      this.projectId = this.additionalConfiguration.remove(BigtableOptionsFactory.PROJECT_ID_KEY);
      this.instanceId = this.additionalConfiguration.remove(BigtableOptionsFactory.INSTANCE_ID_KEY);
    }

    /**
     * Specifies the project ID for the Cloud Bigtable cluster.
     * @param projectId The project ID for the cluster.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public Builder withProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    /**
     * Specifies the Cloud Bigtable instanceId.
     * @param instanceId The Cloud Bigtable instanceId.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public Builder withInstanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    /**
     * Adds additional connection configuration.
     * {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more information about
     * configuration options.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public Builder withConfiguration(String key, String value) {
      Preconditions.checkArgument(value != null, "Value cannot be null");
      this.additionalConfiguration.put(key, value);
      return this;
    }

    /**
     * Builds the {@link CloudBigtableConfiguration}.
     * @return The new {@link CloudBigtableConfiguration}.
     */
    public CloudBigtableConfiguration build() {
      return new CloudBigtableConfiguration(projectId, instanceId, additionalConfiguration);
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
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   *          See {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more
   *          information about configuration options.
   */
  public CloudBigtableConfiguration(String projectId, String instanceId,
      Map<String, String> additionalConfiguration) {
    this.configuration = new HashMap<>(additionalConfiguration);
    setValue(BigtableOptionsFactory.PROJECT_ID_KEY, projectId, "Project ID");
    setValue(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId, "Instance ID");
  }

  private void setValue(String key, String value, String type) {
    Preconditions.checkArgument(!configuration.containsKey(key), "%s was set twice", key);
    Preconditions.checkArgument(value != null, "%s must be set.", type);
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
   * Gets the Cloud Bigtable instance id.
   * @return The Cloud Bigtable instance id.
   */
  public String getInstanceId() {
    return configuration.get(BigtableOptionsFactory.INSTANCE_ID_KEY);
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
    Configuration config = new Configuration(false);

    // This setting can potentially decrease performance for large scale writes. However, this
    // setting prevents problems that occur when streaming Sources, such as PubSub, are used.
    // To override this behavior, call:
    //    Builder.withConfiguration(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, 
    //                              BigtableOptions.BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT);
    config.set(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, "0");
    for (Entry<String, String> entry : configuration.entrySet()) {
      config.set(entry.getKey(), entry.getValue());
    }
    return config;
  }

  /**
   * Creates a new {@link Builder} object containing the existing configuration.
   * @return A new {@link Builder}.
   */
  public Builder toBuilder() {
    Builder builder = new Builder();
    copyConfig(builder);
    return builder;
  }

  /**
   * Gets an immutable copy of the configuration map.
   */
  protected ImmutableMap<String, String> getConfiguration() {
    return ImmutableMap.copyOf(configuration);
  }

  /**
   * Compares this configuration with the specified object.
   * @param obj The object to compare this configuration against.
   * @return {@code true} if the given object has the same configuration, {@code false} otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    CloudBigtableConfiguration other = (CloudBigtableConfiguration) obj;
    return Objects.equals(configuration, other.configuration);
  }

  public void copyConfig(Builder builder) {
    builder.copyFrom(configuration);
  }
}
