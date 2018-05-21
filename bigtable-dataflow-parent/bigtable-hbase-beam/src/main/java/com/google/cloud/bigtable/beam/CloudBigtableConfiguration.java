/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.Map.Entry;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;
import org.apache.beam.sdk.repackaged.com.google.common.base.Strings;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hadoop.conf.Configuration;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a Cloud
 * Bigtable instance.
 */
public class CloudBigtableConfiguration implements Serializable {

  private static final long serialVersionUID = 1655181275627002133L;

  protected static final String VALUE_UNAVAILABLE = "Unavailable during pipeline construction";

  /**
   * Builds a {@link CloudBigtableConfiguration}.
   */
  public static class Builder {
    protected ValueProvider<String> projectId;
    protected ValueProvider<String> instanceId;
    protected Map<String, ValueProvider<String>> additionalConfiguration = new HashMap<>();

    public Builder() {
    }

    protected void copyFrom(Map<String, ValueProvider<String>> configuration) {
      this.additionalConfiguration.putAll(configuration);

      this.projectId = this.additionalConfiguration.remove(BigtableOptionsFactory.PROJECT_ID_KEY);
      this.instanceId = this.additionalConfiguration.remove(BigtableOptionsFactory.INSTANCE_ID_KEY);
    }

    /**
     * Specifies the project ID for the Cloud Bigtable instance.
     * @param projectId The project ID for the instance.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public Builder withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * Specifies the project ID for the Cloud Bigtable instance.
     * @param projectId The project ID for the instance.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    Builder withProjectId(ValueProvider<String> projectId) {
      this.projectId = projectId;
      return this;
    }

    /**
     * Specifies the Cloud Bigtable instanceId.
     * @param instanceId The Cloud Bigtable instanceId.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public Builder withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    /**
     * Specifies the Cloud Bigtable instanceId.
     * @param instanceId The Cloud Bigtable instanceId.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    Builder withInstanceId(ValueProvider<String> instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    /**
     * Specifies the AppProfile to use.
     *
     * <p>This is a private alpha release of Cloud Bigtable replication. This feature
     * is not currently available to most Cloud Bigtable customers. This feature
     * might be changed in backward-incompatible ways and is not recommended for
     * production use. It is not subject to any SLA or deprecation policy.
     */
    public Builder withAppProfileId(String appProfileId) {
      return withAppProfileId(StaticValueProvider.of(appProfileId));
    }

    /**
     * Specifies the AppProfile to use.
     *
     * <p>This is a private alpha release of Cloud Bigtable replication. This feature
     * is not currently available to most Cloud Bigtable customers. This feature
     * might be changed in backward-incompatible ways and is not recommended for
     * production use. It is not subject to any SLA or deprecation policy.
     */
    Builder withAppProfileId(ValueProvider<String> appProfileId) {
      return withConfiguration(BigtableOptionsFactory.APP_PROFILE_ID_KEY, appProfileId);
    }

    /**
     * Adds additional connection configuration.
     * {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more information about
     * configuration options.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    public Builder withConfiguration(String key, String value) {
      return withConfiguration(key, StaticValueProvider.of(value));
    }

    /**
     * Adds additional connection configuration.
     * {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more information about
     * configuration options.
     * @return The {@link CloudBigtableConfiguration.Builder} for chaining convenience.
     */
    Builder withConfiguration(String key, ValueProvider<String> value) {
      Preconditions.checkArgument(value != null, "Value cannot be null");
      this.additionalConfiguration.put(key, value);
      return this;
    }

    /**
     * Builds the {@link CloudBigtableConfiguration}.
     *
     * @return The new {@link CloudBigtableConfiguration}.
     */
    public CloudBigtableConfiguration build() {
      // Keeping the legacy constructor for backwards compatibility.
      // Choose the new one if instance is specified.
      return new CloudBigtableConfiguration(
          projectId, instanceId, additionalConfiguration);
    }
  }

  // Not final due to serialization of CloudBigtableScanConfiguration.
  private Map<String, ValueProvider<String>> configuration;

  // Used for serialization of CloudBigtableScanConfiguration.
  CloudBigtableConfiguration() {
  }

  /**
   * Creates a {@link CloudBigtableConfiguration} using the specified project ID and instance ID.
   *
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID.
   * @param additionalConfiguration A {@link Map} with additional connection configuration. See
   *          {@link BigtableOptionsFactory#fromConfiguration(Configuration)} for more information
   *          about configuration options.
   */
  protected CloudBigtableConfiguration(
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId,
      Map<String, ValueProvider<String>> additionalConfiguration) {
    this.configuration = new HashMap<>(additionalConfiguration);
    setValue(BigtableOptionsFactory.PROJECT_ID_KEY, projectId, "Project ID");
    setValue(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId, "Instance ID");
  }

  private void setValue(String key, ValueProvider<String> value, String type) {
    Preconditions.checkArgument(!configuration.containsKey(key), "%s was set twice", key);
    Preconditions.checkArgument(value != null, "%s must be set.", type);
    configuration.put(key, value);
  }

  /**
   * Gets the project ID for the Cloud Bigtable instance.
   * @return The project ID for the instance.
   */
  public String getProjectId() {
    return configuration.get(BigtableOptionsFactory.PROJECT_ID_KEY).get();
  }

  /**
   * Gets the Cloud Bigtable instance id.
   * @return The Cloud Bigtable instance id.
   */
  public String getInstanceId() {
    return configuration.get(BigtableOptionsFactory.INSTANCE_ID_KEY).get();
  }

  /**
   * Get the Cloud Bigtable App Profile id.
   */
  public String getAppProfileId() {
    return configuration.get(BigtableOptionsFactory.APP_PROFILE_ID_KEY).get();
  }

  /**
   * Converts the {@link CloudBigtableConfiguration} to a {@link BigtableOptions} object.
   * @return The {@link BigtableOptions} object.
   */
  public BigtableOptions toBigtableOptions() throws IOException {
    return BigtableOptionsFactory.fromConfiguration(toHBaseConfig()).build();
  }

  /**
   * Converts the {@link CloudBigtableConfiguration} to an HBase {@link Configuration}.
   * @return The {@link Configuration}.
   */
  public Configuration toHBaseConfig() {
    Configuration config = new Configuration(false);

    config.set(BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");

    // Beam should use a different endpoint for data operations than online traffic.
    config.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY,
      BigtableOptions.BIGTABLE_BATCH_DATA_HOST_DEFAULT);

    config.set(BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY,
      String.valueOf(TimeUnit.SECONDS.toMillis(5)));

    config.set(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY,
      String.valueOf(TimeUnit.MINUTES.toMillis(5)));

    // This setting can potentially decrease performance for large scale writes. However, this
    // setting prevents problems that occur when streaming Sources, such as PubSub, are used.
    // To override this behavior, call:
    //    Builder.withConfiguration(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, 
    //                              BigtableOptions.BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT);
    config.set(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, "0");
    for (Entry<String, ValueProvider<String>> entry : configuration.entrySet()) {
      // If the value from ValueProvider is null, the value was not provided at runtime.
      if (entry.getValue().get() != null) {
        config.set(entry.getKey(), entry.getValue().get());
      }
    }
    setUserAgent(config);
    return config;
  }

  private void setUserAgent(Configuration config) {
    String beamUserAgent = "HBaseBeam";
    if (configuration.containsKey(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY)) {
      beamUserAgent += "," + configuration.get(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY);
    }
    config.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, beamUserAgent);
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
  protected ImmutableMap<String, ValueProvider<String>> getConfiguration() {
    return ImmutableMap.copyOf(configuration);
  }

  /**
   * Compares this configuration with the specified object.
   *
   * @param obj The object to compare this configuration against.
   * @return {@code true} if the given object has the same configuration, {@code false} otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    CloudBigtableConfiguration other = (CloudBigtableConfiguration) obj;
    if (!Objects.equals(configuration.keySet(), other.configuration.keySet())) {
      return false;
    }
    for (String key : configuration.keySet()) {
      if (!Objects.equals(configuration.get(key).get(), other.configuration.get(key).get())) {
        return false;
      }
    }
    return true;
  }

  public void copyConfig(Builder builder) {
    builder.copyFrom(configuration);
  }

  /**
   * Checks if the parameters are accessible. Runtime parameters are not accessible at pipeline
   * construction time.
   */
  protected boolean areParametersAccessible() {
    return configuration.get(BigtableOptionsFactory.PROJECT_ID_KEY).isAccessible();
  }

  /**
   * @param <T> parameter The runtime parameter.
   * @return the String value of runtime parameter if the parameter is accessible, returns
   *     "Unavailable during pipeline construction" otherwise for debugging purpose.
   */
  protected static <T> String getDisplayValue(ValueProvider<T> parameter) {
    if (parameter.isAccessible()) {
      return String.valueOf(parameter.get());
    }
    return VALUE_UNAVAILABLE;
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(
        DisplayData.item(
                "projectId",
                getDisplayValue(configuration.get(BigtableOptionsFactory.PROJECT_ID_KEY)))
            .withLabel("Project ID"));
    builder.add(
        DisplayData.item(
                "instanceId",
                getDisplayValue(configuration.get(BigtableOptionsFactory.INSTANCE_ID_KEY)))
            .withLabel("Instance ID"));
    Map<String, ValueProvider<String>> hashMap =
        new HashMap<String, ValueProvider<String>>(configuration);
    hashMap.remove(BigtableOptionsFactory.PROJECT_ID_KEY);
    hashMap.remove(BigtableOptionsFactory.INSTANCE_ID_KEY);
    for (Entry<String, ValueProvider<String>> entry : configuration.entrySet()) {
      builder.add(
          DisplayData.item(entry.getKey(), getDisplayValue(entry.getValue()))
              .withLabel(entry.getKey()));
    }
  }

  protected static void checkNotNullOrEmpty(String value, String name) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(value),
        "A " + name + " must be set to configure Bigtable properly.");
  }

  public void validate() {
    if (areParametersAccessible()) {
      checkNotNullOrEmpty(getProjectId(), "projectId");
      checkNotNullOrEmpty(getInstanceId(), "instanceId");
    }
  }
}
