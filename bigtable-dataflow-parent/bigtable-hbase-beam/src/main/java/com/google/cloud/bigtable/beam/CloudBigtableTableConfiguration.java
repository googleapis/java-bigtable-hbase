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

import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable instance, including a table to connect to in the instance.
 */
public class CloudBigtableTableConfiguration extends CloudBigtableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  /**
   * Builds a {@link CloudBigtableTableConfiguration}.
   */
  public static class Builder extends CloudBigtableConfiguration.Builder {
    protected ValueProvider<String> tableId;

    public Builder() {
    }
    
    /**
     * Specifies the table to connect to.
     * @param tableId The table to connect to.
     * @return The {@link CloudBigtableTableConfiguration.Builder} for chaining convenience.
     */
    Builder withTableId(ValueProvider<String> tableId) {
      this.tableId = tableId;
      return this;
    }

    /**
     * Specifies the table to connect to.
     * @param tableId The table to connect to.
     * @return The {@link CloudBigtableTableConfiguration.Builder} for chaining convenience.
     */
    public Builder withTableId(String tableId) {
      return withTableId(StaticValueProvider.of(tableId));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableConfiguration.Builder#withProjectId(ValueProvider)} so that
     * it returns {@link CloudBigtableTableConfiguration.Builder}.
     */
    @Override
    Builder withProjectId(ValueProvider<String> projectId) {
      super.withProjectId(projectId);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableConfiguration.Builder#withProjectId(String)} so that it
     * returns {@link CloudBigtableTableConfiguration.Builder}.
     */
    @Override
    public Builder withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableConfiguration.Builder#withInstanceId(ValueProvider)} so that
     * it returns {@link CloudBigtableTableConfiguration.Builder}.
     */
    @Override
    Builder withInstanceId(ValueProvider<String> instanceId) {
      super.withInstanceId(instanceId);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableConfiguration.Builder#withInstanceId(String)} so that it
     * returns {@link CloudBigtableTableConfiguration.Builder}.
     */
    @Override
    public Builder withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableConfiguration.Builder#withConfiguration(String,
     * ValueProvider)} so that it returns {@link CloudBigtableTableConfiguration.Builder}.
     */
    @Override
    Builder withConfiguration(String key, ValueProvider<String> value) {
      super.withConfiguration(key, value);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableConfiguration.Builder#withConfiguration(String, String)} so
     * that it returns {@link CloudBigtableTableConfiguration.Builder}.
     */
    @Override
    public Builder withConfiguration(String key, String value) {
      return withConfiguration(key, StaticValueProvider.of(value));
    }

    /**
     * Builds the {@link CloudBigtableTableConfiguration}.
     * @return The new {@link CloudBigtableTableConfiguration}.
     */
    @Override
    public CloudBigtableTableConfiguration build() {
      return new CloudBigtableTableConfiguration(
          projectId, instanceId, tableId, additionalConfiguration);
    }
  }

  protected ValueProvider<String> tableId;

  // This is required for serialization of CloudBigtableScanConfiguration.
  CloudBigtableTableConfiguration() {
  }

  /**
   * Creates a {@link CloudBigtableTableConfiguration} using the specified configuration.
   *
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID
   * @param tableId The table to connect to in the instance.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  protected CloudBigtableTableConfiguration(
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId,
      ValueProvider<String> tableId,
      Map<String, ValueProvider<String>> additionalConfiguration) {
    super(projectId, instanceId, additionalConfiguration);
    this.tableId = tableId;
  }

  /**
   * Gets the table specified by the configuration.
   * @return The table ID.
   */
  public String getTableId() {
    return tableId.get();
  }

  @Override
  public Builder toBuilder() {
    Builder builder = new Builder();
    copyConfig(builder);
    return builder;
  }

  public void copyConfig(Builder builder) {
    super.copyConfig(builder);
    builder.tableId = tableId;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects.equals(getTableId(), ((CloudBigtableTableConfiguration) obj).getTableId());
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("tableId", tableId).withLabel("Table ID"));
  }

  @Override
  public void validate() {
    super.validate();
    checkNotNullOrEmpty(getTableId(), "tableid");
  }
}
