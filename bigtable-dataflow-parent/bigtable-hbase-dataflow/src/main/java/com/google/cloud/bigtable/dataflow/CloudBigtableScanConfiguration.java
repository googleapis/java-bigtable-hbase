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


import org.apache.hadoop.hbase.client.Scan;

import com.google.bigtable.repackaged.com.google.cloud.grpc.BigtableInstanceName;
import com.google.bigtable.repackaged.com.google.cloud.hbase.adapters.Adapters;
import com.google.bigtable.repackaged.com.google.cloud.hbase.adapters.read.DefaultReadHooks;
import com.google.bigtable.repackaged.com.google.cloud.hbase.adapters.read.ReadHooks;
import com.google.bigtable.repackaged.com.google.com.google.bigtable.v2.ReadRowsRequest;

import java.util.Map;
import java.util.Objects;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable cluster; a table to connect to in the cluster; and a filter on the table in the form of
 * a {@link Scan}.
 */
public class CloudBigtableScanConfiguration extends CloudBigtableTableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  /**
   * Converts a {@link CloudBigtableOptions} object to a {@link CloudBigtableScanConfiguration}
   * object with a default full table {@link Scan}.
   * @param options The {@link CloudBigtableOptions} object.
   * @return The new {@link CloudBigtableScanConfiguration}.
   */
  public static CloudBigtableScanConfiguration fromCBTOptions(CloudBigtableOptions options) {
    return fromCBTOptions(options, new Scan());
  }

  /**
   * Converts a {@link CloudBigtableOptions} object to a {@link CloudBigtableScanConfiguration}
   * that will perform the specified {@link Scan} on the table.
   * @param options The {@link CloudBigtableOptions} object.
   * @param scan The {@link Scan} to add to the configuration.
   * @return The new {@link CloudBigtableScanConfiguration}.
   */
  public static CloudBigtableScanConfiguration fromCBTOptions(CloudBigtableOptions options,
      Scan scan) {
    CloudBigtableScanConfiguration.Builder builder = new CloudBigtableScanConfiguration.Builder();
    copyOptions(options, builder);
    return builder.withScan(scan).build();
  }

  /**
   * Converts a {@link CloudBigtableOptions} object to a {@link CloudBigtableScanConfiguration}
   * that will perform the specified {@link Scan} on the table.
   * @param config The {@link CloudBigtableTableConfiguration} object.
   * @param scan The {@link Scan} to add to the configuration.
   * @return The new {@link CloudBigtableScanConfiguration}.
   */
  public static CloudBigtableScanConfiguration fromConfig(CloudBigtableTableConfiguration config,
      Scan scan) {
    CloudBigtableScanConfiguration.Builder builder = new CloudBigtableScanConfiguration.Builder();
    config.copyConfig(builder);
    return builder.withScan(scan).build();
  }

  /**
   * Builds a {@link CloudBigtableScanConfiguration}.
   */
  public static class Builder extends CloudBigtableTableConfiguration.Builder {
    protected Scan scan = new Scan();
    private ReadRowsRequest request;

    public Builder() {
    }

    /**
     * Specifies the {@link Scan} that will be used to filter the table.
     * @param scan The {@link Scan} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withScan(Scan scan) {
      this.scan = scan;
      this.request = null;
      return this;
    }

    /**
     * Specifies the {@link ReadRowsRequest} that will be used to filter the table.
     * @param request The {@link ReadRowsRequest} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withRequest(ReadRowsRequest request) {
      this.request = request;
      this.scan = null;
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder withProjectId(String projectId) {
      super.withProjectId(projectId);
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder withInstanceId(String instanceId) {
      super.withInstanceId(instanceId);
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder withConfiguration(String key, String value) {
      super.withConfiguration(key, value);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * Overrides {@link CloudBigtableScanConfiguration.Builder#withTableId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withTableId(String tableId) {
      super.withTableId(tableId);
      return this;
    }

    /**
     * Builds the {@link CloudBigtableScanConfiguration}.
     * @return The new {@link CloudBigtableScanConfiguration}.
     */
    @Override
    public CloudBigtableScanConfiguration build() {
      if (request == null) {
        ReadHooks readHooks = new DefaultReadHooks();
        ReadRowsRequest.Builder builder = Adapters.SCAN_ADAPTER.adapt(scan, readHooks);
        builder.setTableName(new BigtableInstanceName(projectId, instanceId).toTableNameStr(tableId));
        request = readHooks.applyPreSendHook(builder.build());
      }
      return new CloudBigtableScanConfiguration(projectId, instanceId, tableId, request,
          additionalConfiguration);
    }
  }

  private final ReadRowsRequest request;

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, instance
   * ID, table ID, {@link Scan} and additional connection configuration.
   *
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID.
   * @param tableId The table to connect to in the cluster.
   * @param request The {@link ReadRowsRequest} that will be used to filter the table.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  protected CloudBigtableScanConfiguration(String projectId, String instanceId,
      String tableId, ReadRowsRequest request, Map<String, String> additionalConfiguration) {
    super(projectId, instanceId, tableId, additionalConfiguration);
    this.request = request;
  }

  /**
   * Gets the {@link Scan} used to filter the table.
   * @return The {@link Scan}.
   */
  public ReadRowsRequest getRequest() {
    return request;
  }

  /**
   * @return The start row for this configuration.
   */
  public byte[] getStartRow() {
    return request.getRows().getRowRanges(0).getStartKeyClosed().toByteArray();
  }

  /**
   * @return The stop row for this configuration.
   */
  public byte[] getStopRow() {
    return request.getRows().getRowRanges(0).getEndKeyOpen().toByteArray();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects
            .equals(request, ((CloudBigtableScanConfiguration) obj).request);
  }

  @Override
  public Builder toBuilder() {
    Builder builder = new Builder();
    copyConfig(builder);
    return builder;
  }

  public void copyConfig(Builder builder) {
    super.copyConfig(builder);
    builder.withRequest(request);
  }
}
