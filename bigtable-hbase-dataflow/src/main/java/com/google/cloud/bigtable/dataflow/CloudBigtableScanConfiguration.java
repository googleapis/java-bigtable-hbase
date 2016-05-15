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

import org.apache.hadoop.hbase.client.Scan;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.common.base.Preconditions;

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
    return new Builder()
        .withProjectId(options.getBigtableProjectId())
        .withZoneId(options.getBigtableZoneId())
        .withClusterId(options.getBigtableClusterId())
        .withTableId(options.getBigtableTableId())
        .withScan(scan)
        .build();
  }

  /**
   * Builds a {@link CloudBigtableScanConfiguration}.
   */
  public static class Builder extends CloudBigtableTableConfiguration.Builder {
    private ReadRowsRequest readRowsRequest;
    private Scan scan = new Scan();

    public Builder() {
    }

    public Builder(CloudBigtableTableConfiguration config) {
    	this(config.getConfiguration());
    	withTableId(config.getTableId());
    }

    protected Builder(Map<String, String> configuration) {
      super(configuration);
    }

    /**
     * Specifies the {@link Scan} that will be used to filter the table.
     * @param scan The {@link Scan} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withScan(Scan scan) {
      this.scan = scan;
      return this;
    }


    public Builder witReadRowsRequest(ReadRowsRequest readRowsRequest) {
      this.readRowsRequest = readRowsRequest;
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
    public Builder withZoneId(String zoneId) {
      super.withZoneId(zoneId);
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder withClusterId(String clusterId) {
      super.withClusterId(clusterId);
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
      if (readRowsRequest == null) {
        ReadHooks readHooks = new DefaultReadHooks();
        ReadRowsRequest.Builder builder = Adapters.SCAN_ADAPTER.adapt(scan, readHooks);
        builder.setTableName(new BigtableClusterName(projectId, zoneId, clusterId).toTableNameStr(tableId));
        this.readRowsRequest = readHooks.applyPreSendHook(builder.build());
      }
      return new CloudBigtableScanConfiguration(projectId, zoneId, clusterId, tableId,
          readRowsRequest, additionalConfiguration);
    }
  }

  private ReadRowsRequest readRowsRequest;

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, zone, cluster
   * ID, table ID and {@link Scan}.
   *
   * @param projectId The project ID for the cluster.
   * @param zoneId The zone where the cluster is located.
   * @param clusterId The cluster ID for the cluster.
   * @param tableId The table to connect to in the cluster.
   * @param readRowsRequest The {@link ReadRowsRequest} that will be used to filter the table.
   */
  public CloudBigtableScanConfiguration(String projectId, String zoneId, String clusterId,
      String tableId, ReadRowsRequest readRowsRequest) {
    this(projectId, zoneId, clusterId, tableId, readRowsRequest, Collections.<String, String> emptyMap());
  }

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, zone, cluster
   * ID, table ID, {@link Scan} and additional connection configuration.
   *
   * @param projectId The project ID for the cluster.
   * @param zoneId The zone where the cluster is located.
   * @param clusterId The cluster ID for the cluster.
   * @param tableId The table to connect to in the cluster.
   * @param scan The {@link Scan} that will be used to filter the table.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  public CloudBigtableScanConfiguration(String projectId, String zoneId, String clusterId,
      String tableId, ReadRowsRequest readRowsRequest, Map<String, String> additionalConfiguration) {
    super(projectId, zoneId, clusterId, tableId, additionalConfiguration);
    this.readRowsRequest = Preconditions.checkNotNull(readRowsRequest);
  }

  public ReadRowsRequest getReadRowsRequest() {
    return readRowsRequest;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects.equals(readRowsRequest, ((CloudBigtableScanConfiguration) obj).readRowsRequest);
  }

  @Override
  public Builder toBuilder() {
    return new Builder(getConfiguration())
        .withTableId(tableId)
        .witReadRowsRequest(readRowsRequest);
  }
}
