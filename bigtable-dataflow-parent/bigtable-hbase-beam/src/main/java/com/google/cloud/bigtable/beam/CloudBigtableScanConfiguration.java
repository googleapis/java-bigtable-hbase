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

import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hadoop.hbase.client.Scan;

import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowRange;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowSet;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.util.ByteStringer;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable instance; a table to connect to in the instance; and a filter on the table in the form of
 * a {@link Scan}.
 */
public class CloudBigtableScanConfiguration extends CloudBigtableTableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  /**
   * Converts a {@link CloudBigtableTableConfiguration} object to a
   * {@link CloudBigtableScanConfiguration} that will perform the specified {@link Scan} on the
   * table.
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
    @Deprecated
    private Scan scan;
    @Deprecated
    private ReadRowsRequest request;

    private String startRow;
    private String stopRow;
    private String filter;
    private int maxVersion;  // Should this be int or long?

    public Builder() {
    }

    /**
     * Specifies the {@link Scan} that will be used to filter the table.
     * @param scan The {@link Scan} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    @Deprecated
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
    @Deprecated
    public Builder withRequest(ReadRowsRequest request) {
      this.request = request;
      this.scan = null;
      return this;
    }

    /**
     * Internal API that allows a Source to configure the request with a new start/stop row range.
     * @param startKey The first key, inclusive.
     * @param stopKey The last key, exclusive.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    @Deprecated
    Builder withKeys(byte[] startKey, byte[] stopKey) {
      final ByteString start = ByteStringer.wrap(startKey);
      final ByteString stop = ByteStringer.wrap(stopKey);
      request =
          request.toBuilder()
              .setRows(RowSet.newBuilder().addRowRanges(
                RowRange.newBuilder().setStartKeyClosed(start).setEndKeyOpen(stop).build()))
              .build();
      return this;
    }

    /**
     * @param startRow
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withStartRow(String startRow) {
      this.startRow = startRow;
      return this;
    }

    /**
     * @param stopRow
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withStopRow(String stopRow) {
      this.stopRow = stopRow;
      return this;
    }

    /**
     * @param filter
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withFilter(String filter) {
      this.filter = filter;
      return this;
    }

    /**
     * @param maxVersion
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withMaxVersion(int maxVersion) {
      this.maxVersion = maxVersion;
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
     * {@inheritDoc} Overrides {@link CloudBigtableScanConfiguration.Builder#withTableId(String)} so
     * that it returns {@link CloudBigtableScanConfiguration.Builder}.
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
        if (scan == null
            && startRow == null
            && stopRow == null
            && filter == null
            && maxVersion == 0) {
          scan = new Scan();
        }
        ReadHooks readHooks = new DefaultReadHooks();
        ReadRowsRequest.Builder builder = Adapters.SCAN_ADAPTER.adapt(scan, readHooks);
        request = readHooks.applyPreSendHook(builder.build());
      }
      return new CloudBigtableScanConfiguration(projectId, instanceId, tableId,
          request, startRow, stopRow, filter, maxVersion, additionalConfiguration);
    }
  }

  @Deprecated
  private final ReadRowsRequest request;

  private String startRow;
  private String stopRow;
  private String filter;
  private int maxVersion;  // Should this be int or long?

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, instance ID,
   * table ID, {@link Scan} and additional connection configuration.
   *
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID.
   * @param tableId The table to connect to in the instance.
   * @param request The {@link ReadRowsRequest} that will be used to filter the table.
   * @param startRow
   * @param stopRow
   * @param filter
   * @param maxVersion
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  protected CloudBigtableScanConfiguration(
      String projectId,
      String instanceId,
      String tableId,
      ReadRowsRequest request,
      String startRow,
      String stopRow,
      String filter,
      int maxVersion,
      Map<String, String> additionalConfiguration) {
    super(projectId, instanceId, tableId, additionalConfiguration);
    this.request = request;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.filter = filter;
    this.maxVersion = maxVersion;
  }

  /**
   * Gets the {@link Scan} used to filter the table.
   *
   * @return The {@link Scan}.
   */
  public ReadRowsRequest getRequest() {
    ReadRowsRequest toBeReturnedRequest;
    if (request == null) {
      // TODO: Build request based on startRow, stopeRow, filter and maxVersion.
      toBeReturnedRequest = ReadRowsRequest.getDefaultInstance();
    } else {
      toBeReturnedRequest = request;
    }

    if (toBeReturnedRequest.getTableName().isEmpty()) {
      BigtableInstanceName bigtableInstanceName =
          new BigtableInstanceName(getProjectId(), getInstanceId());
      String fullTableName = bigtableInstanceName.toTableNameStr(tableId);
      toBeReturnedRequest = toBeReturnedRequest.toBuilder().setTableName(fullTableName).build();
    }
    return toBeReturnedRequest;
  }

  /**
   * @return The start row for this configuration.
   */
  public byte[] getStartRow() {
    return getStartRowByteString().toByteArray();
  }

  /**
   * @return The stop row for this configuration.
   */
  public byte[] getStopRow() {
    return getStopRowByteString().toByteArray();
  }

  /**
   * @return The start row for this configuration.
   */
  byte[] getZeroCopyStartRow() {
    return ByteStringer.extract(getStartRowByteString());
  }

  /**
   * @return The stop row for this configuration.
   */
  byte[] getZeroCopyStopRow() {
    return ByteStringer.extract(getStopRowByteString());
  }

  ByteString getStartRowByteString() {
    return getRowRange().getStartKeyClosed();
  }

  ByteString getStopRowByteString() {
    return getRowRange().getEndKeyOpen();
  }

  RowRange getRowRange() {
    RowSet rows = getRequest().getRows();
    return rows.getRowRanges(0);
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects.equals(getRequest(), ((CloudBigtableScanConfiguration) obj).getRequest());
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
    builder.withStartRow(startRow);
    builder.withStopRow(stopRow);
    builder.withFilter(filter);
    builder.withMaxVersion(maxVersion);
  }

  /**
   * Creates a {@link ByteKeyRange} representing the start and stop keys for this instance.
   * @return A {@link ByteKeyRange}.
   */
  public ByteKeyRange toByteKeyRange() {
    return ByteKeyRange.of(ByteKey.copyFrom(getZeroCopyStartRow()),
      ByteKey.copyFrom(getZeroCopyStopRow()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(
        DisplayData.item("readRowsRequest", getRequest().toString()).withLabel("ReadRowsRequest"));
  }
}
