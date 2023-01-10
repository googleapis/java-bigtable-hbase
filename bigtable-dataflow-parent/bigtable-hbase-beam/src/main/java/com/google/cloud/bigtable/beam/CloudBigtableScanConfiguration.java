/*
 * Copyright 2017 Google LLC
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

import com.google.bigtable.repackaged.com.google.api.core.InternalExtensionOnly;
import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowRange;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowSet;
import com.google.bigtable.repackaged.com.google.bigtable.v2.TableName;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Query;
import com.google.bigtable.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableMap;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.hbase.BigtableFixedProtoScan;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable instance; a table to connect to in the instance; and a filter on the table in the form
 * of a {@link Scan}.
 */
@InternalExtensionOnly
public class CloudBigtableScanConfiguration extends CloudBigtableTableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;
  protected static final String PLACEHOLDER_TABLE_ID = "PLACEHOLDER_TABLE_ID";
  protected static final String PLACEHOLDER_PROJECT_ID = "PLACEHOLDER_PROJECT_ID";
  protected static final String PLACEHOLDER_INSTANCE_ID = "PLACEHOLDER_INSTANCE_ID";
  protected static final String PLACEHOLDER_APP_PROFILE_ID = "PLACEHOLDER_APP_PROFILE_ID";

  enum ScanType {
    FIXED,
    SERIALIZABLE,
    HBASE
  }

  /**
   * Converts a {@link CloudBigtableTableConfiguration} object to a {@link
   * CloudBigtableScanConfiguration} that will perform the specified {@link Scan} on the table.
   *
   * @param config The {@link CloudBigtableTableConfiguration} object.
   * @param scan The {@link Scan} to add to the configuration.
   * @return The new {@link CloudBigtableScanConfiguration}.
   */
  public static CloudBigtableScanConfiguration fromConfig(
      CloudBigtableTableConfiguration config, Scan scan) {
    CloudBigtableScanConfiguration.Builder builder = new CloudBigtableScanConfiguration.Builder();
    config.copyConfig(builder);
    return builder.withScan(scan).build();
  }

  /**
   * Converts configuration map to a {@link CloudBigtableScanConfiguration}.
   *
   * @param projectId Value provider for project id
   * @param instanceId Value provider for instance id
   * @param tableId table id
   * @param scan The {@link Scan} to add to the configuration
   * @param configuration A map of all the configurations
   * @return
   */
  public static CloudBigtableScanConfiguration fromConfig(
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId,
      ValueProvider<String> tableId,
      ValueProvider<Scan> scan,
      Map<String, ValueProvider<String>> configuration) {
    CloudBigtableScanConfiguration.Builder builder = new CloudBigtableScanConfiguration.Builder();
    for (String key : configuration.keySet()) {
      if (!key.equals(BigtableOptionsFactory.PROJECT_ID_KEY)
          && !key.equals(BigtableOptionsFactory.INSTANCE_ID_KEY)) {
        builder.withConfiguration(key, configuration.get(key));
      }
    }
    return builder
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withTableId(tableId)
        .withScan(scan)
        .build();
  }

  /** Builds a {@link CloudBigtableScanConfiguration}. */
  public static class Builder extends CloudBigtableTableConfiguration.Builder {
    private ValueProvider<Scan> scan;

    public Builder() {}

    /**
     * Specifies the {@link Scan} that will be used to filter the table.
     *
     * @param scan The {@link Scan} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withScan(Scan scan) {
      return withScan(StaticValueProvider.of(scan));
    }

    /**
     * Specifies the {@link Scan} that will be used to filter the table.
     *
     * @param scan The {@link Scan} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withScan(ValueProvider<Scan> scan) {
      this.scan = scan;
      return this;
    }

    /**
     * @deprecated Please use {@link #withScan(Scan)} instead.
     *     <p>Specifies the {@link ReadRowsRequest} that will be used to filter the table.
     * @param request The {@link ReadRowsRequest} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    @Deprecated
    public Builder withRequest(ReadRowsRequest request) {
      return withScan(new BigtableFixedProtoValueProvider(projectId, instanceId, tableId, request));
    }

    /**
     * @deprecated Please use {@link #withScan(Scan)} instead.
     *     <p>Specifies the {@link ReadRowsRequest} that will be used to filter the table.
     * @param request The {@link ReadRowsRequest} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    @Deprecated
    public Builder withRequest(ValueProvider<ReadRowsRequest> request) {
      return withScan(new BigtableFixedProtoValueProvider(request));
    }

    /**
     * Internal API that allows a Source to configure the request with a new start/stop row range.
     *
     * @param startKey The first key, inclusive.
     * @param stopKey The last key, exclusive.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withKeys(byte[] startKey, byte[] stopKey) {
      Preconditions.checkNotNull(scan, "Scan cannot be empty.");
      // withKeys is never called from the template so this precondition is valid
      Preconditions.checkState(scan.isAccessible(), "Scan must be accessible.");
      ByteString start = ByteString.copyFrom(startKey);
      ByteString end = ByteString.copyFrom(stopKey);
      if (scan.get() instanceof BigtableFixedProtoScan) {
        // Keep the behavior from the previous implementation, create a new rowRange instead of
        // adding to the existing row ranges.
        ReadRowsRequest.Builder request =
            ((BigtableFixedProtoScan) scan.get()).getRequest().toBuilder();
        request.setRows(
            RowSet.newBuilder()
                .addRowRanges(RowRange.newBuilder().setStartKeyClosed(start).setEndKeyOpen(end)));
        return withRequest(request.build());
      } else if (scan instanceof BigtableFixedProtoValueProvider) {
        ReadRowsRequest.Builder request =
            ((BigtableFixedProtoValueProvider) scan).getRequest().toBuilder();
        request.setRows(
            RowSet.newBuilder()
                .addRowRanges(RowRange.newBuilder().setStartKeyClosed(start).setEndKeyOpen(end)));
        return withRequest(request.build());
      } else {
        return withScan(scan.get().withStartRow(startKey).withStopRow(stopKey));
      }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withProjectId(String)} so that it
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
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withProjectId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withProjectId(ValueProvider<String> projectId) {
      super.withProjectId(projectId);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Builder withInstanceId(String instanceId) {
      super.withInstanceId(instanceId);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Builder withInstanceId(ValueProvider<String> instanceId) {
      super.withInstanceId(instanceId);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Builder withAppProfileId(String appProfileId) {
      super.withAppProfileId(appProfileId);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Builder withAppProfileId(ValueProvider<String> appProfileId) {
      super.withAppProfileId(appProfileId);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Builder withConfiguration(String key, String value) {
      super.withConfiguration(key, value);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Builder withConfiguration(String key, ValueProvider<String> value) {
      super.withConfiguration(key, value);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withTableId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withTableId(String tableId) {
      super.withTableId(tableId);
      return this;
    }

    /**
     * {@inheritDoc} Overrides {@link CloudBigtableTableConfiguration.Builder#withTableId(String)}
     * so that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withTableId(ValueProvider<String> tableId) {
      super.withTableId(tableId);
      return this;
    }

    /**
     * Builds the {@link CloudBigtableScanConfiguration}.
     *
     * @return The new {@link CloudBigtableScanConfiguration}.
     */
    @Override
    public CloudBigtableScanConfiguration build() {
      if (scan == null) {
        // If scan is not set, default it to a full table scan
        this.scan =
            StaticValueProvider.of(
                new BigtableFixedProtoScan(
                    ReadRowsRequest.newBuilder()
                        .setTableName(
                            TableName.format(projectId.get(), instanceId.get(), tableId.get()))
                        .setRows(
                            RowSet.newBuilder().addRowRanges(RowRange.newBuilder().build()).build())
                        .build()));
      }
      return new CloudBigtableScanConfiguration(
          projectId, instanceId, tableId, scan, additionalConfiguration);
    }
  }

  private final ValueProvider<Scan> scan;

  /**
   * Provides an updated request by setting the table name in the existing request if the table name
   * wasn't set.
   */
  private static class ScanWithTableNameValueProvider implements ValueProvider<Scan>, Serializable {
    private final ValueProvider<String> projectId;
    private final ValueProvider<String> instanceId;
    private final ValueProvider<String> tableId;
    private final ValueProvider<Scan> scanValueProvider;
    private Scan cachedScan;

    ScanWithTableNameValueProvider(
        ValueProvider<String> projectId,
        ValueProvider<String> instanceId,
        ValueProvider<String> tableId,
        ValueProvider<Scan> scan) {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
      this.scanValueProvider = scan;
    }

    @Override
    public Scan get() {
      if (cachedScan == null) {
        if (scanValueProvider == null) {
          cachedScan = new Scan();
          cachedScan.setMaxVersions(Integer.MAX_VALUE);
        } else {
          cachedScan = scanValueProvider.get();
        }
      }
      return cachedScan;
    }

    @Override
    public boolean isAccessible() {
      return projectId.isAccessible()
          && instanceId.isAccessible()
          && tableId.isAccessible()
          && scanValueProvider.isAccessible();
    }

    @Override
    public String toString() {
      if (isAccessible()) {
        return String.valueOf(get());
      }
      return VALUE_UNAVAILABLE;
    }
  }

  /** Value provider for {@link SerializableScan}. */
  static class SerializableScanValueProvider implements ValueProvider<Scan>, Serializable {
    private final ValueProvider<String> projectId;
    private final ValueProvider<String> instanceId;
    private final ValueProvider<String> tableId;
    private final ValueProvider<Scan> scanValueProvider;
    private SerializableScan cachedScan;

    SerializableScanValueProvider(
        ValueProvider<String> projectId,
        ValueProvider<String> instanceId,
        ValueProvider<String> tableId,
        ValueProvider<Scan> scan) {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
      this.scanValueProvider = scan;
    }

    @Override
    public Scan get() {
      if (cachedScan == null) {
        cachedScan = ((SerializableScan) scanValueProvider.get());
      }
      return cachedScan;
    }

    @Override
    public boolean isAccessible() {
      return projectId.isAccessible()
          && instanceId.isAccessible()
          && tableId.isAccessible()
          && scanValueProvider.isAccessible();
    }

    @Override
    public String toString() {
      if (isAccessible()) {
        return String.valueOf(get());
      }
      return VALUE_UNAVAILABLE;
    }
  }

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, instance ID,
   * table ID, {@link Scan} and additional connection configuration.
   *
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID.
   * @param tableId The table to connect to in the instance.
   * @param scan The {@link Scan} that will be used to filter the table.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  protected CloudBigtableScanConfiguration(
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId,
      ValueProvider<String> tableId,
      ValueProvider<Scan> scan,
      Map<String, ValueProvider<String>> additionalConfiguration) {
    super(projectId, instanceId, tableId, additionalConfiguration);
    if (scan instanceof SerializableScanValueProvider
        || (scan instanceof StaticValueProvider && scan.get() instanceof SerializableScan)) {
      this.scan = new SerializableScanValueProvider(projectId, instanceId, tableId, scan);
    } else {
      this.scan = new ScanWithTableNameValueProvider(projectId, instanceId, tableId, scan);
    }
  }

  /**
   * Gets the {@link Scan} used to filter the table.
   *
   * @return The {@link Scan}.
   */
  @Deprecated
  public ReadRowsRequest getRequest() {
    Preconditions.checkNotNull(scan, "Scan cannot be empty.");
    Preconditions.checkState(scan.isAccessible(), "Scan must be accessible.");
    if (scan instanceof BigtableFixedProtoValueProvider) {
      return ((BigtableFixedProtoValueProvider) scan).getRequest();
    } else if (scan.get() instanceof BigtableFixedProtoScan) {
      return ((BigtableFixedProtoScan) scan.get()).getRequest();
    } else {
      Scan hbaseScan = null;
      if (scan.get() instanceof SerializableScan) {
        hbaseScan = ((SerializableScan) scan.get()).toScan();
      }
      ReadHooks readHooks = new DefaultReadHooks();
      Query query = Query.create(getTableId());
      query =
          Adapters.SCAN_ADAPTER.adapt(hbaseScan == null ? scan.get() : hbaseScan, readHooks, query);
      readHooks.applyPreSendHook(query);
      return query.toProto(
          RequestContext.create(getProjectId(), getInstanceId(), getAppProfileId()));
    }
  }

  public ValueProvider<Scan> getScan() {
    return scan;
  }

  /** @return The start row for this configuration. */
  public byte[] getStartRow() {
    return getStartRowByteString().toByteArray();
  }

  /** @return The stop row for this configuration. */
  public byte[] getStopRow() {
    return getStopRowByteString().toByteArray();
  }

  /** @return The start row for this configuration. */
  byte[] getZeroCopyStartRow() {
    return ByteStringer.extract(getStartRowByteString());
  }

  /** @return The stop row for this configuration. */
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
    builder.withRequest(getRequest());
  }

  /**
   * Creates a {@link ByteKeyRange} representing the start and stop keys for this instance.
   *
   * @return A {@link ByteKeyRange}.
   */
  public ByteKeyRange toByteKeyRange() {
    return ByteKeyRange.of(
        ByteKey.copyFrom(getZeroCopyStartRow()), ByteKey.copyFrom(getZeroCopyStopRow()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("scan", getDisplayValue(scan)).withLabel("Scan"));
  }

  /**
   * The writeReplace method allows the developer to provide a replacement object that will be
   * serialized instead of the original one. We use this to keep the enclosed class immutable. For
   * more details on the technique see <a
   * href="https://lingpipe-blog.com/2009/08/10/serializing-immutable-singletons-serialization-proxy/">this
   * article</a>.
   */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Serializable {

    private String projectId;
    private String instanceId;
    private String tableId;
    private Scan scan;
    private ImmutableMap<String, ValueProvider<String>> additionalConfiguration;

    public SerializationProxy(CloudBigtableScanConfiguration configuration) {
      this.projectId = configuration.getProjectId();
      this.instanceId = configuration.getInstanceId();
      this.tableId = configuration.getTableId();
      this.scan = configuration.getScan().get();
      Map<String, ValueProvider<String>> map = new HashMap<>();
      map.putAll(configuration.getConfiguration());
      map.remove(BigtableOptionsFactory.PROJECT_ID_KEY);
      map.remove(BigtableOptionsFactory.INSTANCE_ID_KEY);
      this.additionalConfiguration =
          new ImmutableMap.Builder<String, ValueProvider<String>>().putAll(map).build();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      StringUtf8Coder.of().encode(projectId, out);
      StringUtf8Coder.of().encode(instanceId, out);
      StringUtf8Coder.of().encode(tableId, out);
      out.writeObject(additionalConfiguration);
      if (scan instanceof BigtableFixedProtoScan) {
        out.writeObject(ScanType.FIXED);
        out.writeObject(((BigtableFixedProtoScan) scan).getRequest());
      } else if (scan instanceof SerializableScan) {
        out.writeObject(ScanType.SERIALIZABLE);
        out.writeObject(scan);
      } else {
        out.writeObject(ScanType.HBASE);
        ProtobufUtil.toScan(scan).writeDelimitedTo(out);
      }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      projectId = StringUtf8Coder.of().decode(in);
      instanceId = StringUtf8Coder.of().decode(in);
      tableId = StringUtf8Coder.of().decode(in);
      additionalConfiguration = (ImmutableMap<String, ValueProvider<String>>) in.readObject();
      ScanType scanType = (ScanType) in.readObject();
      if (scanType == ScanType.FIXED) {
        ReadRowsRequest request = (ReadRowsRequest) in.readObject();
        scan = new BigtableFixedProtoScan(request);
      } else if (scanType == ScanType.SERIALIZABLE) {
        scan = (SerializableScan) in.readObject();
      } else {
        scan = ProtobufUtil.toScan(ClientProtos.Scan.parseDelimitedFrom(in));
      }
    }

    Object readResolve() {
      return new CloudBigtableScanConfiguration(
          StaticValueProvider.of(projectId),
          StaticValueProvider.of(instanceId),
          StaticValueProvider.of(tableId),
          StaticValueProvider.of(scan),
          additionalConfiguration);
    }
  }
}
