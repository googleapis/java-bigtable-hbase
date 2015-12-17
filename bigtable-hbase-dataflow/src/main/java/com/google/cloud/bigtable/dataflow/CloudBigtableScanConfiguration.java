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

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
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
    return new CloudBigtableScanConfiguration(
        options.getBigtableProjectId(),
        options.getBigtableZoneId(),
        options.getBigtableClusterId(),
        options.getBigtableTableId(),
        scan);
  }

  /**
   * Builds a {@link CloudBigtableScanConfiguration}.
   */
  public static class Builder extends CloudBigtableTableConfiguration.Builder {
    protected Scan scan = new Scan();

    public Builder() {
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
      return new CloudBigtableScanConfiguration(projectId, zoneId, clusterId, tableId, scan);
    }
  }

  /**
   * HBase's {@link Scan} does not implement {@link Serializable}.  This class provides a wrapper
   * around {@link Scan} to properly serialize it.
   */
  private static class SerializableScan implements Serializable {
    private static final long serialVersionUID = 1998373680347356757L;
    private transient Scan scan;

    public SerializableScan(Scan scan) {
      this.scan = scan;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.writeObject(toEncodedString());
    }

    private String toEncodedString() throws IOException {
      return Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      this.scan = toScan((String) in.readObject());
    }

    private static Scan toScan(String scanStr) throws IOException {
      try {
        return ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(Base64.decode(scanStr)));
      } catch (InvalidProtocolBufferException ipbe) {
        throw new IOException("Could not deserialize the Scan.", ipbe);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof SerializableScan)) {
        return false;
      }
      SerializableScan other = (SerializableScan) obj;
      try {
        return toEncodedString().equals(other.toEncodedString());
      } catch (IOException e) {
        throw new RuntimeException("Could not check SerializableScan equality", e);
      }
    }
  }

  private SerializableScan serializableScan;

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, zone, cluster
   * ID, table ID and {@link Scan}.
   *
   * @param projectId The project ID for the cluster.
   * @param zoneId The zone where the cluster is located.
   * @param clusterId The cluster ID for the cluster.
   * @param tableId The table to connect to in the cluster.
   * @param scan The {@link Scan} that will be used to filter the table.
   */
  public CloudBigtableScanConfiguration(String projectId, String zoneId, String clusterId,
      String tableId, Scan scan) {
    this(projectId, zoneId, clusterId, tableId, scan, Collections.<String, String> emptyMap());
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
      String tableId, Scan scan, Map<String, String> additionalConfiguration) {
    super(projectId, zoneId, clusterId, tableId, additionalConfiguration);
    this.serializableScan = new SerializableScan(scan);
  }

  /**
   * Gets the {@link Scan} used to filter the table.
   * @return The {@link Scan}.
   */
  public Scan getScan() {
    return serializableScan.scan;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects
            .equals(serializableScan, ((CloudBigtableScanConfiguration) obj).serializableScan);
  }

  @Override
  public Builder toBuilder() {
    return new Builder(getConfiguration())
        .withTableId(tableId)
        .withScan(serializableScan.scan);
  }
}
