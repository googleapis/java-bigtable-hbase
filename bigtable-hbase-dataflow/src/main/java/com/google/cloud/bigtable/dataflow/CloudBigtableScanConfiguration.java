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

/**
 * This class encapsulates the metadata required to create a connection to a Bigtable cluster,
 * and a specific table therein along with a filter on the table in the form of a {@link Scan}.
 * NOTE: {@link Scan} isn't Serializable, so this class uses HBase's {@link ProtobufUtil} as part
 * of the {@Externalizable} implementation.
 */
public class CloudBigtableScanConfiguration extends CloudBigtableTableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  public static CloudBigtableScanConfiguration fromCBTOptions(CloudBigtableOptions options) {
    return fromCBTOptions(options, new Scan());
  }

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
  public static class Builder extends CloudBigtableTableConfiguration.Builder<Builder> {
    protected Scan scan = new Scan();

    public Builder withScan(Scan scan) {
      this.scan = scan;
      return this;
    }

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
      out.writeObject(Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
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
  }

  private SerializableScan serializableScan;

  public CloudBigtableScanConfiguration(String projectId, String zone, String cluster,
      String table, Scan scan) {
    super(projectId, zone, cluster, table);
    this.serializableScan = new SerializableScan(scan);
  }

  public Scan getScan() {
    return serializableScan.scan;
  }
}
