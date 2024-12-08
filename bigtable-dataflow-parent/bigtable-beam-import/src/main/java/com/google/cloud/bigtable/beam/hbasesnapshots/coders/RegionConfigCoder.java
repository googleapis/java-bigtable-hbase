/*
 * Copyright 2024 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.coders;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.*;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;

/** Implementation of {@link Coder} for encoding and decoding of {@link RegionConfig} */
@InternalApi("For internal usage only")
public class RegionConfigCoder extends Coder<RegionConfig> {
  private static final VarLongCoder longCoder = VarLongCoder.of();

  @Override
  public void encode(RegionConfig value, OutputStream outStream) throws IOException {
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
    objectOutputStream.writeObject(value.getSnapshotConfig());

    HBaseProtos.RegionInfo regionInfo = ProtobufUtil.toRegionInfo(value.getRegionInfo());
    ByteArrayOutputStream boas1 = new ByteArrayOutputStream();
    regionInfo.writeTo(boas1);
    objectOutputStream.writeObject(boas1.toByteArray());

    HBaseProtos.TableSchema tableSchema = ProtobufUtil.toTableSchema(value.getTableDescriptor());
    ByteArrayOutputStream boas2 = new ByteArrayOutputStream();
    tableSchema.writeTo(boas2);
    objectOutputStream.writeObject(boas2.toByteArray());

    longCoder.encode(value.getRegionSize(), outStream);
  }

  @Override
  public RegionConfig decode(InputStream inStream) throws IOException {
    ObjectInputStream objectInputStream = new ObjectInputStream(inStream);
    SnapshotConfig snapshotConfig;
    try {
      snapshotConfig = (SnapshotConfig) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new CoderException("Failed to deserialize RestoredSnapshotConfig", e);
    }

    RegionInfo regionInfoProto = null;
    try {
      regionInfoProto =
          ProtobufUtil.toRegionInfo(
              HBaseProtos.RegionInfo.parseFrom((byte[]) objectInputStream.readObject()));
    } catch (ClassNotFoundException e) {
      throw new CoderException("Failed to parse regionInfo", e);
    }

    TableDescriptor tableSchema = null;
    try {
      tableSchema =
          ProtobufUtil.toTableDescriptor(
              TableSchema.parseFrom((byte[]) objectInputStream.readObject()));
    } catch (ClassNotFoundException e) {
      throw new CoderException("Failed to parse tableSchema", e);
    }

    Long regionsize = longCoder.decode(inStream);

    return RegionConfig.builder()
        .setSnapshotConfig(snapshotConfig)
        .setRegionInfo(regionInfoProto)
        .setTableDescriptor(tableSchema)
        .setRegionSize(regionsize)
        .build();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {}
}
