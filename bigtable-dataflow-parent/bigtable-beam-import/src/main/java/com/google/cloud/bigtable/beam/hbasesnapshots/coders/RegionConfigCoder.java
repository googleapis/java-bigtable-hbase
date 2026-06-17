/*
 * Copyright 2026 Google LLC
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
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

  private static final Coder<SnapshotConfig> snapshotConfigCoder =
      SerializableCoder.of(SnapshotConfig.class);
  private static final Coder<byte[]> byteArrayCoder = ByteArrayCoder.of();

  @Override
  public void encode(RegionConfig value, OutputStream outStream) throws IOException {
    // 1. Encode SnapshotConfig using standard SerializableCoder
    snapshotConfigCoder.encode(value.getSnapshotConfig(), outStream);

    // 2. Encode RegionInfo by converting to HBase protobuf and then to byte array
    HBaseProtos.RegionInfo regionInfo = ProtobufUtil.toRegionInfo(value.getRegionInfo());
    byteArrayCoder.encode(regionInfo.toByteArray(), outStream);

    // 3. Encode TableDescriptor by converting to HBase protobuf and then to byte array
    HBaseProtos.TableSchema tableSchema = ProtobufUtil.toTableSchema(value.getTableDescriptor());
    byteArrayCoder.encode(tableSchema.toByteArray(), outStream);

    // 4. Encode region size using variable-length long coder
    longCoder.encode(value.getRegionSize(), outStream);
  }

  @Override
  public RegionConfig decode(InputStream inStream) throws IOException {
    // 1. Decode SnapshotConfig
    SnapshotConfig snapshotConfig = snapshotConfigCoder.decode(inStream);

    // 2. Decode RegionInfo from bytes via HBase protobuf
    byte[] regionInfoBytes = byteArrayCoder.decode(inStream);
    RegionInfo regionInfo =
        ProtobufUtil.toRegionInfo(HBaseProtos.RegionInfo.parseFrom(regionInfoBytes));

    // 3. Decode TableDescriptor from bytes via HBase protobuf
    byte[] tableSchemaBytes = byteArrayCoder.decode(inStream);
    TableDescriptor tableDescriptor =
        ProtobufUtil.toTableDescriptor(TableSchema.parseFrom(tableSchemaBytes));

    // 4. Decode region size
    Long regionSize = longCoder.decode(inStream);

    return RegionConfig.builder()
        .setSnapshotConfig(snapshotConfig)
        .setRegionInfo(regionInfo)
        .setTableDescriptor(tableDescriptor)
        .setRegionSize(regionSize)
        .build();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {
    throw new Coder.NonDeterministicException(
        this,
        "RegionConfigCoder is non-deterministic because it encodes SnapshotConfig using"
            + " SerializableCoder.");
  }
}
