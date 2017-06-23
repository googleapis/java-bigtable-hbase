/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

// TODO: This was a copy of https://github.com/apache/beam/blob/release-2.0.0/sdks/java/io/hbase/src/main/java/org/apache/beam/sdk/io/hbase/SerializableScan.java
// If that class were public, we'd be able to use it here.
public class SerializableScan implements Serializable {
  private transient Scan scan;

  public SerializableScan() {
  }

  public SerializableScan(Scan scan) {
    set(scan);
  }

  public Scan get() {
    return scan;
  }

  public void set(Scan scan) {
    Preconditions.checkNotNull(scan, "Scan must not be null.");
    this.scan = scan;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    ProtobufUtil.toScan(scan).writeDelimitedTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    scan = ProtobufUtil.toScan(ClientProtos.Scan.parseDelimitedFrom(in));
  }
}
