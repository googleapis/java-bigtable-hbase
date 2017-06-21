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
package com.google.cloud.bigtable.beam.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

/**
 * When Dataflow notices a slowdown in executing Puts and Deletes, it will send those Puts and
 * Deletes to be processed on another Dataflow machine. This class handles the serialization and
 * deserialization of the Puts and Deletes.
 *
 * <p> See CloudBigtableIO#initializeForWrite(Pipeline).
 */
public class HBaseMutationCoder extends AtomicCoder<Mutation> {

  private static final long serialVersionUID = -3853654063196018580L;

  public static Coder<?> of() {
    return new HBaseMutationCoder();
  }

  @Override
  public void encode(Mutation mutation, OutputStream outStream) throws CoderException, IOException {
    MutationType type = getType(mutation);
    MutationProto proto = ProtobufUtil.toMutation(type, mutation);
    proto.writeDelimitedTo(outStream);
  }

  private static MutationType getType(Mutation mutation) {
    if (mutation instanceof Put) {
      return MutationType.PUT;
    } else if (mutation instanceof Delete) {
      return MutationType.DELETE;
    } else {
      // Increment and Append are not idempotent.  They should not be used in distributed jobs.
      throw new IllegalArgumentException("Only Put and Delete are supported");
    }
  }

  @Override
  public Mutation decode(InputStream inStream) throws CoderException, IOException {
    return ProtobufUtil.toMutation(MutationProto.parseDelimitedFrom(inStream));
  }
}