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
package com.google.cloud.bigtable.dataflow.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation.MutationCase;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;

/**
 * When Dataflow notices a slowdown in executing Puts and Deletes, it will send those Puts and
 * Deletes to be processed on another Dataflow machine. This class handles the serialization and
 * deserialization of the Puts and Deletes.
 *
 * See {@link CloudBigtableIO#initializeForWrite(Pipeline)}.
 */
public class HBaseMutationConverter implements BigtableConverter<Mutation> {

  private static final long serialVersionUID = -3853654063196018580L;
  private static final PutAdapter PUT_ADAPTER = new PutAdapter(Integer.MAX_VALUE);

  @Override
  public void encode(Mutation mutation, OutputStream outStream) throws IOException {
    MutateRowRequest request;
    if (mutation instanceof Put) {
      request = PUT_ADAPTER.adapt((Put) mutation).build();
    } else if (mutation instanceof Delete) {
      request = Adapters.DELETE_ADAPTER.adapt((Delete) mutation).build();
    } else {
      // Increment and Append are not idempotent.  They should not be used in distributed jobs.
      throw new IllegalArgumentException("Only Put and Delete are supported");
    }
    request.writeDelimitedTo(outStream);
  }

  @Override
  public Mutation decode(InputStream inStream) throws IOException {
    MutateRowRequest request = MutateRowRequest.parseDelimitedFrom(inStream);
    if (request.getMutationsCount() == 0) {
      // Increment and Append are not idempotent.  They should not be used in distributed jobs.
      throw new IllegalArgumentException("Invalid MutateRowRequest");
    }
    if(request.getMutations(0).getMutationCase() == MutationCase.SET_CELL) {
      return PUT_ADAPTER.adapt(request);
    } else {
      return Adapters.DELETE_ADAPTER.adapt(request);
    }
  }
}