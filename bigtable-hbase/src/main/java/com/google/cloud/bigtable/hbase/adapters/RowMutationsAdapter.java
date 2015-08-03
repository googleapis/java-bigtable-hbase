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
package com.google.cloud.bigtable.hbase.adapters;


import com.google.bigtable.v1.MutateRowRequest;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;

/**
 * An adapter that adapts a {@link RowMutations} object into an Bigtable
 * {@link MutateRowRequest}.
 */
public class RowMutationsAdapter {

  protected final OperationAdapter<Mutation,
      MutateRowRequest.Builder> mutationAdapter;

  public RowMutationsAdapter(
      OperationAdapter<Mutation, MutateRowRequest.Builder> mutationAdapter) {
    this.mutationAdapter = mutationAdapter;
  }

  public MutateRowRequest.Builder adapt(RowMutations mutations) {
    MutateRowRequest.Builder result = MutateRowRequest.newBuilder();

    result.setRowKey(ByteString.copyFrom(mutations.getRow()));

    for (Mutation mutation : mutations.getMutations()) {
      MutateRowRequest.Builder bigtableBuilder = mutationAdapter.adapt(mutation);
      result.addAllMutations(bigtableBuilder.getMutationsList());
    }

    return result;
  }
}
