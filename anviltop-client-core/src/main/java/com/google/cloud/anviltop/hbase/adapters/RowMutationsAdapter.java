/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;


import com.google.bigtable.anviltop.AnviltopData;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;

/**
 * An adapter that adapts a @{link RowMutations} object into an Anviltop
 * @{link AnviltopData.RowMutation.Builder}.
 */
public class RowMutationsAdapter {

  protected final OperationAdapter<Mutation, AnviltopData.RowMutation.Builder> mutationAdapter;

  public RowMutationsAdapter(
      OperationAdapter<Mutation, AnviltopData.RowMutation.Builder> mutationAdapter) {
    this.mutationAdapter = mutationAdapter;
  }

  public AnviltopData.RowMutation.Builder adapt(RowMutations mutations) {
    AnviltopData.RowMutation.Builder result = AnviltopData.RowMutation.newBuilder();
    result.setRowKey(ByteString.copyFrom(mutations.getRow()));

    for (Mutation mutation : mutations.getMutations()) {
      AnviltopData.RowMutation.Builder anviltopMutation = mutationAdapter.adapt(mutation);
      result.addAllMods(anviltopMutation.getModsList());
    }

    return result;
  }
}
