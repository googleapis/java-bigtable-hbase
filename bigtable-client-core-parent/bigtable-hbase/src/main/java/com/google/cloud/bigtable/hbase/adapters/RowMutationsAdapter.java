/*
 * Copyright 2015 Google LLC
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

import com.google.api.core.InternalApi;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;

/**
 * An adapter that adapts a {@link org.apache.hadoop.hbase.client.RowMutations} object into Google
 * Cloud Java {@link com.google.cloud.bigtable.data.v2.models.MutationApi}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class RowMutationsAdapter extends MutationAdapter<RowMutations> {

  protected final MutationAdapter<Mutation> mutationAdapter;

  /**
   * Constructor for RowMutationsAdapter.
   *
   * @param mutationAdapter a {@link MutationAdapter} object.
   */
  public RowMutationsAdapter(MutationAdapter<Mutation> mutationAdapter) {
    this.mutationAdapter = mutationAdapter;
  }

  /** {@inheritDoc} */
  @Override
  public void adapt(
      RowMutations operation, com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutationApi) {
    for (Mutation mutation : operation.getMutations()) {
      mutationAdapter.adapt(mutation, mutationApi);
    }
  }
}
