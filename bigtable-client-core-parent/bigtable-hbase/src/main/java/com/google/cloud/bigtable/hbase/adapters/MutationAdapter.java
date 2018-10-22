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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.protobuf.ByteString;

/**
 * Adapt an HBase {@link Mutation} Operation into a Google Cloud Java
 * {@link com.google.cloud.bigtable.data.v2.models.Mutation}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public abstract class MutationAdapter<T extends Row>
    implements OperationAdapter<T, com.google.cloud.bigtable.data.v2.models.MutationApi<?>> {

  protected static byte[] getBytes(ByteString bs) {
    return ByteStringer.extract(bs);
  }

  /** {@inheritDoc} */
  @Override
  public final void adapt(T operation, com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation) {
    adaptMutations(operation, mutation);
  }

  public final void toEntry(T operation, com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation) {
    adaptMutations(operation, mutation);
  }

  /**
   * Converts an HBase {@link Mutation} which represents a set of changes to a single row from an
   * HBase perspective to a Google Cloud Java {@link com.google.cloud.bigtable.data.v2.models.Mutation}
   * which represent the set of changes. The name "Mutation" represents a more granular change in
   * Bigtable than an HBase "Mutation"; An HBase {@link Cell} is akin to a Cloud Bigtable {@link
   * com.google.bigtable.v2.Mutation}. A CloudBigtable {@link MutateRowRequest} or {@link
   * com.google.bigtable.v2.MutateRowsRequest.Entry} is akin to an HBase {@link Mutation}.
   *
   * @param operation The HBase {@link Mutation} to convert
   * @param mutation The model {@link com.google.cloud.bigtable.data.v2.models.MutationApi<?>}
   * @return void
   */
  protected abstract void adaptMutations(
      T operation,
      com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation
  );
}
