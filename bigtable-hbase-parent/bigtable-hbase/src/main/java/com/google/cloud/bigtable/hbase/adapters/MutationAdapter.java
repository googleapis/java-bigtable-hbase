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

import java.util.Collection;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

/**
 * Adapt an HBase {@link Mutation} Operation into a Bigtable {@link MutateRowRequest.Builder} or
 * {@link MutateRowsRequest.Entry}.
 * @author sduskis
 * @version $Id: $Id
 */

public abstract class MutationAdapter<T extends Row>
    implements OperationAdapter<T, MutateRowRequest.Builder> {

  protected static byte[] getBytes(ByteString bs) {
    return ByteStringer.extract(bs);
  }

  /** {@inheritDoc} */
  @Override
  public final MutateRowRequest.Builder adapt(T operation) {
    return MutateRowRequest.newBuilder()
        .setRowKey(ByteString.copyFrom(operation.getRow()))
        .addAllMutations(adaptMutations(operation));
  }

  public final MutateRowsRequest.Entry toEntry(T operation) {
    return MutateRowsRequest.Entry.newBuilder()
        .setRowKey(ByteString.copyFrom(operation.getRow()))
        .addAllMutations(adaptMutations(operation)).build();
  }

  /**
   * Converts an HBase {@link Mutation} which represents a set of changes to a single row from an
   * HBase perspective to a collection of Cloud Bigtable {@link com.google.bigtable.v2.Mutation}
   * which represent the set of changes. The name "Mutation" represents a more granular change in
   * Bigtable than an HBase "Mutation"; An HBase {@link Cell} is akin to a Cloud Bigtable
   * {@link com.google.bigtable.v2.Mutation}. A CloudBigtable {@link MutateRowRequest} or
   * {@link MutateRowsRequest.Entry} is akin to an HBase {@link Mutation}.
   *
   * @param operation The HBase {@link Mutation} to convert
   * @return a {@link Collection} of Cloud Bigtable {@link}
   */
  protected abstract Collection<com.google.bigtable.v2.Mutation> adaptMutations(T operation);
}
