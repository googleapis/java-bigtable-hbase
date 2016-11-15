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

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * An interface for adapters that will convert an HBase Operation into an Bigtable
 * @param <T> The HBase operation type
 * @param <U> The Bigtable message type
 * @author sduskis
 * @version $Id: $Id
 */
public abstract class MutationAdapter<T extends org.apache.hadoop.hbase.client.Mutation>
    implements OperationAdapter<T, MutateRowRequest.Builder> {

  protected static byte[] getBytes(ByteString bs) {
    return ByteStringer.extract(bs);
  }

  /**
   * Adapt a single HBase Operation to a single @{link MutateRowRequest}.
   * @param operation The HBase operation to convert.
   * @return An equivalent Bigtable
   */
  @Override
  public MutateRowRequest.Builder adapt(T operation) {
    return MutateRowRequest.newBuilder()
        .setRowKey(getRowByteString(operation))
        .addAllMutations(toMutationList(operation));
  }

  public MutateRowsRequest.Entry.Builder adaptToBulkEntry(T operation) {
    return MutateRowsRequest.Entry.newBuilder()
        .setRowKey(getRowByteString(operation))
        .addAllMutations(toMutationList(operation));
  }

  private ByteString getRowByteString(T operation) {
    final byte[] row = getRow(operation);
    return row == null ? ByteString.EMPTY : ByteString.copyFrom(row);
  }

  protected abstract byte[] getRow(T operation);

  protected abstract List<Mutation> toMutationList(T operation);
}
