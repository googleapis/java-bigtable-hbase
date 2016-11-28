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

import com.google.api.client.util.Clock;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.MutationCase;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 * Adapt an HBase {@link Put} Operation into a Bigtable {@link MutateRowRequest.Builder} or
 * {@link MutateRowsRequest.Entry}.
 * @author sduskis
 * @version $Id: $Id
 */
public class PutAdapter extends MutationAdapter<Put> {
  private final int maxKeyValueSize;
  private final boolean setClientTimestamp;

  @VisibleForTesting
  Clock clock = Clock.SYSTEM;

  /**
   * <p>Constructor for PutAdapter.</p>
   *
   * @param maxKeyValueSize a int.
   */
  public PutAdapter(int maxKeyValueSize) {
    this.maxKeyValueSize = maxKeyValueSize;
    this.setClientTimestamp = true;
  }

  /**
   * <p>Constructor for PutAdapter.</p>
   *
   * @param maxKeyValueSize a int.
   * @param setClientTimestamp a boolean.
   */
  public PutAdapter(int maxKeyValueSize, boolean setClientTimestamp) {
    this.maxKeyValueSize = maxKeyValueSize;
    this.setClientTimestamp = setClientTimestamp;
  }

  @Override
  protected Collection<Mutation> adaptMutations(Put operation) {
    if (operation.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }

    // Bigtable uses a 1ms granularity. Use this timestamp if the Put does not have one specified to
    // make mutations idempotent.
    long currentTimestampMicros = setClientTimestamp ? clock.currentTimeMillis() * 1000 : -1;
    final int rowLength = operation.getRow().length;

    List<Mutation> mutations = new ArrayList<>(operation.size());
    for (Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {
      ByteString familyString = ByteString.copyFrom(entry.getKey());
      int familySize = familyString.size();

      for (Cell cell : entry.getValue()) {
        int qualifierLength = cell.getQualifierLength();
        int valueLength = cell.getValueLength();
        // Since we are not using the interface involving KeyValues, we reconstruct how big they would be.
        // 20 bytes for metadata plus the length of all the elements.
        int keyValueSize = (20 + rowLength + familySize + qualifierLength + valueLength);
        if (maxKeyValueSize > 0 && keyValueSize > maxKeyValueSize) {
          throw new IllegalArgumentException("KeyValue size too large");
        }

        ByteString cellQualifierByteString = ByteString.copyFrom(
            cell.getQualifierArray(),
            cell.getQualifierOffset(),
            qualifierLength);

        ByteString value = ByteString.copyFrom(
            cell.getValueArray(),
            cell.getValueOffset(),
            valueLength);

        long timestampMicros = currentTimestampMicros;
        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
          timestampMicros = BigtableConstants.BIGTABLE_TIMEUNIT.convert(cell.getTimestamp(),
            BigtableConstants.HBASE_TIMEUNIT);
        }

        mutations.add(Mutation.newBuilder()
            .setSetCell(SetCell.newBuilder()
                .setFamilyNameBytes(familyString)
                .setColumnQualifier(cellQualifierByteString)
                .setValue(value)
                .setTimestampMicros(timestampMicros))
            .build());
      }
    }
    return mutations;
  }

  /**
   * <p>adapt.</p>
   *
   * @param request a {@link com.google.bigtable.v2.MutateRowRequest} object.
   * @return a {@link org.apache.hadoop.hbase.client.Put} object.
   * @throws java.io.IOException if any.
   */
  public Put adapt(MutateRowRequest request) throws IOException {
    if (request.getMutationsCount() == 0) {
      throw new IllegalArgumentException("No columns to insert");
    }

    byte[] rowkeyArray = request.getRowKey().toByteArray();
    Put put = new Put(rowkeyArray);
    for (Mutation mutation : request.getMutationsList()) {
      if (mutation.getMutationCase() != MutationCase.SET_CELL) {
        throw new IllegalArgumentException(
            "Cannot process mutation of type: " + mutation.getMutationCase());
      }
      SetCell setCell = mutation.getSetCell();
      long timestampHbase;
      if (setCell.getTimestampMicros() == -1) {
        timestampHbase = HConstants.LATEST_TIMESTAMP;
      } else {
        timestampHbase = BigtableConstants.HBASE_TIMEUNIT.convert(setCell.getTimestampMicros(),
          BigtableConstants.BIGTABLE_TIMEUNIT);
      }
      put.add(new RowCell(rowkeyArray, getBytes(setCell.getFamilyNameBytes()),
          getBytes(setCell.getColumnQualifier()), timestampHbase, getBytes(setCell.getValue())));
    }
    return put;
  }
}
