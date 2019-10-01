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
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;

/**
 * Adapt an HBase {@link Put} Operation into a Google Cloud Java {@link
 * com.google.cloud.bigtable.data.v2.models.MutationApi}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class PutAdapter extends MutationAdapter<Put> {
  private final int maxKeyValueSize;
  private final boolean setClientTimestamp;

  @VisibleForTesting Clock clock = Clock.SYSTEM;

  /**
   * Constructor for PutAdapter.
   *
   * @param maxKeyValueSize a int.
   */
  public PutAdapter(int maxKeyValueSize) {
    this.maxKeyValueSize = maxKeyValueSize;
    this.setClientTimestamp = true;
  }

  /**
   * Constructor for PutAdapter.
   *
   * @param maxKeyValueSize a int.
   * @param setClientTimestamp a boolean.
   */
  public PutAdapter(int maxKeyValueSize, boolean setClientTimestamp) {
    this.maxKeyValueSize = maxKeyValueSize;
    this.setClientTimestamp = setClientTimestamp;
  }

  PutAdapter withServerSideTimestamps() {
    return new PutAdapter(maxKeyValueSize, false);
  }

  /** {@inheritDoc} */
  @Override
  public void adapt(
      Put operation, com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation) {
    if (operation.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }

    // Bigtable uses a 1ms granularity. Use this timestamp if the Put does not have one specified to
    // make mutations idempotent.
    long currentTimestampMicros = setClientTimestamp ? clock.currentTimeMillis() * 1000 : -1;
    final int rowLength = operation.getRow().length;

    for (Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {
      ByteString familyString = ByteString.copyFrom(entry.getKey());
      int familySize = familyString.size();

      for (Cell cell : entry.getValue()) {
        int qualifierLength = cell.getQualifierLength();
        int valueLength = cell.getValueLength();
        // Since we are not using the interface involving KeyValues, we reconstruct how big they
        // would be.
        // 20 bytes for metadata plus the length of all the elements.
        int keyValueSize = (20 + rowLength + familySize + qualifierLength + valueLength);
        if (maxKeyValueSize > 0 && keyValueSize > maxKeyValueSize) {
          throw new IllegalArgumentException("KeyValue size too large");
        }

        ByteString cellQualifierByteString =
            ByteString.copyFrom(
                cell.getQualifierArray(), cell.getQualifierOffset(), qualifierLength);

        ByteString value =
            ByteString.copyFrom(cell.getValueArray(), cell.getValueOffset(), valueLength);

        long timestampMicros = currentTimestampMicros;
        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
          timestampMicros = TimestampConverter.hbase2bigtable(cell.getTimestamp());
        }
        mutation.setCell(
            familyString.toStringUtf8(), cellQualifierByteString, timestampMicros, value);
      }
    }
  }

  boolean isSetClientTimestamp() {
    return setClientTimestamp;
  }
}
