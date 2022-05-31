/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.utils.timestamper;

import com.google.api.core.InternalApi;
import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

@InternalApi("For internal use only")
public class InPlaceTimestamper implements Timestamper {
  private final MonotonicTimer timer = new MonotonicTimer();

  @Override
  public Put fillTimestamp(Put put) {
    long timestamp = timer.getCurrentTimeMillis();
    TimestampUtils.setPutTimestamp(put, timestamp);
    return put;
  }

  @Override
  public RowMutations fillTimestamp(RowMutations rowMutations) {
    long timestamp = timer.getCurrentTimeMillis();
    setRowMutationsTimestamp(rowMutations, timestamp);
    return rowMutations;
  }

  @Override
  public Mutation fillTimestamp(Mutation mutation) {
    if (mutation instanceof Put) {
      return fillTimestamp((Put) mutation);
    }
    return mutation;
  }

  @Override
  public <T extends Row> List<T> fillTimestamp(List<T> list) {
    long timestamp = timer.getCurrentTimeMillis();
    for (T r : list) {
      setTimestamp(r, timestamp);
    }
    return list;
  }

  private void setTimestamp(Row row, long timestamp) {
    // Those casts are totally safe, but there are not subclasses of Put and RowMutations that we
    // know of.
    if (row instanceof Put) {
      TimestampUtils.setPutTimestamp((Put) row, timestamp);
    } else if (row instanceof RowMutations) {
      setRowMutationsTimestamp((RowMutations) row, timestamp);
    }
    // Bigtable doesn't support timestamps for Increment and Append and only a specific subset of
    // Deletes, let's not modify them.
  }

  private void setRowMutationsTimestamp(RowMutations rowMutations, long timestamp) {
    for (Mutation mutation : rowMutations.getMutations()) {
      setTimestamp(mutation, timestamp);
    }
  }
}
