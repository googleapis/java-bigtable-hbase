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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

@InternalApi("For internal use only")
public class CopyingTimestamper implements Timestamper {
  private final MonotonicTimer timer = new MonotonicTimer();

  @Override
  public Put fillTimestamp(Put put) {
    long timestamp = timer.getCurrentTimeMillis();
    return setPutTimestamp(put, timestamp);
  }

  @Override
  public RowMutations fillTimestamp(RowMutations rowMutations) {
    long timestamp = timer.getCurrentTimeMillis();
    return setRowMutationsTimestamp(rowMutations, timestamp);
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
    List<T> result = new ArrayList<>();
    for (T row : list) {
      result.add(setTimestamp(row, timestamp));
    }
    return result;
  }

  private <T extends Row> T setTimestamp(T row, long timestamp) {
    // Those casts are totally safe, but there are not subclasses of Put and RowMutations that we
    // know of.
    if (row instanceof Put) {
      return (T) setPutTimestamp((Put) row, timestamp);
    } else if (row instanceof RowMutations) {
      return (T) setRowMutationsTimestamp((RowMutations) row, timestamp);
    }
    // Bigtable doesn't support timestamps for Increment and Append and only a specific subset of
    // Deletes, let's not modify them.
    return row;
  }

  private Put setPutTimestamp(Put put, long timestamp) {
    Put putCopy = clonePut(put);
    TimestampUtils.setPutTimestamp(putCopy, timestamp);
    return putCopy;
  }

  private Put clonePut(Put toClone) {
    // This copy shares Cells with the original.
    Put putCopy = new Put(toClone);
    cloneFamilyCallMap(putCopy.getFamilyCellMap());
    return putCopy;
  }

  private void cloneFamilyCallMap(NavigableMap<byte[], List<Cell>> familyCellMap) {
    for (List<Cell> cells : familyCellMap.values()) {
      cloneCellList(cells);
    }
  }

  private void cloneCellList(List<Cell> cells) {
    for (int i = 0; i < cells.size(); i++) {
      cells.set(i, cloneCell(cells.get(i)));
    }
  }

  private Cell cloneCell(Cell cell) {
    if (!(cell instanceof KeyValue)) {
      throw new RuntimeException(
          "CopyingTimestamper doesn't support Puts with cells different than the default KeyValue cell.");
    }
    try {
      return ((KeyValue) cell).clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(
          "KeyValue implementation doesn't support clone() method and CopyingTimestamper cannot use it.");
    }
  }

  private RowMutations setRowMutationsTimestamp(RowMutations rowMutations, long timestamp) {
    try {
      RowMutations result = new RowMutations(rowMutations.getRow());
      for (Mutation mutation : rowMutations.getMutations()) {
        if (mutation instanceof Put) {
          result.add(setPutTimestamp((Put) mutation, timestamp));
        } else if (mutation instanceof Delete) {
          result.add((Delete) mutation);
        } else {
          // Only and `Delete`s and `Put`s are supported.
          throw new UnsupportedOperationException();
        }
      }
      return result;
    } catch (IOException e) {
      // IOException is thrown when row of added mutation doesn't match `RowMutation`s row.
      // This shouldn't happen.
      throw new RuntimeException(e);
    }
  }
}
