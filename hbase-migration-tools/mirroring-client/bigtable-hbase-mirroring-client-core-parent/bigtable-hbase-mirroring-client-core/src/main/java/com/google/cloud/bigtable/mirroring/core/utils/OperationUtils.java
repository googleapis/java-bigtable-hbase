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
package com.google.cloud.bigtable.mirroring.core.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;

public class OperationUtils {
  public static Put makePutFromResult(Result result) {
    Put put = new Put(result.getRow());
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry :
        result.getMap().entrySet()) {
      byte[] family = familyEntry.getKey();
      for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
          familyEntry.getValue().entrySet()) {
        byte[] qualifier = qualifierEntry.getKey();
        for (Map.Entry<Long, byte[]> valueEntry : qualifierEntry.getValue().entrySet()) {
          long timestamp = valueEntry.getKey();
          byte[] value = valueEntry.getValue();
          put.addColumn(family, qualifier, timestamp, value);
        }
      }
    }
    return put;
  }

  public static Result emptyResult() {
    return Result.create(new Cell[0]);
  }

  public static class RewrittenIncrementAndAppendIndicesInfo<T extends Row> {
    /** In batch() when an input Row's isReturnResults is false an empty Result is returned. */
    public final List<T> operations;

    private final Set<Integer> unwantedResultIndices;

    public RewrittenIncrementAndAppendIndicesInfo(List<? extends T> inputOperations) {
      this.unwantedResultIndices = new HashSet<>();
      this.operations = new ArrayList<>(inputOperations);
      for (int i = 0; i < operations.size(); i++) {
        Row row = operations.get(i);
        if (row instanceof Increment) {
          ((Increment) row).setReturnResults(true);
          this.unwantedResultIndices.add(i);
        } else if (row instanceof Append) {
          ((Append) row).setReturnResults(true);
          this.unwantedResultIndices.add(i);
        }
      }
    }

    public void discardUnwantedResults(Object[] results) {
      if (!this.unwantedResultIndices.isEmpty()) {
        for (int i = 0; i < results.length; i++) {
          if (results[i] instanceof Result && this.unwantedResultIndices.contains(i)) {
            Row op = this.operations.get(i);
            if (op instanceof Increment || op instanceof Append) {
              results[i] = emptyResult();
            }
          }
        }
      }
    }
  }
}
