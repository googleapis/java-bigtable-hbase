/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Helper class to deduplicate Cells in a mutation.
 *
 * This is used primarily to apply client-side Increment and Append deduplication logic.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class CellDeduplicationHelper {

  protected static class QualifierOnlyKey {
    final byte[] qualifier;

    public QualifierOnlyKey(byte[] qualifier) {
      this.qualifier = qualifier;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      QualifierOnlyKey that = (QualifierOnlyKey) o;

      return Arrays.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(qualifier);
    }
  }

  /**
   * Given a Mutation and a family with potentially duplicate cell family:qualifier combinations,
   * remove all entries except for the last for that family:qualifier pair.
   *
   * @param mutation a {@link org.apache.hadoop.hbase.client.Mutation} object.
   * @param family an array of byte.
   * @return a {@link java.util.List} object.
   */
  public static List<Cell> deduplicateFamily(Mutation mutation, byte[] family) {
    List<Cell> result = new LinkedList<>();
    Map<QualifierOnlyKey, Cell> latestCells = new HashMap<>();
    List<Cell> familyCells = mutation.getFamilyCellMap().get(family);
    if (familyCells == null || family.length == 0) {
      return new LinkedList<>();
    }
    for (Cell cell : familyCells) {
      QualifierOnlyKey key = new QualifierOnlyKey(CellUtil.cloneQualifier(cell));
      latestCells.put(key, cell);
    }
    result.addAll(latestCells.values());
    return result;
  }
}
