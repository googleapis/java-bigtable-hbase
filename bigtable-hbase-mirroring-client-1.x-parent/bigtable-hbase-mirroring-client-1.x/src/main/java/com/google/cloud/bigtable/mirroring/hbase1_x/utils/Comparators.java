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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import com.google.api.core.InternalApi;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

@InternalApi("For internal usage only")
public class Comparators {
  public static boolean resultsEqual(Result result1, Result result2) {
    if (result1 == null && result2 == null) {
      return true;
    }
    if (result1 == null || result2 == null) {
      return false;
    }
    int rowsComparisionResult =
        CellComparator.compareRows(result1.getRow(), 0, 0, result2.getRow(), 0, 0);
    if (rowsComparisionResult != 0) {
      return false;
    }
    Cell[] cells1 = result1.rawCells();
    Cell[] cells2 = result2.rawCells();

    if (cells1 == null && cells2 == null) {
      return true;
    }

    if (cells1 == null || cells2 == null) {
      return false;
    }

    if (cells1.length != cells2.length) {
      return false;
    }
    for (int i = 0; i < cells1.length; i++) {
      int cellResult = CellComparator.compare(cells1[i], cells2[i], true);
      if (cellResult != 0) {
        return false;
      }
      if (!CellUtil.matchingValue(cells1[i], cells2[i])) {
        return false;
      }
    }
    return true;
  }
}
