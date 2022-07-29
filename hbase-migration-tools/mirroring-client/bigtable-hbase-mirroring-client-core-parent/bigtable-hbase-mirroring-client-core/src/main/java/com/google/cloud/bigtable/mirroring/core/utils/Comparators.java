/*
 * Copyright 2022 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.utils.compat.CellComparatorCompat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

@InternalApi("For internal usage only")
public class Comparators {
  private static CellComparatorCompat cellComparator;

  static {
    // Try to construct 2.x CellComparator compatibility wrapper if available.
    final String comparatorCompat1xImplClass =
        "com.google.cloud.bigtable.mirroring.core.utils.compat.CellComparatorCompatImpl";
    final String comparatorCompat2xImplClass =
        "com.google.cloud.bigtable.mirroring.hbase2_x.utils.compat.CellComparatorCompatImpl";
    try {
      cellComparator =
          (CellComparatorCompat) Class.forName(comparatorCompat2xImplClass).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      try {
        cellComparator =
            (CellComparatorCompat) Class.forName(comparatorCompat1xImplClass).newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
        throw new IllegalStateException(ex);
      }
    }
  }

  public static boolean resultsEqual(Result result1, Result result2) {
    if (result1 == null && result2 == null) {
      return true;
    }
    if (result1 == null || result2 == null) {
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
      if (cells1[i] == null && cells2[i] == null) {
        continue;
      }

      if (cells1[i] == null || cells2[i] == null) {
        return false;
      }

      int cellResult = cellComparator.compareCells(cells1[i], cells2[i]);
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
