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
package com.google.cloud.bigtable.mirroring.hbase2_x.utils.compat;

import com.google.cloud.bigtable.mirroring.core.utils.compat.CellComparatorCompat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;

public class CellComparatorCompatImpl implements CellComparatorCompat {
  static CellComparator cellComparator = CellComparator.getInstance();

  @Override
  public int compareCells(Cell cell1, Cell cell2) {
    return cellComparator.compare(cell1, cell2, true);
  }
}
