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

package com.google.cloud.bigtable.hbase.replication.adapters;

import java.util.ArrayList;
import org.apache.hadoop.hbase.Cell;

/**
 * BigtableWALEntry abstracts minimal functionality from WAL.Entry required for this replication
 * library.
 */
public class BigtableWALEntry {
  private long walWriteTimestamp;
  private ArrayList<Cell> cells;
  private String tableName;

  public BigtableWALEntry(long timeStamp, ArrayList<Cell> cells, String tableName) {
    this.walWriteTimestamp = timeStamp;
    this.cells = cells;
    this.tableName = tableName;
  }

  public ArrayList<Cell> getCells() {
    return this.cells;
  }

  public long getWalWriteTime() {
    return this.walWriteTimestamp;
  }

  public String getTableName() {
    return this.tableName;
  }
}
