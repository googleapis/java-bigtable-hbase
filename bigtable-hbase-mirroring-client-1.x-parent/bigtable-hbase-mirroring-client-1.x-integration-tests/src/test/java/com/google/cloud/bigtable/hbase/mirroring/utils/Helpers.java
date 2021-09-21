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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.common.primitives.Longs;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;

public class Helpers {

  public static Put createPut(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
    Put put = new Put(row);
    put.addColumn(family, qualifier, value);
    return put;
  }

  public static Put createPut(
      byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value) {
    Put put = new Put(row);
    put.addColumn(family, qualifier, timestamp, value);
    return put;
  }

  public static Put createPut(int id, byte[] family, byte[] qualifier) {
    byte[] rowAndValue = Longs.toByteArray(id);
    return createPut(rowAndValue, family, qualifier, id, rowAndValue);
  }

  public static Get createGet(byte[] row, byte[] family, byte[] qualifier) {
    Get put = new Get(row);
    put.addColumn(family, qualifier);
    return put;
  }

  public static Delete createDelete(byte[] rowKey, byte[] family, byte[] qualifier) {
    Delete delete = new Delete(rowKey);
    delete.addColumns(family, qualifier);
    return delete;
  }

  public static Scan createScan(byte[] family, byte[] qualifier) {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return scan;
  }

  public static RowMutations createRowMutations(byte[] row, Mutation... mutations)
      throws IOException {
    RowMutations rowMutations = new RowMutations(row);
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        rowMutations.add((Put) mutation);
      } else if (mutation instanceof Delete) {
        rowMutations.add((Delete) mutation);
      } else {
        throw new UnsupportedOperationException();
      }
    }
    return rowMutations;
  }

  public static Increment createIncrement(
      byte[] rowKey, byte[] columnFamily, byte[] qualifier, int ts) throws IOException {
    Increment increment = new Increment(rowKey);
    increment.addColumn(columnFamily, qualifier, 1);
    increment.setTimeRange(ts, ts + 1);
    return increment;
  }

  public static Append createAppend(
      byte[] rowKey, byte[] columnFamily, byte[] qualifier, byte[] value) {
    Append append = new Append(rowKey);
    append.add(columnFamily, qualifier, value);
    return append;
  }
}
