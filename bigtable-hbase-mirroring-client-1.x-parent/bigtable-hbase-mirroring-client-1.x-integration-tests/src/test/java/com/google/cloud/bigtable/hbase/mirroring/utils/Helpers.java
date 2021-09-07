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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;

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

  public static Get createGet(byte[] row, byte[] family, byte[] qualifier) {
    Get put = new Get(row);
    put.addColumn(family, qualifier);
    return put;
  }
}
