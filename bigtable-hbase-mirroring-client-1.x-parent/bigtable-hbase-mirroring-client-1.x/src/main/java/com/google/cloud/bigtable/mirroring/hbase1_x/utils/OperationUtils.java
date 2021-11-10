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

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

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
}
