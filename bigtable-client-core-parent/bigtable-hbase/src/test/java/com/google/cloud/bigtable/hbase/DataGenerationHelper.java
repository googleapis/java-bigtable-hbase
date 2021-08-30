/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/** Methods to generate test data. */
public class DataGenerationHelper {

  public byte[] randomData(String prefix) {
    return Bytes.toBytes(prefix + RandomStringUtils.randomAlphanumeric(8));
  }

  public byte[][] randomData(String prefix, int count) {
    return randomData(prefix, count, 8);
  }

  public byte[][] randomData(String prefix, int count, int size) {
    byte[][] result = new byte[count][];
    for (int i = 0; i < count; ++i) {
      result[i] = Bytes.toBytes(prefix + RandomStringUtils.randomAlphanumeric(size));
    }
    return result;
  }

  public long[] sequentialTimestamps(int count) {
    return sequentialTimestamps(count, System.currentTimeMillis());
  }

  public long[] sequentialTimestamps(int count, long firstValue) {
    assert count > 0;
    long[] timestamps = new long[count];
    timestamps[0] = firstValue;
    for (int i = 1; i < timestamps.length; ++i) {
      timestamps[i] = timestamps[0] + i;
    }
    return timestamps;
  }
}
