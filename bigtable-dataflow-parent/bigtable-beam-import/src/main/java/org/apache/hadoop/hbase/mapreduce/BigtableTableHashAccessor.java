/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HashTable.ResultHasher;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;

/** A helper class to access package private fields of HashTable.TableHash. */
public class BigtableTableHashAccessor {

  // Restrict object creation. This class should only be used to access state from TableHash.
  private BigtableTableHashAccessor() {}

  public static int getNumHashFiles(TableHash hash) {
    return hash.numHashFiles;
  }

  public static ImmutableList<ImmutableBytesWritable> getPartitions(TableHash hash) {
    return ImmutableList.copyOf(hash.partitions);
  }

  public static ImmutableBytesWritable getStartRow(TableHash hash) {
    return new ImmutableBytesWritable(hash.startRow);
  }

  public static ImmutableBytesWritable getStopRow(TableHash hash) {
    return new ImmutableBytesWritable(hash.stopRow);
  }

  public static Scan getScan(TableHash hash) throws IOException {
    return hash.initScan();
  }

  // Wrapper to access ResultHasher from ComputeAndValidateHashFromBigtableDoFn.
  public static class BigtableResultHasher extends ResultHasher {}
}
