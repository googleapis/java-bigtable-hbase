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
package com.google.cloud.bigtable.beam.validation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash.Reader;
import org.apache.hadoop.hbase.util.Bytes;

/** Wrapper class for HashTable.TableHash to increase testability of classes using it. */
public class TableHashWrapper {

  private TableHash hash;

  private TableHashWrapper(TableHash hash) {
    this.hash = hash;
  }

  public static TableHashWrapper create(SerializableConfiguration conf, String hashTableOutputDir) {
    TableHashWrapper tableHashWrapper;
    try {
      tableHashWrapper =
          new TableHashWrapper(TableHash.read(conf.get(), new Path(hashTableOutputDir)));
    } catch (IOException e) {
      throw new RuntimeException("Failed to read HashTable's output via TableHash.read", e);
    }
    // TODO add more validations from SyncTable.
    Preconditions.checkArgument(
        tableHashWrapper.getNumHashFiles() == (tableHashWrapper.getPartitions().size() + 1),
        String.format(
            "Corrupt hashtable output. %d hash files for %d partitions. Expected %d files.",
            tableHashWrapper.getNumHashFiles(),
            tableHashWrapper.getPartitions().size(),
            tableHashWrapper.getPartitions().size() + 1));
    return tableHashWrapper;
  }

  public int getNumHashFiles() {
    return BigtableTableHashAccessor.getNumHashFiles(hash);
  }

  public ImmutableList<ImmutableBytesWritable> getPartitions() {
    return BigtableTableHashAccessor.getPartitions(hash);
  }

  public ImmutableBytesWritable getStartRow() {
    return BigtableTableHashAccessor.getStartRow(hash);
  }

  public ImmutableBytesWritable getStopRow() {
    return BigtableTableHashAccessor.getStopRow(hash);
  }

  public Reader newReader(Configuration conf, ImmutableBytesWritable startRow) {
    try {
      return hash.newReader(conf, startRow);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to open reader at " + Bytes.toStringBinary(startRow.copyBytes()), e);
    }
  }
}
