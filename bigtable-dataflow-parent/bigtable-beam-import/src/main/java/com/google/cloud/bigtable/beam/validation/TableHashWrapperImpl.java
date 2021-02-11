/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import static com.google.cloud.bigtable.beam.validation.SyncTableUtils.immutableBytesToString;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash.Reader;

class TableHashWrapperImpl implements TableHashWrapper {

  static TableHashWrapper create(Configuration conf, String hashTableOutputDir) throws IOException {
    TableHash tableHash = TableHash.read(conf, new Path(hashTableOutputDir));

    TableHashWrapper tableHashWrapper = new TableHashWrapperImpl(tableHash);
    Preconditions.checkArgument(
        tableHashWrapper.getNumHashFiles() == (tableHashWrapper.getPartitions().size() + 1),
        "Corrupt hashtable output. %d hash files for %d partitions. Expected %d files.",
        tableHashWrapper.getNumHashFiles(),
        tableHashWrapper.getPartitions().size(),
        tableHashWrapper.getPartitions().size() + 1);
    return tableHashWrapper;
  }

  private final TableHash hash;

  private TableHashWrapperImpl(TableHash hash) {
    this.hash = hash;
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

  public Scan getScan() {
    try {
      return BigtableTableHashAccessor.getScan(hash);
    } catch (IOException e) {
      throw new RuntimeException("Failed to init a scan from TableHash: ", e);
    }
  }

  public TableHashReader newReader(Configuration conf, ImmutableBytesWritable startRow) {
    try {
      return TableHashReaderImpl.create(hash.newReader(conf, startRow));
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to open reader at " + immutableBytesToString(startRow.copyBytes()), e);
    }
  }

  static class TableHashReaderImpl implements TableHashReader {

    private final Reader reader;

    static TableHashReaderImpl create(TableHash.Reader reader) {
      Preconditions.checkNotNull(reader, "Reader can not be null.");
      return new TableHashReaderImpl(reader);
    }

    private TableHashReaderImpl(TableHash.Reader reader) {
      this.reader = reader;
    }

    @Override
    public boolean next() throws IOException {
      return reader.next();
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() {
      return reader.getCurrentKey();
    }

    @Override
    public ImmutableBytesWritable getCurrentHash() {
      return reader.getCurrentHash();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
