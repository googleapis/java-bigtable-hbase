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

import com.google.bigtable.repackaged.com.google.gson.Gson;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * A fake for TableHashWrapper that allows us to mock the behavior of hbase's HashTable.TableHash
 */
public class FakeTableHashWrapper implements TableHashWrapper {

  // Sorted list of partition keys splitting the key range.
  public List<ImmutableBytesWritable> partitions;
  // List of <Key,Hash> sorted by key.
  public List<KV<ImmutableBytesWritable, ImmutableBytesWritable>> hashes;
  public ImmutableBytesWritable startRowInclusive;
  public ImmutableBytesWritable stopRowExclusive;
  public Scan scan;
  private static final long serialVersionUID = 34876543L;

  public FakeTableHashWrapper() {
    this(
        new ImmutableBytesWritable(),
        new ImmutableBytesWritable(),
        new ArrayList<>(),
        new ArrayList<>(),
        new Scan());
  }

  public FakeTableHashWrapper(
      ImmutableBytesWritable startRowInclusive,
      ImmutableBytesWritable stopRowExclusive,
      List<ImmutableBytesWritable> partitions,
      List<KV<ImmutableBytesWritable, ImmutableBytesWritable>> hashes,
      Scan scan) {
    super();
    this.startRowInclusive = startRowInclusive;
    this.stopRowExclusive = stopRowExclusive;
    this.partitions = partitions;
    this.hashes = hashes;
    this.scan = scan;
  }

  @Override
  public int getNumHashFiles() {
    return partitions.size() + 1;
  }

  @Override
  public ImmutableList<ImmutableBytesWritable> getPartitions() {
    return ImmutableList.copyOf(partitions);
  }

  @Override
  public ImmutableBytesWritable getStartRow() {
    return startRowInclusive;
  }

  @Override
  public ImmutableBytesWritable getStopRow() {
    return stopRowExclusive;
  }

  @Override
  public Scan getScan() {
    return scan;
  }

  @Override
  public TableHashReader newReader(Configuration conf, ImmutableBytesWritable startRow) {
    return new FakeTableHashReader(startRow);
  }

  private void writeObject(ObjectOutputStream s) throws IOException {
    Gson gson = new Gson();
    s.writeObject(gson.toJson(scan));
    s.writeObject(gson.toJson(startRowInclusive));
    s.writeObject(gson.toJson(stopRowExclusive));
    s.writeObject(gson.toJson(partitions));
    s.writeObject(gson.toJson(hashes));
  }

  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
    Gson gson = new Gson();
    scan = gson.fromJson((String) s.readObject(), Scan.class);
    startRowInclusive = gson.fromJson((String) s.readObject(), ImmutableBytesWritable.class);
    stopRowExclusive = gson.fromJson((String) s.readObject(), ImmutableBytesWritable.class);
    partitions = gson.fromJson((String) s.readObject(), ArrayList.class);
    hashes = gson.fromJson((String) s.readObject(), ArrayList.class);
  }

  public class FakeTableHashReader implements TableHashReader {
    private final ImmutableBytesWritable startRow;
    // Copy of items to be read by this reader.
    private final List<KV<ImmutableBytesWritable, ImmutableBytesWritable>> entriesToRead;
    // First next() will make index = 0, and compare it with the size of entriesToRead.
    private int index = -1;

    public FakeTableHashReader(ImmutableBytesWritable startRow) {
      this.startRow = startRow;
      entriesToRead = new ArrayList<>();
      for (KV<ImmutableBytesWritable, ImmutableBytesWritable> hash : hashes) {
        // Collect all the entries after startRow.
        if (hash.getKey().compareTo(startRow) >= 0) {
          entriesToRead.add(hash);
        }
      }
    }

    @Override
    public boolean next() throws IOException {
      return ++index < entriesToRead.size();
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() {
      return entriesToRead.get(index).getKey();
    }

    @Override
    public ImmutableBytesWritable getCurrentHash() {
      return entriesToRead.get(index).getValue();
    }

    @Override
    public void close() throws IOException {
      // NOOP
    }
  }
}
