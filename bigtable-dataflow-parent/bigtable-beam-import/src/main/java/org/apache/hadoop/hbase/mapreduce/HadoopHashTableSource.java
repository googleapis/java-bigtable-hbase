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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.RangeHash;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A beam source to read output of Hadoop HashTable job. The source creates 1 workitem per HashTable
 * partition and emits a row-range/hash pair.
 */
public class HadoopHashTableSource extends BoundedSource<RangeHash> {

  public static class RangeHash implements Serializable {
    public byte[] startInclusive;
    public byte[] endExclusive;
    public byte[] hash;

    public RangeHash() {}

    static RangeHash of(
        ImmutableBytesWritable startInclusive,
        ImmutableBytesWritable endExclusive,
        ImmutableBytesWritable hash) {
      RangeHash out = new RangeHash();
      out.startInclusive = startInclusive.copyBytes();
      // TODO: use Arrays.copyFrom everywhere.
      out.endExclusive = endExclusive.copyBytes();
      out.hash = hash.copyBytes();
      return out;
    }

    @Override
    public String toString() {
      return String.format(
          "RangeHash{ range = [ %s, %s), hash: %s }",
          // TODO: use HEX representation everywhere.
          new String(startInclusive), new String(endExclusive), Bytes.toHex(hash));
    }
  }

  public static final Log LOG = LogFactory.getLog(HadoopHashTableSource.class);

  // Hadoop configuration to load the output of HBase HashTable job's output.
  public final SerializableConfiguration conf;
  // Path to the output of HashTable job. Usually in GCS.
  protected String hashTableOutputPathDir;

  // Coder to encode/decode the RangeHash
  private final SerializableCoder<RangeHash> coder;

  public HadoopHashTableSource(SerializableConfiguration conf, String hashTableOutputPathDir) {
    this.conf = conf;
    this.hashTableOutputPathDir = hashTableOutputPathDir;
    this.coder = SerializableCoder.of(RangeHash.class);
  }

  @Override
  public List<? extends BoundedSource<RangeHash>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // TODO: We can also make SampleRows call to bigtable like BigtableIO and determine partitions.
    // To make the partitions aligned with the HashTable, we will need to create a new reader from
    // every partition key and adjust the partition key to reader.getCurrentKey(). This aligns the
    // partitions to approx tablet boundaries (First hashTable batch in the tablet)
    // If we do that, we don't really need the Source to be in Apache package. We can add a
    // a validator that can perform some validations and return a loaded TableHash.
    TableHash hash = null;
    try {
      hash = TableHash.read(conf.get(), new Path(hashTableOutputPathDir));
    } catch (IOException e) {
      LOG.fatal("Failed to read HashTable's output via TableHash.read", e);
      throw e;
    }

    Preconditions.checkArgument(
        hash.numHashFiles == (hash.partitions.size() + 1),
        String.format(
            "Corrupt hashtable output. %d hash files for %d partitions. Expected %d files.",
            hash.numHashFiles, hash.partitions.size(), hash.partitions.size() + 1));

    List<KeyBasedHashTableSource> splitSources = new ArrayList<>(hash.partitions.size() + 1);
    if (hash.partitions.size() == 0) {
      // There are 0 partitions and 1 hashfile, return single source with full key range.
      // TODO: Validate the hash's start/stop row with the dataflow start/stop rows.
      splitSources.add(
          new KeyBasedHashTableSource(
              conf,
              hashTableOutputPathDir,
              new ImmutableBytesWritable(hash.startRow),
              new ImmutableBytesWritable(hash.stopRow)));
      return splitSources;
    }

    // For custom start and end keys, we can't add a empty start key blindly. We need to rely on
    // hashtable to add the right partitions.
    ImmutableBytesWritable startRow = new ImmutableBytesWritable(hash.startRow);

    // The output of HashTable is organized as partition file and a set of datafiles.
    // Partition file contains a list of partitions, these partitions split the key-range of a table
    // into roughly equal row-ranges and hashes for these row-ranges are stored in a single
    // datafile.
    //
    // There are always numPartitions +1 data files. Datafile(i) covers hashes for [partition{i-1},
    // partition{i}).
    // So a partition file containing entries [b,f] for a table with row range [a,z] will have 3
    // data files containing hashes.
    // file0 will contain [a(startRow), b), file1 will contain [b,f), and file3 will contain
    // [f,z(stopRow))
    for (int i = 0; i < hash.partitions.size(); i++) {
      LOG.warn(
          "Adding: ["
              + new String(startRow.get())
              + ", "
              + new String(hash.partitions.get(i).get())
              + "]");
      splitSources.add(
          new KeyBasedHashTableSource(
              conf, hashTableOutputPathDir, startRow, hash.partitions.get(i)));
      startRow = hash.partitions.get(i);
    }
    LOG.warn(
        "Adding: ["
            + new String(startRow.get())
            + ", "
            + new String(HConstants.EMPTY_END_ROW)
            + "]");
    // Add the last range for [lastPartition, stopRow).
    splitSources.add(
        new KeyBasedHashTableSource(
            conf,
            hashTableOutputPathDir,
            hash.partitions.get(hash.partitions.size() - 1),
            new ImmutableBytesWritable(HConstants.EMPTY_END_ROW)));
    LOG.error("Returning " + splitSources.size() + " sources. Bundled at every " + hash.batchSize);
    LOG.error("From " + hash.partitions.size() + " partitions");
    return splitSources;
  }

  @Override
  public Coder<RangeHash> getOutputCoder() {
    return coder;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    // TODO: Understand how this impacts parallelism and try to use SampleRows to determine approx
    // sizes like in CloudBigtableIO
    return 0;
  }

  @Override
  public BoundedReader<RangeHash> createReader(PipelineOptions options) throws IOException {
    // TODO: Maybe throw an exception here. Reader should always come from KeyBasedSource.
    return null;
  }

  class KeyBasedHashTableSource extends HadoopHashTableSource {

    private final byte[] startRow;
    private final byte[] stopRow;

    public KeyBasedHashTableSource(
        SerializableConfiguration conf,
        String pathDir,
        ImmutableBytesWritable startRow,
        ImmutableBytesWritable stopRow) {
      super(conf, pathDir);
      this.startRow = startRow.copyBytes();
      this.stopRow = stopRow.copyBytes();
    }

    @Override
    public BoundedReader createReader(PipelineOptions options) throws IOException {
      TableHash hash = null;
      try {
        hash = TableHash.read(conf.get(), new Path(hashTableOutputPathDir));
      } catch (IOException e) {
        LOG.fatal("failed to read tableHash", e);
      }
      return new HashBasedReader(
          this,
          new ImmutableBytesWritable(startRow),
          new ImmutableBytesWritable(stopRow),
          hash.newReader(conf.get(), new ImmutableBytesWritable(startRow)));
    }
  }

  class HashBasedReader extends BoundedReader<RangeHash> {

    KeyBasedHashTableSource source;
    TableHash.Reader reader;
    ImmutableBytesWritable startRow;
    ImmutableBytesWritable stopRow;
    transient int numKeys = 0;
    transient boolean workItemEnded = false;
    transient boolean scanEnded = false;
    transient ImmutableBytesWritable cachedBatchStartKey;
    transient ImmutableBytesWritable cachedBatchHash;

    public HashBasedReader(
        KeyBasedHashTableSource source,
        ImmutableBytesWritable startRow,
        ImmutableBytesWritable stopRow,
        TableHash.Reader reader) {
      this.reader = reader;
      this.source = source;
      this.startRow = startRow;
      this.stopRow = stopRow;
    }

    private String immutableBytesToString(ImmutableBytesWritable bytes) {
      if (bytes == null) {
        return "";
      }
      return new String(bytes.get());
    }

    @Override
    public boolean start() throws IOException {
      // TODO: Beam SDK recommends putting expensive operations here, maybe load the reader here.
      // Loading the reader here will mean that we need to pass the TableHash here.
      // https://screenshot.googleplex.com/4DtzFXmFxcyUkc5
      LOG.error(
          "Starting a new reader at key range ["
              + immutableBytesToString(startRow)
              + " ,"
              + immutableBytesToString(stopRow)
              + ").");
      numKeys = 0;

      if (reader.next()) {
        cachedBatchStartKey = reader.getCurrentKey();
        cachedBatchHash = reader.getCurrentHash();
        // We have atleast 1 range to return, so we will return true here. If there is only 1 range
        // advance will get reader.next() == false and create a range from [cachedBatchStartKey, "")

        // We have consumed the reader's current here, and then next time the getCurrent will be
        // called, it will get the same value. For getCurrent to get a new value, advancing here.
        reader.next();
        return true;
      }

      return false;
    }

    @Override
    public boolean advance() throws IOException {
      numKeys++;
      // Avoid infinite loops
      if (workItemEnded || scanEnded) {
        LOG.warn(
            "Ending workitem at key "
                + immutableBytesToString(cachedBatchStartKey)
                + " scanEnded "
                + scanEnded
                + " workitemEnded "
                + workItemEnded);
        return false;
      }

      if (!reader.next()) {
        // There are no hash batches remaining. We have a cached start, return that with an
        // HConstants.EMPTY_END_RANGE. Return true to emit the cached range.
        LOG.warn(
            "Setting scan ended to true on row " + immutableBytesToString(cachedBatchStartKey));
        try {
          LOG.warn(
              "Setting scan ended to true on row "
                  + immutableBytesToString(reader.getCurrentKey()));
        } catch (Exception e) {
          LOG.error("Error logging the scan ended key ", e);
        }
        scanEnded = true;
        return true;
      }

      if (stopRow.equals(reader.getCurrentKey())) {
        LOG.warn(
            "Setting workitem ended to true on row "
                + immutableBytesToString(reader.getCurrentKey()));
        // This workitem has ended here. But we have a cached start key, we need to return the
        // cached batch with the partition end key.
        workItemEnded = true;
        return true;
      }

      // Reached here means the scan is not ended and the current workitem has not ended. Keep
      // reading.
      return true;
    }

    @Override
    public RangeHash getCurrent() throws NoSuchElementException {
      RangeHash out = null;
      if (scanEnded) {
        // Emit a sentinel key/hash to conclude the scan till the end of key range.
        LOG.error("Scan ended, returning range with empty stopRow");
        out =
            RangeHash.of(
                cachedBatchStartKey,
                new ImmutableBytesWritable(HConstants.EMPTY_END_ROW),
                cachedBatchHash);
        cachedBatchHash = null;
      } else {
        // No need to handle the workitemEnded cases specially here. We will cache the
        // stopRowExclusive and its hash but advance() will return false, so it won't be used in
        // this
        // work item.
        out = RangeHash.of(cachedBatchStartKey, reader.getCurrentKey(), cachedBatchHash);
        // Move to the next step.
        cachedBatchStartKey = reader.getCurrentKey();
        cachedBatchHash = reader.getCurrentHash();
      }

      return out;
    }

    @Override
    public void close() throws IOException {
      LOG.error(
          "Finishing a new reader at key range ["
              + immutableBytesToString(startRow)
              + " ,"
              + immutableBytesToString(stopRow)
              + ") and reading "
              + numKeys
              + " keys.");
      reader.close();
    }

    @Override
    public BoundedSource<RangeHash> getCurrentSource() {
      return source;
    }
  }
}
