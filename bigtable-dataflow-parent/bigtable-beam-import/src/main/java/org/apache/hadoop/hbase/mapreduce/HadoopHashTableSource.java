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

import autovalue.shaded.com.google$.common.annotations.$VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.AvroCoder;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
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
 * data file and emits a row-range/hash pair.
 */
public class HadoopHashTableSource extends BoundedSource<RangeHash> {

  @DefaultCoder(AvroCoder.class)
  public static class RangeHash {
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
          Bytes.toStringBinary(startInclusive),
          Bytes.toStringBinary(endExclusive),
          Bytes.toHex(hash));
    }
  }

  public static final Log LOG = LogFactory.getLog(HadoopHashTableSource.class);

  // Hadoop configuration to load the output of HBase HashTable job's output.
  protected final SerializableConfiguration conf;
  // Path to the output of HashTable job. Usually in GCS.
  protected String hashTableOutputPathDir;

  // Coder to encode/decode the RangeHash
  private final AvroCoder<RangeHash> coder;

  @$VisibleForTesting
  Supplier<BigtableTableHashAccessor> tableHashHelperSupplier;

  public HadoopHashTableSource(
      // TODO do we need to ValueProvider<SerializableConfiguration>?
      SerializableConfiguration conf, ValueProvider<String> hashTableOutputPathDir) {
    this.conf = conf;
    this.hashTableOutputPathDir = hashTableOutputPathDir.get();
    this.coder = AvroCoder.of(RangeHash.class);
    Supplier<BigtableTableHashAccessor> helperSupplier = () -> {
      return BigtableTableHashAccessor.create(conf, hashTableOutputPathDir);
    };
  }

  @Override
  public List<? extends BoundedSource<RangeHash>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // This method relies on the partitioning done by HBase-HashTable job. There is a possibility
    // of stragglers. SyncTable handles it by using a group by and further splitting workitems.
    BigtableTableHashAccessor hash = null;
    hash = tableHashHelperSupplier.get();

    ImmutableList<ImmutableBytesWritable> partitions = hash.getPartitions();
    int numPartitions = partitions.size();

    List<KeyBasedHashTableSource> splitSources = new ArrayList<>(numPartitions + 1);
    if (numPartitions == 0) {
      // There are 0 partitions and 1 hashfile, return single source with full key range.
      splitSources.add(
          new KeyBasedHashTableSource(
              conf,
              hashTableOutputPathDir,
              new ImmutableBytesWritable(hash.getStartRow()),
              new ImmutableBytesWritable(hash.getStopRow())));
      return splitSources;
    }

    // Use the HashTable start key. The value is HConstants.EMPTY_START_ROW for full table scan.
    ImmutableBytesWritable startRow = new ImmutableBytesWritable(hash.getStartRow());

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
    for (int i = 0; i < numPartitions; i++) {
      LOG.debug(
          "Adding: ["
              + Bytes.toStringBinary(startRow.get())
              + ", "
              + Bytes.toStringBinary(partitions.get(i).get())
              + "]");
      splitSources.add(
          new KeyBasedHashTableSource(
              conf, hashTableOutputPathDir, startRow, partitions.get(i)));
      startRow = partitions.get(i);
    }
    // Add the last range for [lastPartition, stopRow).
    LOG.debug(
        "Adding: ["
            + Bytes.toStringBinary(startRow.get())
            + ", "
            + Bytes.toStringBinary(hash.getStopRow().get())
            + "]");
    // Add the last range for [lastPartition, stopRow).
    splitSources.add(
        new KeyBasedHashTableSource(
            conf,
            hashTableOutputPathDir,
            partitions.get(numPartitions - 1),
            new ImmutableBytesWritable(hash.getStopRow())));
    LOG.info("Returning " + splitSources.size() + " sources from " + numPartitions + " partitions");
    return splitSources;
  }

  @Override
  public Coder<RangeHash> getOutputCoder() {
    return coder;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    // HashTable data files don't expose a method to estimate size or lineCount.
    return 0;
  }

  @Override
  public BoundedReader<RangeHash> createReader(PipelineOptions options) throws IOException {
    // Reader should always come from KeyBasedSource.
    throw new IllegalArgumentException("Reader can't be created from HadoopHashTableSource.");
  }

  // A Beam source to read from a single workitem.
  protected static class KeyBasedHashTableSource extends HadoopHashTableSource {

    private final String startRow;
    private final String stopRow;

    public KeyBasedHashTableSource(
        SerializableConfiguration conf,
        String pathDir,
        ImmutableBytesWritable startRow,
        ImmutableBytesWritable stopRow) {
      super(conf, StaticValueProvider.of(pathDir));
      this.startRow = encodeImmutableBytesWritable(startRow);
      this.stopRow = encodeImmutableBytesWritable(stopRow);
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
          decodeImmutableBytesWritable(startRow),
          decodeImmutableBytesWritable(stopRow),
          hash.newReader(conf.get(), decodeImmutableBytesWritable(startRow)));
    }

    static final String encodeImmutableBytesWritable(ImmutableBytesWritable immutableBytesWritable){
      return Base64.getEncoder().encodeToString(immutableBytesWritable.copyBytes());
    }

    static final ImmutableBytesWritable decodeImmutableBytesWritable(String base64EncodedBytes){
      return new ImmutableBytesWritable(Base64.getDecoder().decode(base64EncodedBytes));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof KeyBasedHashTableSource)) {
        return false;
      }
      KeyBasedHashTableSource that = (KeyBasedHashTableSource) o;
      return Objects.equal(startRow, that.startRow) &&
          Objects.equal(stopRow, that.stopRow);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(startRow, stopRow);
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
      return Bytes.toStringBinary(bytes.get());
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug(
          "Starting a new reader at key range ["
              + immutableBytesToString(startRow)
              + " ,"
              + immutableBytesToString(stopRow)
              + ").");
      numKeys = 0;

      if (reader.next()) {
        cachedBatchStartKey = reader.getCurrentKey();
        cachedBatchHash = reader.getCurrentHash();

        // The reader's current is consumed here, advance the reader to get a new value in
        // getCurrent.
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

      // TODO the second key might have been skipped here, the reader is already advanced on line275
      if (!reader.next()) {
        // There are no hash batches remaining. We have a cached start, return that with an
        // HConstants.EMPTY_END_RANGE. Return true to emit the cached range.
        LOG.debug(
            "Setting scan ended to true on row " + immutableBytesToString(cachedBatchStartKey));
        scanEnded = true;
        // There is a a cached Range that needs to be returned.
        return true;
      }

      if (stopRow.equals(reader.getCurrentKey())) {
        LOG.debug(
            "Setting workitem ended to true on row "
                + immutableBytesToString(reader.getCurrentKey()));
        // This workitem has ended here. But we have a cached start key, we need to return the
        // cached batch with the partition end key.
        workItemEnded = true;
        return true;
      }

      return true;
    }

    @Override
    public RangeHash getCurrent() throws NoSuchElementException {
      RangeHash out = null;
      if (scanEnded) {
        // Emit a sentinel key/hash to conclude the scan till the end of key range.
        LOG.debug("Scan ended, returning range with empty stopRow");
        out =
            RangeHash.of(
                cachedBatchStartKey,
                new ImmutableBytesWritable(HConstants.EMPTY_END_ROW),
                cachedBatchHash);
        cachedBatchHash = null;
      } else {
        // workitemEnded case is handled normally. TableHash.Reader's current item acts as
        // stopRowExclusive for HashBasedReader. So when TableHash.Reader reaches the workitem's end
        // stopRowExclusive is cached but never emitted (becuase advance returns false).
        out = RangeHash.of(cachedBatchStartKey, reader.getCurrentKey(), cachedBatchHash);
        // Move to the next step.
        cachedBatchStartKey = reader.getCurrentKey();
        cachedBatchHash = reader.getCurrentHash();
      }

      return out;
    }

    @Override
    public void close() throws IOException {
      LOG.debug(
          "Finishing a reader for key range ["
              + immutableBytesToString(startRow)
              + " ,"
              + immutableBytesToString(stopRow)
              + ") after reading "
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
