/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.beam;

import static com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration.ScanType;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.bigtable.repackaged.com.google.api.core.InternalExtensionOnly;
import com.google.bigtable.repackaged.com.google.api.gax.rpc.WatchdogTimeoutException;
import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.bigtable.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableMap;
import com.google.cloud.bigtable.batch.common.CloudBigtableServiceImpl;
import com.google.cloud.bigtable.hbase.BigtableFixedProtoScan;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to create {@link PTransform}s for reading and writing <a
 * href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a> entities in a Beam pipeline.
 *
 * <p>Google Cloud Bigtable offers you a fast, fully managed, massively scalable NoSQL database
 * service that's ideal for web, mobile, and Internet of Things applications requiring terabytes to
 * petabytes of data. Unlike comparable market offerings, Cloud Bigtable doesn't require you to
 * sacrifice speed, scale, or cost efficiency when your applications grow. Cloud Bigtable has been
 * battle-tested at Google for more than 10 years--it's the database driving major applications such
 * as Google Analytics and Gmail.
 *
 * <p>To use {@link CloudBigtableIO}, users must use gcloud to get a credential for Cloud Bigtable:
 *
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>To read a {@link PCollection} from a table, with an optional {@link Scan}, use {@link
 * CloudBigtableIO#read(CloudBigtableScanConfiguration)}:
 *
 * <pre>{@code
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Result> = p.apply(
 *   Read.from(CloudBigtableIO.read(
 *      new CloudBigtableScanConfiguration.Builder()
 *          .withProjectId("project-id")
 *          .withInstanceId("instance-id")
 *          .withTableId("table-id")
 *          .build())));
 * }</pre>
 *
 * <p>To write a {@link PCollection} to a table, use {@link
 * CloudBigtableIO#writeToTable(CloudBigtableTableConfiguration)}:
 *
 * <pre>{@code
 * PipelineOptions options =
 *     PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Mutation> mutationCollection = ...;
 * mutationCollection.apply(
 *   CloudBigtableIO.writeToTable(
 *      new CloudBigtableScanConfiguration.Builder()
 *          .withProjectId("project-id")
 *          .withInstanceId("instance-id")
 *          .withTableId("table-id")
 *          .build()));
 * }</pre>
 */
public class CloudBigtableIO {

  /**
   * A {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially filtered by a
   * {@link Scan}.
   */
  @InternalExtensionOnly
  @SuppressWarnings("serial")
  abstract static class AbstractSource extends BoundedSource<Result> {
    protected static final Logger SOURCE_LOG = LoggerFactory.getLogger(AbstractSource.class);
    protected static final long SIZED_BASED_MAX_SPLIT_COUNT = 4_000;
    static final long COUNT_MAX_SPLIT_COUNT = 15_360;

    /** Configuration for a Cloud Bigtable connection, a table, and an optional scan. */
    private final CloudBigtableScanConfiguration configuration;

    private transient List<KeyOffset> sampleRowKeys;

    AbstractSource(CloudBigtableScanConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public Coder<Result> getOutputCoder() {
      return getResultCoder();
    }

    // TODO: Move the splitting logic to bigtable-hbase, and separate concerns between beam needs
    // and Cloud Bigtable logic.
    protected List<SourceWithKeys> getSplits(long desiredBundleSizeBytes) throws Exception {
      desiredBundleSizeBytes =
          Math.max(
              calculateEstimatedSizeBytes(null) / SIZED_BASED_MAX_SPLIT_COUNT,
              desiredBundleSizeBytes);
      CloudBigtableScanConfiguration conf = getConfiguration();
      byte[] scanStartKey = conf.getStartRow();
      byte[] scanEndKey = conf.getStopRow();
      List<SourceWithKeys> splits = new ArrayList<>();
      byte[] startKey = HConstants.EMPTY_START_ROW;
      long lastOffset = 0;
      for (KeyOffset response : getSampleRowKeys()) {
        byte[] endKey = response.getKey().toByteArray();
        // Avoid empty regions.
        if (Bytes.equals(startKey, endKey) && startKey.length > 0) {
          continue;
        }

        long offset = response.getOffsetBytes();
        // Get all the start/end key ranges that match the user supplied Scan.  See
        // https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/TableInputFormatBase.java#L298
        // for original logic.
        if (isWithinRange(scanStartKey, scanEndKey, startKey, endKey)) {
          byte[] splitStart = null;
          byte[] splitStop = null;
          if (scanStartKey.length == 0 || Bytes.compareTo(startKey, scanStartKey) >= 0) {
            splitStart = startKey;
          } else {
            splitStart = scanStartKey;
          }

          if ((scanEndKey.length == 0 || Bytes.compareTo(endKey, scanEndKey) <= 0)
              && endKey.length > 0) {
            splitStop = endKey;
          } else {
            splitStop = scanEndKey;
          }
          splits.addAll(split(offset - lastOffset, desiredBundleSizeBytes, splitStart, splitStop));
        }
        lastOffset = offset;
        startKey = endKey;
      }
      // Create one last region if the last region doesn't reach the end or there are no regions.
      byte[] endKey = HConstants.EMPTY_END_ROW;
      if (!Bytes.equals(startKey, endKey) && scanEndKey.length == 0) {
        splits.add(createSourceWithKeys(startKey, endKey, 0));
      }
      List<SourceWithKeys> result = reduceSplits(splits);

      // Randomize the list, since the default behavior would lead to multiple workers hitting the
      // same tablet.
      Collections.shuffle(result);
      return result;
    }

    private List<SourceWithKeys> reduceSplits(List<SourceWithKeys> splits) {
      if (splits.size() < COUNT_MAX_SPLIT_COUNT) {
        return splits;
      }
      List<SourceWithKeys> reducedSplits = new ArrayList<>();
      SourceWithKeys start = null;
      SourceWithKeys lastSeen = null;
      int numberToCombine =
          (int) ((splits.size() + COUNT_MAX_SPLIT_COUNT - 1) / COUNT_MAX_SPLIT_COUNT);
      int counter = 0;
      long size = 0;
      for (SourceWithKeys source : splits) {
        if (counter == 0) {
          start = source;
        }
        size += source.getEstimatedSize();
        counter++;
        lastSeen = source;
        if (counter == numberToCombine) {
          reducedSplits.add(
              createSourceWithKeys(
                  start.getConfiguration().getStartRow(),
                  source.getConfiguration().getStopRow(),
                  size));
          counter = 0;
          size = 0;
          start = null;
        }
      }
      if (start != null) {
        reducedSplits.add(
            createSourceWithKeys(
                start.getConfiguration().getStartRow(),
                lastSeen.getConfiguration().getStopRow(),
                size));
      }
      return reducedSplits;
    }

    /** Checks if the range of the region is within the range of the scan. */
    protected static boolean isWithinRange(
        byte[] scanStartKey, byte[] scanEndKey, byte[] startKey, byte[] endKey) {
      return (scanStartKey.length == 0
              || endKey.length == 0
              || Bytes.compareTo(scanStartKey, endKey) < 0)
          && (scanEndKey.length == 0 || Bytes.compareTo(scanEndKey, startKey) > 0);
    }

    /**
     * Performs a call to get sample row keys from {@link
     * CloudBigtableServiceImpl#getSampleRowKeys(CloudBigtableTableConfiguration)} if they are not
     * yet cached. The sample row keys give information about tablet key boundaries and estimated
     * sizes.
     *
     * <p>For internal use only - public for technical reasons.
     */
    @InternalApi("For internal usage only")
    public synchronized List<KeyOffset> getSampleRowKeys() throws IOException {
      if (sampleRowKeys == null) {
        sampleRowKeys = new CloudBigtableServiceImpl().getSampleRowKeys(getConfiguration());
      }
      return sampleRowKeys;
    }

    @VisibleForTesting
    void setSampleRowKeys(List<KeyOffset> sampleRowKeys) {
      this.sampleRowKeys = sampleRowKeys;
    }

    /** Validates the existence of the table in the configuration. */
    @Override
    public void validate() {
      getConfiguration().validate();
    }

    /**
     * Gets an estimated size based on data returned from {@link #getSampleRowKeys}. The estimate
     * will be high if a {@link org.apache.hadoop.hbase.client.Scan} is set on the {@link
     * CloudBigtableScanConfiguration}; in such cases, the estimate will not take the Scan into
     * account, and will return a larger estimate than what the {@link CloudBigtableIO.Reader} will
     * actually read.
     *
     * @param options The pipeline options.
     * @return The estimated size of the data, in bytes.
     * @throws IOException
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      try {
        return calculateEstimatedSizeBytes(options);
        // When using templates, we can't estimate the source size because we can't pull out the
        // source table. So we return unknown (0L).
      } catch (IllegalStateException e) {
        return 0;
      }
    }

    protected long calculateEstimatedSizeBytes(PipelineOptions options) throws IOException {
      long totalEstimatedSizeBytes = 0;

      byte[] scanStartKey = getConfiguration().getStartRow();
      byte[] scanStopKey = getConfiguration().getStopRow();

      byte[] startKey = HConstants.EMPTY_START_ROW;
      long lastOffset = 0;
      for (KeyOffset response : getSampleRowKeys()) {
        byte[] currentEndKey = response.getKey().toByteArray();
        // Avoid empty regions.
        if (Bytes.equals(startKey, currentEndKey) && startKey.length != 0) {
          continue;
        }
        long offset = response.getOffsetBytes();
        if (isWithinRange(scanStartKey, scanStopKey, startKey, currentEndKey)) {
          totalEstimatedSizeBytes += (offset - lastOffset);
        }
        lastOffset = offset;
        startKey = currentEndKey;
      }
      SOURCE_LOG.info("Estimated size in bytes: " + totalEstimatedSizeBytes);

      return totalEstimatedSizeBytes;
    }

    /**
     * Splits the region based on the start and stop key. Uses {@link Bytes#split(byte[], byte[],
     * int)} under the covers.
     *
     * @throws IOException
     */
    protected List<SourceWithKeys> split(
        long regionSize, long desiredBundleSizeBytes, byte[] startKey, byte[] stopKey)
        throws IOException {
      Preconditions.checkState(desiredBundleSizeBytes >= 0);
      int splitCount = (int) Math.ceil((double) (regionSize) / (double) (desiredBundleSizeBytes));

      if (splitCount < 2 || stopKey.length == 0 || Bytes.compareTo(startKey, stopKey) >= 0) {
        return Collections.singletonList(createSourceWithKeys(startKey, stopKey, regionSize));
      } else {
        if (stopKey.length > 0) {
          Preconditions.checkState(
              Bytes.compareTo(startKey, stopKey) <= 0,
              "Source keys not in order: [%s, %s]",
              Bytes.toStringBinary(startKey),
              Bytes.toStringBinary(stopKey));
          Preconditions.checkState(regionSize > 0, "Source size must be positive", regionSize);
        }
        try {
          byte[][] splitKeys = Bytes.split(startKey, stopKey, splitCount - 1);
          Preconditions.checkState(splitCount + 1 == splitKeys.length);
          List<SourceWithKeys> result = new ArrayList<>();
          for (int i = 0; i < splitCount; i++) {
            result.add(createSourceWithKeys(splitKeys[i], splitKeys[i + 1], regionSize));
          }
          return result;
        } catch (Exception e) {
          SOURCE_LOG.warn(
              String.format(
                  "Could not split '%s' and '%s', so using that as a range.",
                  Bytes.toString(startKey), Bytes.toString(stopKey)),
              e);
          return Collections.singletonList(createSourceWithKeys(startKey, stopKey, regionSize));
        }
      }
    }

    @VisibleForTesting
    SourceWithKeys createSourceWithKeys(byte[] startKey, byte[] stopKey, long size) {
      CloudBigtableScanConfiguration updatedConfig =
          getConfiguration().toBuilder().withKeys(startKey, stopKey).build();
      return new SourceWithKeys(updatedConfig, size);
    }

    /**
     * Creates a reader that will scan the entire table based on the {@link Scan} in the
     * configuration.
     *
     * @return A reader for the table.
     */
    @Override
    public BoundedSource.BoundedReader<Result> createReader(PipelineOptions options) {
      return new CloudBigtableIO.Reader(this);
    }

    /** @return the configuration */
    protected CloudBigtableScanConfiguration getConfiguration() {
      return configuration;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      configuration.populateDisplayData(builder);
    }

    abstract static class AbstractSerializationProxy implements Serializable {

      private ValueProvider<String> projectId;
      private ValueProvider<String> instanceId;
      private ValueProvider<String> tableId;
      private Map<String, ValueProvider<String>> additionalConfiguration;
      private transient ValueProvider<Scan> scan;

      public AbstractSerializationProxy(CloudBigtableScanConfiguration configuration) {
        this.projectId = configuration.getProjectIdValueProvider();
        this.instanceId = configuration.getInstanceIdValueProvider();
        this.tableId = configuration.getTableIdValueProvider();
        this.scan = configuration.getScanValueProvider();
        Map<String, ValueProvider<String>> map = new HashMap<>();
        map.putAll(configuration.getConfiguration());
        map.remove(BigtableOptionsFactory.PROJECT_ID_KEY);
        map.remove(BigtableOptionsFactory.INSTANCE_ID_KEY);
        this.additionalConfiguration =
            new ImmutableMap.Builder<String, ValueProvider<String>>().putAll(map).build();
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        if (scan.isAccessible()) {
          Scan scanValue = scan.get();
          if (scanValue instanceof BigtableFixedProtoScan) {
            out.writeObject(ScanType.FIXED);
            out.writeObject(((BigtableFixedProtoScan) scanValue).getRequest());
          } else {
            out.writeObject(ScanType.HBASE);
            ProtobufUtil.toScan(scanValue).writeDelimitedTo(out);
          }
        } else {
          out.writeObject(ScanType.DEFER);
          out.writeObject(scan);
        }
      }

      private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        ScanType scanType = (ScanType) in.readObject();
        if (scanType == ScanType.FIXED) {
          ReadRowsRequest request = (ReadRowsRequest) in.readObject();
          scan = ValueProvider.StaticValueProvider.of(new BigtableFixedProtoScan(request));
        } else if (scanType == ScanType.DEFER) {
          scan = (ValueProvider<Scan>) in.readObject();
        } else {
          scan =
              ValueProvider.StaticValueProvider.of(
                  ProtobufUtil.toScan(ClientProtos.Scan.parseDelimitedFrom(in)));
        }
      }

      abstract Object readResolve();

      protected CloudBigtableScanConfiguration buildScanConfig() {
        CloudBigtableScanConfiguration conf =
            CloudBigtableScanConfiguration.createConfig(
                projectId, instanceId, tableId, scan, additionalConfiguration);
        return conf;
      }
    }
  }

  /**
   * A {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially filtered by a
   * {@link Scan}.
   */
  @InternalExtensionOnly
  public static class Source extends AbstractSource {
    private static final long serialVersionUID = -5580115943635114126L;

    Source(CloudBigtableScanConfiguration configuration) {
      super(configuration);
    }

    // TODO: Add a method on the server side that will be a more precise split based on server-side
    // statistics
    /**
     * Splits the table based on keys that belong to tablets, known as "regions" in the HBase API.
     * The current implementation uses the HBase {@link RegionLocator} interface, which calls {@link
     * CloudBigtableServiceImpl#getSampleRowKeys(CloudBigtableTableConfiguration)} under the covers.
     * A {@link SourceWithKeys} may correspond to a single region or a portion of a region.
     *
     * <p>If a split is smaller than a single region, the split is calculated based on the
     * assumption that the data is distributed evenly between the region's startKey and stopKey.
     * That assumption may not be correct for any specific start/stop key combination.
     *
     * <p>This method is called internally by Beam. Do not call it directly.
     *
     * @param desiredBundleSizeBytes The desired size for each bundle, in bytes.
     * @param options The pipeline options.
     * @return A list of sources split into groups.
     */
    @Override
    public List<? extends BoundedSource<Result>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // Update the desiredBundleSizeBytes in order to limit the number of splits to
      // MAX_SPLIT_COUNT.  This is an extremely rough estimate for large scale jobs.  There is
      // currently a hard limit on both the count of Sources as well as the sum of the sizes of
      // serialized Sources.  This solution will not work for large workloads for cases where either
      // the row key sizes are large, or the scan is large.
      //
      // TODO: Work on a more robust algorithm for splitting that works for more cases.
      List<? extends BoundedSource<Result>> splits = getSplits(desiredBundleSizeBytes);
      SOURCE_LOG.info("Creating {} splits.", splits.size());
      SOURCE_LOG.debug("Created splits {}.", splits);
      return splits;
    }

    @Override
    public Coder<Result> getOutputCoder() {
      return getResultCoder();
    }

    /**
     * The writeReplace method allows the developer to provide a replacement object that will be
     * serialized instead of the original one. We use this to keep the enclosed class immutable. For
     * more details on the technique see <a
     * href="https://lingpipe-blog.com/2009/08/10/serializing-immutable-singletons-serialization-proxy/">this
     * article</a>.
     */
    private Object writeReplace() {
      return new SourceSerializationProxy(getConfiguration());
    }

    static class SourceSerializationProxy extends AbstractSerializationProxy {
      public SourceSerializationProxy(CloudBigtableScanConfiguration configuration) {
        super(configuration);
      }

      @Override
      Object readResolve() {
        return new Source(buildScanConfig());
      }
    }
  }

  /**
   * A {@link BoundedSource} for a Cloud Bigtable {@link Table} with a start/stop key range, along
   * with a potential filter via a {@link Scan}.
   */
  protected static class SourceWithKeys extends AbstractSource {
    private static final long serialVersionUID = 1L;
    /**
     * An estimate of the size of the source, in bytes.
     *
     * <p>NOTE: This value is a guesstimate. It could be significantly off, especially if there is a
     * {@link Scan} selected in the configuration. It will also be off if the start and stop keys
     * are calculated via {@link CloudBigtableIO.Source#split(long, PipelineOptions)}.
     */
    private final long estimatedSize;

    protected SourceWithKeys(CloudBigtableScanConfiguration configuration, long estimatedSize) {
      super(configuration);
      byte[] stopRow = configuration.getStopRow();
      if (stopRow.length > 0) {
        byte[] startRow = configuration.getStartRow();
        if (Bytes.compareTo(startRow, stopRow) >= 0) {
          throw new IllegalArgumentException(
              String.format(
                  "Source keys not in order: [%s, %s]",
                  Bytes.toStringBinary(startRow), Bytes.toStringBinary(stopRow)));
        }
        Preconditions.checkState(
            estimatedSize >= 0, "Source size cannot be negative", estimatedSize);
      }
      this.estimatedSize = estimatedSize;
      SOURCE_LOG.debug("Source with split: {}.", this);
    }

    @Override
    protected long calculateEstimatedSizeBytes(PipelineOptions options) throws IOException {
      return estimatedSize;
    }

    public long getEstimatedSize() {
      return estimatedSize;
    }

    // TODO: Add a method on the server side that will be a more precise split based on server-
    // side statistics
    /**
     * Splits the bundle based on the assumption that the data is distributed evenly between
     * startKey and stopKey. That assumption may not be correct for any specific start/stop key
     * combination.
     *
     * <p>This method is called internally by Beam. Do not call it directly.
     *
     * @param desiredBundleSizeBytes The desired size for each bundle, in bytes.
     * @param options The pipeline options.
     * @return A list of sources split into groups.
     */
    @Override
    public List<? extends BoundedSource<Result>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      final CloudBigtableScanConfiguration conf = getConfiguration();
      List<? extends BoundedSource<Result>> newSplits =
          split(estimatedSize, desiredBundleSizeBytes, conf.getStartRow(), conf.getStopRow());
      SOURCE_LOG.trace("Splitting split {} into {}", this, newSplits);
      return newSplits;
    }

    @Override
    public Coder<Result> getOutputCoder() {
      return getResultCoder();
    }

    @Override
    public String toString() {
      return String.format(
          "Split start: '%s', end: '%s', size: %d.",
          Bytes.toStringBinary(getConfiguration().getStartRow()),
          Bytes.toStringBinary(getConfiguration().getStopRow()),
          estimatedSize);
    }

    private Object writeReplace() {
      return new SourceWithKeysSerializationProxy(getConfiguration(), estimatedSize);
    }

    static class SourceWithKeysSerializationProxy extends AbstractSerializationProxy {

      private long estimatedSize;

      public SourceWithKeysSerializationProxy(
          CloudBigtableScanConfiguration configuration, long estimatedSize) {
        super(configuration);
        this.estimatedSize = estimatedSize;
      }

      Object readResolve() {
        CloudBigtableScanConfiguration conf = buildScanConfig();
        return new SourceWithKeys(conf, estimatedSize);
      }
    }
  }
  /** Reads rows for a specific {@link Table}, usually filtered by a {@link Scan}. */
  @VisibleForTesting
  static class Reader extends BoundedReader<Result> {
    static final String RETRY_IDLE_TIMEOUT = "google.cloud.bigtable.retry.idle.timeout";

    private static final Logger READER_LOG = LoggerFactory.getLogger(Reader.class);

    private CloudBigtableIO.AbstractSource source;

    private transient Connection connection;
    private transient ResultScanner scanner;
    private transient Result current;
    protected long workStart;
    private final AtomicLong rowsRead = new AtomicLong();
    private final ByteKeyRangeTracker rangeTracker;
    private transient Result lastScannedRow;

    private final AtomicInteger attempt = new AtomicInteger(3);

    @VisibleForTesting
    Reader(CloudBigtableIO.AbstractSource source) {
      this.source = source;
      this.rangeTracker = ByteKeyRangeTracker.of(source.getConfiguration().toByteKeyRange());
    }

    /**
     * Creates a {@link Connection}, {@link Table} and {@link ResultScanner} and advances to the
     * next {@link Result}.
     */
    @Override
    public boolean start() throws IOException {
      initializeScanner();
      workStart = System.currentTimeMillis();
      return advance();
    }

    @VisibleForTesting
    void initializeScanner() throws IOException {
      Configuration config = source.getConfiguration().toHBaseConfig();

      connection = ConnectionFactory.createConnection(config);
      Scan scan = source.getConfiguration().getScanValueProvider().get();
      scanner =
          connection
              .getTable(TableName.valueOf(source.getConfiguration().getTableId()))
              .getScanner(scan);
    }

    /** Calls {@link ResultScanner#next()}. */
    @Override
    public boolean advance() throws IOException {
      try {
        boolean hasMore = tryAdvance();
        // reset attempt after a success read
        attempt.set(3);
        return hasMore;
      } catch (Throwable e) {
        // if retry idle timeout is disabled, throw the exception
        if (!source.getConfiguration().toHBaseConfig().getBoolean(RETRY_IDLE_TIMEOUT, true)) {
          throw e;
        }
        // Exception is not idle timeout, throw it
        Throwable exception = findCause(e, WatchdogTimeoutException.class);
        if (exception == null) {
          throw e;
        }
        if (exception.getMessage() == null || !exception.getMessage().contains("idle")) {
          throw e;
        }
        // Run out ot retry attempt, throw the exception
        if (attempt.decrementAndGet() <= 0) {
          throw e;
        }
        READER_LOG.warn("got idle timeout exception, will try to reset the scanner and retry", e);
        resetScanner();
        return tryAdvance();
      }
    }

    private boolean tryAdvance() throws IOException {
      Result row = scanner.next();
      lastScannedRow = row;
      if (row != null && rangeTracker.tryReturnRecordAt(true, ByteKey.copyFrom(row.getRow()))) {
        current = row;
        rowsRead.addAndGet(1l);
        return true;
      } else {
        current = null;
        rangeTracker.markDone();
        return false;
      }
    }

    private void resetScanner() throws IOException {
      CloudBigtableScanConfiguration scanConfiguration = source.getConfiguration();
      Scan scan;
      if (lastScannedRow != null) {
        byte[] rowKey = lastScannedRow.getRow();
        // ScanConfiguration always gets start key and end key from the RowRange, and it expects
        // start key to be inclusive and end key to be exclusive.
        byte[] newStartKey = Arrays.copyOf(rowKey, rowKey.length + 1);
        scan =
            scanConfiguration
                .toBuilder()
                .withKeys(newStartKey, scanConfiguration.getStopRow())
                .build()
                .getScanValueProvider()
                .get();
      } else {
        scan = scanConfiguration.getScanValueProvider().get();
        READER_LOG.info("last scanned row key is null, haven't read any row yet");
      }

      scanner =
          connection
              .getTable(TableName.valueOf(source.getConfiguration().getTableId()))
              .getScanner(scan);
    }

    static Throwable findCause(Throwable e, Class<? extends Throwable> cls) {
      Throwable throwable = e;
      while (throwable != null && !cls.isAssignableFrom(e.getClass())) {
        throwable = throwable.getCause();
      }
      return throwable;
    }

    @Override
    public final Double getFractionConsumed() {
      if (rangeTracker.isDone()) {
        return 1.0;
      }
      return rangeTracker.getFractionConsumed();
    }

    /**
     * Attempt to split the work by some percent of the ByteKeyRange based on a lexicographical
     * split (and not statistics about the underlying table, which would be better, but that
     * information does not exist).
     */
    @Override
    public final synchronized BoundedSource<Result> splitAtFraction(double fraction) {
      if (fraction < .01 || fraction > .99) {
        return null;
      }
      ByteKey splitKey;
      try {
        splitKey = rangeTracker.getRange().interpolateKey(fraction);
      } catch (IllegalArgumentException e) {
        READER_LOG.info(
            "{}: Failed to interpolate key for fraction {}.", rangeTracker.getRange(), fraction);
        return null;
      }

      READER_LOG.info(
          "Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);

      long estimatedSizeBytes = -1;
      try {
        estimatedSizeBytes = source.calculateEstimatedSizeBytes(null);
      } catch (IOException e) {
        READER_LOG.info(
            "{}: Failed to get estimated size for key for fraction {}.",
            rangeTracker.getRange(),
            fraction);
        return null;
      }
      SourceWithKeys residual = null;
      SourceWithKeys primary = null;
      try {
        long newPrimarySize = (long) (fraction * estimatedSizeBytes);
        long residualSize = estimatedSizeBytes - newPrimarySize;

        byte[] currentStartKey = rangeTracker.getRange().getStartKey().getBytes();
        byte[] splitKeyBytes = splitKey.getBytes();
        byte[] currentStopKey = rangeTracker.getRange().getEndKey().getBytes();

        if (!rangeTracker.trySplitAtPosition(splitKey)) {
          return null;
        }

        primary = source.createSourceWithKeys(currentStartKey, splitKeyBytes, newPrimarySize);
        residual = source.createSourceWithKeys(splitKeyBytes, currentStopKey, residualSize);

        this.source = primary;
        return residual;
      } catch (Throwable t) {
        try {
          String msg =
              String.format(
                  "%d Failed to get estimated size for key for fraction %f.",
                  rangeTracker.getRange(), fraction);
          READER_LOG.warn(msg, t);
        } catch (Throwable t1) {
          // ignore.
        }
        return null;
      }
    }

    @VisibleForTesting
    protected void setConnection(Connection connection) {
      this.connection = connection;
    }

    @VisibleForTesting
    protected void setScanner(ResultScanner scanner) {
      this.scanner = scanner;
    }

    @VisibleForTesting
    public ByteKeyRangeTracker getRangeTracker() {
      return rangeTracker;
    }

    /** Closes the {@link ResultScanner}, {@link Table}, and {@link Connection}. */
    @Override
    public void close() throws IOException {
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
      if (connection != null) {
        connection.close();
        connection = null;
      }
      long totalOps = getRowsReadCount();
      long elapsedTimeMs = System.currentTimeMillis() - workStart;
      long operationsPerSecond = elapsedTimeMs == 0 ? 0 : (totalOps * 1000 / elapsedTimeMs);
      READER_LOG.info(
          "{} Complete: {} operations in {} ms. That's {} operations/sec",
          this,
          totalOps,
          elapsedTimeMs,
          operationsPerSecond);
    }

    @VisibleForTesting
    long getRowsReadCount() {
      return rowsRead.get();
    }

    @Override
    public final Result getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public final synchronized BoundedSource<Result> getCurrentSource() {
      return source;
    }

    @Override
    public String toString() {
      return String.format(
          "Reader for: ['%s' - '%s']",
          Bytes.toStringBinary(rangeTracker.getStartPosition().getBytes()),
          Bytes.toStringBinary(rangeTracker.getStopPosition().getBytes()));
    }
  }

  ///////////////////// Write Class /////////////////////////////////

  /**
   * This is a DoFn that relies on {@link BufferedMutator} as the implementation to write data to
   * Cloud Bigtable. The main function of this class is to manage Aggregators relating to mutations.
   *
   * @param <InputType>
   */
  private abstract static class BufferedMutatorDoFn<InputType>
      extends AbstractCloudBigtableTableDoFn<InputType, Void> {

    private static final long serialVersionUID = 1L;

    // Stats
    protected static final Counter mutationsCounter =
        Metrics.counter(CloudBigtableIO.class, "Mutations");
    protected static final Counter exceptionsCounter =
        Metrics.counter(CloudBigtableIO.class, "Exceptions");
    protected static final Gauge cumulativeThrottlingSeconds =
        Metrics.gauge(CloudBigtableIO.class, "ThrottlingSeconds");

    public BufferedMutatorDoFn(CloudBigtableConfiguration config) {
      super(config);
    }

    @Setup
    public synchronized void setup() {
      // TODO set up buffered mutator stats exporter
    }

    protected BufferedMutator createBufferedMutator(Object context, String tableName)
        throws IOException {
      return getConnection()
          .getBufferedMutator(
              new BufferedMutatorParams(TableName.valueOf(tableName))
                  .listener(createExceptionListener(context)));
    }

    protected ExceptionListener createExceptionListener(final Object context) {
      return new ExceptionListener() {
        @Override
        public void onException(
            RetriesExhaustedWithDetailsException exception, BufferedMutator mutator)
            throws RetriesExhaustedWithDetailsException {
          logExceptions(context, exception);
          throw exception;
        }
      };
    }
  }

  /**
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of {@link
   * Mutation}s to a table specified via a {@link CloudBigtableTableConfiguration} using the
   * BufferedMutator.
   */
  @InternalExtensionOnly
  public static class CloudBigtableSingleTableBufferedWriteFn
      extends BufferedMutatorDoFn<Mutation> {
    private static final long serialVersionUID = 2L;
    private transient BufferedMutator mutator;

    public CloudBigtableSingleTableBufferedWriteFn(CloudBigtableTableConfiguration config) {
      super(config);
    }

    @StartBundle
    public void setupBufferedMutator(StartBundleContext context) throws IOException {
      mutator =
          createBufferedMutator(
              context, ((CloudBigtableTableConfiguration) getConfig()).getTableId());
    }

    /** Performs an asynchronous mutation via {@link BufferedMutator#mutate(Mutation)}. */
    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      Mutation mutation = context.element();
      if (DOFN_LOG.isTraceEnabled()) {
        DOFN_LOG.trace("Persisting {}", Bytes.toStringBinary(mutation.getRow()));
      }
      mutator.mutate(mutation);
      mutationsCounter.inc();
    }

    /** Closes the {@link BufferedMutator} and {@link Connection}. */
    @FinishBundle
    public synchronized void finishBundle(@SuppressWarnings("unused") FinishBundleContext context)
        throws Exception {
      try {
        if (mutator != null) {
          mutator.close();
          mutator = null;
        }
      } catch (RetriesExhaustedWithDetailsException exception) {
        exceptionsCounter.inc(exception.getCauses().size());
        logExceptions(null, exception);
        rethrowException(exception);
      }
    }
  }

  /**
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of {@link KV}
   * of (String tableName, List of {@link Mutation}s) to the specified table.
   *
   * <p>NOTE: This {@link DoFn} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences. Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one. In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
  @InternalExtensionOnly
  public static class CloudBigtableMultiTableWriteFn
      extends BufferedMutatorDoFn<KV<String, Iterable<Mutation>>> {
    private static final long serialVersionUID = 2L;

    // Stats
    private transient Map<String, BufferedMutator> mutators;

    public CloudBigtableMultiTableWriteFn(CloudBigtableConfiguration config) {
      super(config);
    }

    @StartBundle
    public void startBundle() throws Exception {
      mutators = new HashMap<>();
    }

    /**
     * Uses the connection to create a new {@link Table} to write the {@link Mutation}s to.
     *
     * <p>NOTE: This method does not create a new table in Cloud Bigtable. The table must already
     * exist.
     *
     * @param context The context for the {@link DoFn}.
     */
    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      KV<String, Iterable<Mutation>> element = context.element();
      BufferedMutator mutator = getMutator(context, element.getKey());
      try {
        for (Mutation mutation : element.getValue()) {
          mutator.mutate(mutation);
          mutationsCounter.inc();
        }
      } catch (RetriesExhaustedWithDetailsException exception) {
        logExceptions(context, exception);
        rethrowException(exception);
      }
    }

    private BufferedMutator getMutator(Object context, String tableName) throws IOException {
      BufferedMutator mutator = mutators.get(tableName);
      if (mutator == null) {
        mutator = createBufferedMutator(context, tableName);
        mutators.put(tableName, mutator);
      }
      return mutator;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      try {
        for (BufferedMutator bufferedMutator : mutators.values()) {
          try {
            bufferedMutator.close();
          } catch (RetriesExhaustedWithDetailsException exception) {
            logExceptions(c, exception);
            rethrowException(exception);
          }
        }
      } finally {
        mutators.clear();
      }
    }
  }

  /**
   * A {@link PTransform} that wraps around a {@link DoFn} that will write {@link Mutation}s to
   * Cloud Bigtable.
   */
  private static class CloudBigtableWriteTransform<T> extends PTransform<PCollection<T>, PDone> {
    private static final long serialVersionUID = -2888060194257930027L;

    private final DoFn<T, Void> function;
    private final CloudBigtableConfiguration configuration;

    public CloudBigtableWriteTransform(
        DoFn<T, Void> function, CloudBigtableConfiguration configuration) {
      this.function = function;
      this.configuration = configuration;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(function));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      configuration.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      function.populateDisplayData(builder);
    }
  }

  /**
   * Creates a {@link PTransform} that can write either a bounded or unbounded {@link PCollection}
   * of {@link Mutation}s to a table specified via a {@link CloudBigtableTableConfiguration}.
   *
   * <p>NOTE: This {@link PTransform} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences. Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one. In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
  public static PTransform<PCollection<Mutation>, PDone> writeToTable(
      CloudBigtableTableConfiguration config) {
    DoFn<Mutation, Void> writeFn = new CloudBigtableSingleTableBufferedWriteFn(config);
    return new CloudBigtableWriteTransform<>(writeFn, config);
  }

  private static Coder<Result> getResultCoder() {
    try {
      return CoderRegistry.createDefault().getCoder(Result.class);
    } catch (CannotProvideCoderException e) {
      e.printStackTrace();
      throw new RuntimeException("Please add beam-sdks-java-io-hbase to your dependencies", e);
    }
  }

  /**
   * Creates a {@link PTransform} that can write either a bounded or unbounded {@link PCollection}
   * of {@link KV} of (String tableName, List of {@link Mutation}s) to the specified table.
   *
   * <p>NOTE: This {@link PTransform} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences. Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one. In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
  public static PTransform<PCollection<KV<String, Iterable<Mutation>>>, PDone>
      writeToMultipleTables(CloudBigtableConfiguration config) {
    return new CloudBigtableWriteTransform<>(new CloudBigtableMultiTableWriteFn(config), config);
  }

  /**
   * Creates a {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially
   * filtered by a {@link Scan}.
   */
  public static BoundedSource<Result> read(CloudBigtableScanConfiguration config) {
    return new Source(config);
  }
}
