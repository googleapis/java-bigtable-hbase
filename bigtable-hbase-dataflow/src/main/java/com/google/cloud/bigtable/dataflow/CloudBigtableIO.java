/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Preconditions;
import com.google.bigtable.v1.BigtableServiceGrpc.BigtableService;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.hbase1_0.BigtableConnection;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.annotations.VisibleForTesting;

/**
 * <p>
 * Utilities to create {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s for reading and
 * writing <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a> entities in a
 * Cloud Dataflow pipeline.
 * </p>
 *
 * <p>
 * Google Cloud Bigtable offers you a fast, fully managed, massively scalable NoSQL database service
 * that's ideal for web, mobile, and Internet of Things applications requiring terabytes to
 * petabytes of data. Unlike comparable market offerings, Cloud Bigtable doesn't require you to
 * sacrifice speed, scale, or cost efficiency when your applications grow. Cloud Bigtable has been
 * battle-tested at Google for more than 10 years--it's the database driving major applications such
 * as Google Analytics and Gmail.
 * </p>
 *
 * <p>
 * To use {@link CloudBigtableIO}, users must use gcloud to get a credential for Cloud Bigtable:
 *
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>
 * To read a {@link PCollection} from a table, with an optional
 * {@link Scan}, use {@link CloudBigtableIO#read(CloudBigtableScanConfiguration)}:
 * </p>
 *
 * <pre>
 * {@code
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Result> = p.apply(
 *   Read.from(CloudBigtableIO.read(
 *      new CloudBigtableScanConfiguration.Builder()
 *          .withProjectId("project-id")
 *          .withZoneId("zone-id")
 *          .withClusterId("cluster-id")
 *          .withTableId("table-id")
 *          .build())));
 * }
 * </pre>
 *
 * <p>
 * To write a {@link PCollection} to a table, use
 * {@link CloudBigtableIO#writeToTable(CloudBigtableTableConfiguration)}:
 * </p>
 *
 * <pre>
 * {@code
 * PipelineOptions options =
 *     PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Mutation> mutationCollection = ...;
 * mutationCollection.apply(
 *   CloudBigtableIO.writeTo(
 *      new CloudBigtableScanConfiguration.Builder()
 *          .withProjectId("project-id")
 *          .withZoneId("zone-id")
 *          .withClusterId("cluster-id")
 *          .withTableId("table-id")
 *          .build()));
 * }
 * </pre>
 */

public class CloudBigtableIO {

  private static Logger LOG = LoggerFactory.getLogger(CloudBigtableIO.class);

  /**
   * Performs a {@link ResultScanner#next()} or {@link ResultScanner#next(int)}.  It also checks if
   * the {@link ResultOutputType} marks the last value in the {@ResultScan}.
   *
   * @param <ResultOutputType> is either a {@link Result} or {@link Result}[];
   */
  private interface ScanIterator<ResultOutputType> extends Serializable {
    /**
     * Get the next unit of work.
     */
    ResultOutputType next(ResultScanner resultScanner) throws IOException;

    /**
     * Is the work complete? Checks for null in the case of {@link Result}, or empty in the case of
     * an array of Results.
     */
    boolean isCompletionMarker(ResultOutputType result);

    long getRowCount(ResultOutputType current);
  }

  /**
   * Iterates the {@link ResultScanner} via {@link ResultScanner#next()}.
   */
  private static final ScanIterator<Result> RESULT_ADVANCER = new ScanIterator<Result>() {
    private static final long serialVersionUID = 1L;

    @Override
    public Result next(ResultScanner resultScanner) throws IOException {
      return resultScanner.next();
    }

    @Override
    public boolean isCompletionMarker(Result result) {
      return result == null;
    }

    @Override
    public long getRowCount(Result result) {
      return result == null ? 0 : 1;
    }
  };

  /**
   * Iterates the {@link ResultScanner} via {@link ResultScanner#next(int)}.
   */
  private static final class ResultArrayIterator implements ScanIterator<Result[]> {
    private static final long serialVersionUID = 1L;
    private final int arraySize;

    public ResultArrayIterator(int arraySize) {
      this.arraySize = arraySize;
    }

    @Override
    public Result[] next(ResultScanner resultScanner) throws IOException {
      return resultScanner.next(arraySize);
    }

    @Override
    public boolean isCompletionMarker(Result[] result) {
      return result == null || result.length == 0;
    }

    @Override
    public long getRowCount(Result[] result) {
      return result == null ? 0 : result.length;
    }
  }

  /**
   * A {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially filtered by a
   * {@link Scan}.
   */
  public static class Source<ResultOutputType> extends BoundedSource<ResultOutputType> {
    private static final long serialVersionUID = -5580115943635114126L;
    private static final Logger SOURCE_LOG = LoggerFactory.getLogger(Source.class);
    private static final long MAX_SPLIT_COUNT = 4_000;

    /**
     * Configuration for a Cloud Bigtable connection, a table, and an optional scan.
     */
    private final CloudBigtableScanConfiguration configuration;
    private transient List<SampleRowKeysResponse> sampleRowKeys;
    private final Coder<ResultOutputType> defaultCoder;
    private final ScanIterator<ResultOutputType> scanIterator;

    /**
     * A {@link BoundedSource} for a Cloud Bigtable {@link Table} with a start/stop key range, along
     * with a potential filter via a {@link Scan}.
     */
    protected class SourceWithKeys extends BoundedSource<ResultOutputType> {
      private static final long serialVersionUID = 2561066007121040429L;

      /**
       * Start key for the range to be scanned.
       */
      private final byte[] startKey;

      /**
       * Stop key for the range to be scanned.
       */
      private final byte[] stopKey;

      /**
       * An estimate of the size of the source, in bytes.
       *
       * <p>NOTE: This value is a guesstimate. It could be significantly off, especially if there is
       * a {@link Scan} selected in the configuration. It will also be off if the start and stop
       * keys are calculated via
       * {@link CloudBigtableIO.Source#splitIntoBundles(long, PipelineOptions)}.
       */
      private final long estimatedSize;

      @VisibleForTesting
      protected SourceWithKeys(byte[] startKey, byte[] stopKey, long estimatedSize) {
        if (stopKey.length > 0) {
          Preconditions.checkState(Bytes.compareTo(startKey, stopKey) < 0,
              "Source keys not in order: [%s, %s]", Bytes.toStringBinary(startKey),
              Bytes.toStringBinary(stopKey));
          Preconditions.checkState(estimatedSize > 0, "Source size must be positive",
              estimatedSize);
        }
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.estimatedSize = estimatedSize;
        SOURCE_LOG.debug("Source with split: {}.", this);
      }

      /**
       * Gets an estimate of the size of the source.
       *
       * <p>NOTE: This value is a guesstimate. It could be significantly off, especially if there is
       * a{@link Scan} selected in the configuration. It will also be off if the start and stop keys
       * are calculated via {@link Source#splitIntoBundles(long, PipelineOptions)}.
       *
       * @param options The pipeline options.
       * @return The estimated size of the source, in bytes.
       */
      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return estimatedSize;
      }

      /**
       * Checks whether the pipeline produces sorted keys.
       *
       * <p>NOTE: HBase supports reverse scans, but Cloud Bigtable does not.
       *
       * @param options The pipeline options.
       * @return Whether the pipeline produces sorted keys.
       */
      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return true;
      }

      /**
       * Gets a coder for HBase {@link Result} objects. See {@link HBaseResultCoder} for more info.
       *
       * @return A coder for {@link Result} objects.
       */
      @Override
      public Coder<ResultOutputType> getDefaultOutputCoder() {
        return defaultCoder;
      }

      // TODO: Add a method on the server side that will be a more precise split based on server-
      // side statistics
      /**
       * Splits the bundle based on the assumption that the data is distributed evenly between
       * {@link #startKey} and {@link #stopKey}. That assumption may not be correct for any specific
       * start/stop key combination.
       *
       * <p>This method is called internally by Cloud Dataflow. Do not call it directly.
       *
       * @param desiredBundleSizeBytes The desired size for each bundle, in bytes.
       * @param options The pipeline options.
       * @return A list of sources split into groups.
       */
      @Override
      public List<BoundedSource<ResultOutputType>> splitIntoBundles(long desiredBundleSizeBytes,
          PipelineOptions options) throws Exception {
        List<BoundedSource<ResultOutputType>> newSplits =
            split(estimatedSize, desiredBundleSizeBytes, startKey, stopKey);
        SOURCE_LOG.trace("Splitting split {} into {}", this, newSplits);
        return newSplits;
      }

      /**
       * Validates the existence of the table in the configuration.
       */
      @Override
      public void validate() {
        CloudBigtableIO.validate(configuration, configuration.getTableId());
      }

      /**
       * Creates a {@link Scan} given the value of the supplied
       * {@link CloudBigtableScanConfiguration}'s scan with the key range specified by the
       * {@link #startKey} and {@link #stopKey}.
       */
      @Override
      public BoundedSource.BoundedReader<ResultOutputType> createReader(PipelineOptions options)
          throws IOException {
        Scan scan = new Scan(configuration.getScan());
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
        return new CloudBigtableIO.Reader<>(this, configuration, scan, scanIterator);
      }

      @Override
      public String toString() {
        return String.format("Split start: '%s', end: '%s', size: %d",
            Bytes.toStringBinary(startKey), Bytes.toStringBinary(stopKey), estimatedSize);
      }
    }

    Source(
        CloudBigtableScanConfiguration configuration,
        Coder<ResultOutputType> defaultCoder,
        ScanIterator<ResultOutputType> scanIterator) {
      this.configuration = configuration;
      this.defaultCoder = defaultCoder;
      this.scanIterator = scanIterator;
    }

    /**
     * Creates a coder for HBase {@link Result} objects. See {@link HBaseResultCoder} for more info.
     *
     * @return A coder for {@link Result} objects.
     */
    @Override
    public Coder<ResultOutputType> getDefaultOutputCoder() {
      return defaultCoder;
    }

    // TODO: Add a method on the server side that will be a more precise split based on server-side
    // statistics
    /**
     * Splits the table based on keys that belong to tablets, known as "regions" in the HBase API.
     * The current implementation uses the HBase {@link RegionLocator} interface, which calls
     * {@link BigtableService#sampleRowKeys(SampleRowKeysRequest,
     * com.google.bigtable.repackaged.io.grpc.stub.StreamObserver)} under the covers.
     * A {@link SourceWithKeys} may correspond to a single region or a portion of a region.
     *
     * <p>
     * If a split is smaller than a single region, the split is calculated based on the assumption
     * that the data is distributed evenly between the region's startKey and stopKey. That
     * assumption may not be correct for any specific start/stop key combination.
     * </p>
     *
     * <p>This method is called internally by Cloud Dataflow. Do not call it directly.</p>
     *
     * @param desiredBundleSizeBytes The desired size for each bundle, in bytes.
     * @param options The pipeline options.
     * @return A list of sources split into groups.
     */
    @Override
    public List<BoundedSource<ResultOutputType>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // Update the desiredBundleSizeBytes in order to limit the number of splits to
      // MAX_SPLIT_COUNT.  This is an extremely rough estimate for large scale jobs.  There is
      // currently a hard limit on both the count of Sources as well as the sum of the sizes of
      // serialized Sources.  This solution will not work for large workloads for cases where either
      // the row key sizes are large, or the scan is large.
      //
      // TODO: Work on a more robust algorithm for splitting that works for more cases.
      long sizeEstimate = getEstimatedSizeBytes(options);
      desiredBundleSizeBytes = Math.max(sizeEstimate / MAX_SPLIT_COUNT, desiredBundleSizeBytes);
      List<BoundedSource<ResultOutputType>> splits = getSplits(desiredBundleSizeBytes);
      SOURCE_LOG.info("Creating {} splits.", splits.size());
      SOURCE_LOG.debug("Created splits {}.", splits);
      return splits;
    }

    // TODO: Move the splitting logic to bigtable-hbase, and separate concerns between dataflow needs
    // and Cloud Bigtable logic.
    protected List<BoundedSource<ResultOutputType>> getSplits(long desiredBundleSizeBytes) throws IOException {
      Scan scan = configuration.getScan();
      byte[] scanStartKey = scan.getStartRow();

      byte[] scanEndKey = scan.getStopRow();
      List<BoundedSource<ResultOutputType>> splits = new ArrayList<>();
      byte[] startKey = HConstants.EMPTY_START_ROW;
      long lastOffset = 0;
      for (SampleRowKeysResponse response : getSampleRowKeys()) {
        byte[] endKey = response.getRowKey().toByteArray();
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
      return splits;
    }

    /**
     * Checks if the range of the region is within the range of the scan.
     */
    private static boolean isWithinRange(byte[] scanStartKey, byte[] scanEndKey,
        byte[] startKey, byte[] endKey) {
      return (scanStartKey.length == 0 || endKey.length == 0
              || Bytes.compareTo(scanStartKey, endKey) < 0)
          && (scanEndKey.length == 0 || Bytes.compareTo(scanEndKey, startKey) > 0);
    }

    /**
     * Performs a call to get sample row keys from
     * {@link BigtableDataClient#sampleRowKeys(SampleRowKeysRequest)} if they are not yet cached.
     * The sample row keys give information about tablet key boundaries and estimated sizes.
     */
    public synchronized List<SampleRowKeysResponse> getSampleRowKeys() throws IOException {
      if (sampleRowKeys == null) {
        BigtableOptions bigtableOptions = configuration.toBigtableOptions();
        try (BigtableSession session = new BigtableSession(bigtableOptions)) {
          BigtableTableName tableName =
              bigtableOptions.getClusterName().toTableName(configuration.getTableId());
          SampleRowKeysRequest request =
              SampleRowKeysRequest.newBuilder().setTableName(tableName.toString()).build();
          sampleRowKeys = session.getDataClient().sampleRowKeys(request);
        }
      }
      return sampleRowKeys;
    }

    /**
     * Validates the existence of the table in the configuration.
     */
    @Override
    public void validate() {
      CloudBigtableIO.validate(configuration, configuration.getTableId());
    }

    /**
     * Gets an estimated size based on data returned from {@link #getSampleRowKeys}. The estimate
     * will be high if a {@link Scan} is set on the {@link CloudBigtableScanConfiguration}; in such
     * cases, the estimate will not take the Scan into account, and will return a larger estimate
     * than what the {@link CloudBigtableIO.Reader} will actually read.
     *
     * @param options The pipeline options.
     * @return The estimated size of the data, in bytes.
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      long totalEstimatedSizeBytes = 0;

      Scan scan = configuration.getScan();
      byte[] scanStartKey = scan.getStartRow();
      byte[] scanEndKey = scan.getStopRow();

      byte[] startKey = HConstants.EMPTY_START_ROW;
      long lastOffset = 0;
      for (SampleRowKeysResponse response : getSampleRowKeys()) {
        byte[] currentEndKey = response.getRowKey().toByteArray();
        // Avoid empty regions.
        if (Bytes.equals(startKey, currentEndKey) && startKey.length != 0) {
          continue;
        }
        long offset = response.getOffsetBytes();
        if (isWithinRange(scanStartKey, scanEndKey, startKey, currentEndKey)) {
          totalEstimatedSizeBytes += (offset - lastOffset);
        }
        lastOffset = offset;
        startKey = currentEndKey;
      }
      SOURCE_LOG.info("Estimated size in bytes: " + totalEstimatedSizeBytes);

      return totalEstimatedSizeBytes;
    }

    /**
     * Checks whether the pipeline produces sorted keys.
     *
     * <p>NOTE: HBase supports reverse scans, but Cloud Bigtable does not.
     *
     * @param options The pipeline options.
     * @return Whether the pipeline produces sorted keys.
     */
    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return true;
    }

    /**
     * Creates a reader that will scan the entire table based on the {@link Scan} in the
     * configuration.
     * @return A reader for the table.
     */
    @Override
    public BoundedSource.BoundedReader<ResultOutputType> createReader(PipelineOptions options)
        throws IOException {
      return new CloudBigtableIO.Reader<>(
          this, configuration, configuration.getScan(), scanIterator);
    }

    /**
     * Splits the region based on the start and stop key. Uses
     * {@link Bytes#split(byte[], byte[], int)} under the covers.
     */
    private List<BoundedSource<ResultOutputType>> split(long regionSize, long desiredBundleSizeBytes,
        byte[] startKey, byte[] stopKey) {
      if (regionSize < desiredBundleSizeBytes || stopKey.length == 0) {
        BoundedSource<ResultOutputType> source = createSourceWithKeys(startKey, stopKey, regionSize);
        return Collections.singletonList(source);
      } else {
        Preconditions.checkState(desiredBundleSizeBytes > 0);
        if (stopKey.length > 0) {
          Preconditions.checkState(Bytes.compareTo(startKey, stopKey) <= 0,
              "Source keys not in order: [%s, %s]", Bytes.toStringBinary(startKey),
              Bytes.toStringBinary(stopKey));
          Preconditions.checkState(regionSize > 0, "Source size must be positive",
              regionSize);
        }
        int splitCount = (int) Math.ceil((double) (regionSize) / (double) (desiredBundleSizeBytes));
        byte[][] splitKeys = Bytes.split(startKey, stopKey, splitCount - 1);
        Preconditions.checkState(splitCount + 1 == splitKeys.length);
        List<BoundedSource<ResultOutputType>> result = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
          result.add(createSourceWithKeys(splitKeys[i], splitKeys[i + 1], regionSize));
        }
        return result;
      }
    }

    @VisibleForTesting
    BoundedSource<ResultOutputType> createSourceWithKeys(byte[] startKey, byte[] stopKey, long size) {
      return new SourceWithKeys(startKey, stopKey, size);
    }
  }

  /**
   * Reads rows for a specific {@link Table}, usually filtered by a {@link Scan}.
   */
  private static class Reader<Results> extends BoundedReader<Results> {
    private static final Logger READER_LOG = LoggerFactory.getLogger(Reader.class);

    private final BoundedSource<Results> source;
    private final Scan scan;
    private final CloudBigtableScanConfiguration config;
    private final ScanIterator<Results> scanIterator;

    private volatile Connection connection;
    private volatile ResultScanner scanner;
    private volatile Table table;
    private volatile Results current;
    protected long workStart;
    private final AtomicLong rowsRead = new AtomicLong();

    private Reader(
        BoundedSource<Results> source,
        CloudBigtableScanConfiguration config,
        Scan scan,
        ScanIterator<Results> scanIterator) {
      this.source = source;
      this.config = config;
      this.scan = scan;
      this.scanIterator = scanIterator;
    }

    /**
     * Calls {@link ResultScanner#next()}.
     */
    @Override
    public boolean advance() throws IOException {
      current = scanIterator.next(scanner);
      rowsRead.addAndGet(scanIterator.getRowCount(current));
      return !scanIterator.isCompletionMarker(current);
    }

    /**
     * Creates a {@link Connection}, {@link Table} and {@link ResultScanner} and advances to the
     * next {@link Result}.
      */
    @Override
    public boolean start() throws IOException {
      long connectionStart = System.currentTimeMillis();
      connection = new BigtableConnection(config.toHBaseConfig());
      table = connection.getTable(TableName.valueOf(config.getTableId()));
      scanner = table.getScanner(scan);
      workStart = System.currentTimeMillis();
      READER_LOG.info("{} Starting work. Creating Scanner took: {} ms.",
        this,
        workStart - connectionStart);
      return advance();
    }

    /**
     * Closes the {@link ResultScanner}, {@link Table}, and {@link Connection}.
     */
    @Override
    public void close() throws IOException {
      scanner.close();
      table.close();
      connection.close();
      long totalOps = rowsRead.get();
      long elapsedTimeMs = System.currentTimeMillis() - workStart;
      long operationsPerSecond = totalOps * 1000 / elapsedTimeMs;
      READER_LOG.info(
          "{} Complete: {} operations in {} ms. That's {} operations/sec",
          this,
          totalOps,
          elapsedTimeMs,
          operationsPerSecond);
    }

    @Override
    public Results getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public BoundedSource<Results> getCurrentSource() {
      return source;
    }

    @Override
    public String toString() {
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();
      return String.format(
          "Reader for: ['%s' - '%s']",
          Bytes.toStringBinary(startRow),
          Bytes.toStringBinary(stopRow));
    }
  }

  ///////////////////// Write Class /////////////////////////////////

  /**
   * Initializes the coders for the Cloud Bigtable Write {@link PTransform}. Sets up {@link Coder}s
   * required to serialize HBase {@link Put}, {@link Delete}, and {@link Mutation} objects. See
   * {@link HBaseMutationCoder} for additional implementation details.
   *
   * @return The {@link Pipeline} for chaining convenience.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Pipeline initializeForWrite(Pipeline p) {
    // This enables the serialization of various Mutation types in the pipeline.
    CoderRegistry registry = p.getCoderRegistry();
    Coder coder = new HBaseMutationCoder();

    // MutationCoder only supports Puts and Deletes. It will throw exceptions for Increment
    // and Append since they are not idempotent. Put is logically idempotent if the column family
    // has a single version(); multiple versions are fine for most cases.  If it's not, add
    // a timestamp to the Put to make it fully idempotent.
    registry.registerCoder(Put.class, coder);
    registry.registerCoder(Delete.class, coder);
    registry.registerCoder(Mutation.class, coder);

    return p;
  }



  /**
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of
   * {@link Mutation}s to a table specified via a {@link CloudBigtableTableConfiguration} using the
   * BufferedMutator.
   */
  public static class CloudBigtableSingleTableBufferedWriteFn
      extends AbstractCloudBigtableTableDoFn<Mutation, Void> {
    private static final long serialVersionUID = 2L;
    private transient BufferedMutator mutator;
    private final String tableName;

    // Stats
    private final Aggregator<Long, Long> mutationsCounter;
    private final Aggregator<Long, Long> exceptionsCounter;

    public CloudBigtableSingleTableBufferedWriteFn(CloudBigtableTableConfiguration config) {
      super(config);
      tableName = config.getTableId();
      mutationsCounter = createAggregator("mutations", new Sum.SumLongFn());
      exceptionsCounter = createAggregator("exceptions", new Sum.SumLongFn());
    }

    private synchronized BufferedMutator getBufferedMutator(Context context)
        throws IOException {
      if (mutator == null) {
        ExceptionListener listener = createExceptionListener(context);
        BufferedMutatorParams params =
            new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(
              AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT).listener(listener);
        mutator = getConnection().getBufferedMutator(params);
      }
      return mutator;
    }

    protected ExceptionListener createExceptionListener(final Context context) {
      return new ExceptionListener() {
        @Override
        public void onException(RetriesExhaustedWithDetailsException exception,
            BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
          logExceptions(context, exception);
          throw exception;
        }
      };
    }

    /**
     * Performs an asynchronous mutation via {@link BufferedMutator#mutate(Mutation)}.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      Mutation mutation = context.element();
      if (DOFN_LOG.isTraceEnabled()) {
        DOFN_LOG.trace("Persisting {}", Bytes.toStringBinary(mutation.getRow()));
      }
      getBufferedMutator(context).mutate(mutation);
      mutationsCounter.addValue(1L);
    }

    /**
     * Closes the {@link BufferedMutator} and {@link Connection}.
     */
    @Override
    public void finishBundle(Context context) throws Exception {
      try {
        if (mutator != null) {
          mutator.close();
        }
      } catch (RetriesExhaustedWithDetailsException exception) {
        exceptionsCounter.addValue((long) exception.getCauses().size());
        logExceptions(context, exception);
        retrowException(exception);
      } finally {
        // Close the connection to clean up resources.
        super.finishBundle(context);
      }
    }
  }

  /**
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of
   * {@link Mutation}s to a table specified via a {@link CloudBigtableTableConfiguration} doing a
   * a single mutation per processElement call.
   */
  public static class CloudBigtableSingleTableSerialWriteFn
      extends AbstractCloudBigtableTableDoFn<Mutation, Void> {
    // Enables the use of this class instead of the buffered mutator one.
    public static final String DO_SERIAL_WRITES = "google.bigtable.dataflow.singletable.serial";

    private static final long serialVersionUID = 2L;
    private transient Table table;
    private final String tableId;

    // Stats
    private final Aggregator<Long, Long> mutationsCounter;

    public CloudBigtableSingleTableSerialWriteFn(CloudBigtableTableConfiguration config) {
      super(config);
      tableId = config.getTableId();

      mutationsCounter = createAggregator("mutations", new Sum.SumLongFn());
    }

    private synchronized Table getTable() throws IOException {
      if (table == null) {
        table = getConnection().getTable(TableName.valueOf(tableId));
      }
      return table;
    }

    /**
     * Performs an asynchronous mutation via {@link BufferedMutator#mutate(Mutation)}.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      Mutation mutation = context.element();
      if (DOFN_LOG.isTraceEnabled()) {
        DOFN_LOG.trace("Persisting {}", Bytes.toStringBinary(mutation.getRow()));
      }

      if (mutation instanceof Put) {
        getTable().put((Put) mutation);
      } else if (mutation instanceof Delete) {
        getTable().delete((Delete) mutation);
      } else {
        throw new IllegalArgumentException("Encountered unsupported mutation type: " + mutation.getClass());
      }

      mutationsCounter.addValue(1L);
    }

    /**
     * Closes the {@link Table} and {@link Connection}.
     */
    @Override
    public void finishBundle(Context context) throws Exception {
      try {
        if (table != null) {
          table.close();
        }
      } finally {
        // Close the connection to clean up resources.
        super.finishBundle(context);
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
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
  public static class CloudBigtableMultiTableWriteFn extends
  AbstractCloudBigtableTableDoFn<KV<String, Iterable<Mutation>>, Void> {
    private static final long serialVersionUID = 2L;

    // Stats
    private final Aggregator<Long, Long> mutationsCounter;

    public CloudBigtableMultiTableWriteFn(CloudBigtableConfiguration config) {
      super(config);

      mutationsCounter = createAggregator("mutations", new Sum.SumLongFn());
    }

    /**
     * Uses the connection to create a new {@link Table} to write the {@link Mutation}s to.
     *
     * <p>NOTE: This method does not create a new table in Cloud Bigtable. The table must already
     * exist.
     *
     * @param context The context for the {@link DoFn}.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      KV<String, Iterable<Mutation>> element = context.element();
      String tableName = element.getKey();
      try (Table t = getConnection().getTable(TableName.valueOf(tableName))) {
        List<Mutation> mutations = Lists.newArrayList(element.getValue());
        int mutationCount = mutations.size();
        t.batch(mutations, new Object[mutationCount]);
        mutationsCounter.addValue((long) mutationCount);
      } catch (RetriesExhaustedWithDetailsException exception) {
        logExceptions(context, exception);
        retrowException(exception);
      }
    }
  }

  /**
   * A {@link PTransform} that wraps around a {@link DoFn} that will write {@link Mutation}s to
   * Cloud Bigtable.
   */
  public static class CloudBigtableWriteTransform<T> extends PTransform<PCollection<T>, PDone> {
    private static final long serialVersionUID = -2888060194257930027L;

    private final DoFn<T, Void> function;

    public CloudBigtableWriteTransform(DoFn<T, Void> function) {
      this.function = function;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      input.apply(ParDo.of(function));
      return PDone.in(input.getPipeline());
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
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
  public static PTransform<PCollection<Mutation>, PDone> writeToTable(
      CloudBigtableTableConfiguration config) {
    validate(config, config.getTableId());

    DoFn<Mutation, Void> writeFn = null;

    // Provide a way to do serial writes, slower but easier to troubleshoot.
    if (config.getConfiguration().get(
        CloudBigtableSingleTableSerialWriteFn.DO_SERIAL_WRITES) != null) {
      writeFn = new CloudBigtableSingleTableSerialWriteFn(config);
    } else {
      writeFn = new CloudBigtableSingleTableBufferedWriteFn(config);
    }

    return new CloudBigtableWriteTransform<>(writeFn);
  }

   /**
   * Creates a {@link PTransform} that can write either a bounded or unbounded {@link PCollection}
   * of {@link KV} of (String tableName, List of {@link Mutation}s) to the specified table.
   *
   * <p>NOTE: This {@link PTransform} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   */
   public static PTransform<PCollection<KV<String, Iterable<Mutation>>>, PDone>
      writeToMultipleTables(CloudBigtableConfiguration config) {
    validate(config);
    return new CloudBigtableWriteTransform<>(new CloudBigtableMultiTableWriteFn(config));
  }

  /**
   * Creates a {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially
   * filtered by a {@link Scan}.
   */
  public static com.google.cloud.dataflow.sdk.io.BoundedSource<Result> read(
      CloudBigtableScanConfiguration config) {
    return new Source<Result>(config, new HBaseResultCoder(), RESULT_ADVANCER);
  }

  /**
   * Creates a {@link BoundedSource} for a Cloud Bigtable {@link Table} for multiple
   * {@linkplain Result}s, which is potentially filtered by a {@link Scan}.
   * @param config The CloudBigtableScanConfiguration which defines the connection information,
   *          table and optional scan.
   * @param resultCount The number of results to get per batch
   */
  public static com.google.cloud.dataflow.sdk.io.BoundedSource<Result[]>
      readBulk(CloudBigtableScanConfiguration config, int resultCount) {
    return new Source<>(config, new HBaseResultArrayCoder(), new ResultArrayIterator(resultCount));
  }

  private static void checkNotNullOrEmpty(String value, String type) {
    checkArgument(
        !isNullOrEmpty(value), "A " + type + " must be set to configure Bigtable properly.");
  }

  private static void validate(CloudBigtableConfiguration configuration, String tableId) {
    validate(configuration);
    checkNotNullOrEmpty(tableId, "tableid");
    if (BigtableSession.isAlpnProviderEnabled()) {
      try (BigtableConnection conn = new BigtableConnection(configuration.toHBaseConfig());
          Admin admin = conn.getAdmin()) {
        Preconditions.checkState(admin.tableExists(TableName.valueOf(tableId)), "Table "
            + tableId + " does not exist.  This dataflow operation could not be run.");
      } catch (IOException | IllegalArgumentException | ExceptionInInitializerError e) {
        LOG.error(String.format("Could not validate that the table exists: %s (%s)", e.getClass()
              .getName(), e.getMessage()), e);
      }
    } else {
      LOG.info("ALPN is not configured. Skipping table existence check.");
    }
  }

  private static void validate(CloudBigtableConfiguration configuration) {
    checkNotNullOrEmpty(configuration.getProjectId(), "projectId");
    checkNotNullOrEmpty(configuration.getZoneId(), "zoneId");
    checkNotNullOrEmpty(configuration.getClusterId(), "clusterId");
  }
}
