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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

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
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

/**
 * <p>
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s for reading and writing <a
 * href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a> entities.
 * </p>
 * <p>
 * Google Cloud Bigtable offers you a fast, fully managed, massively scalable NoSQL database service
 * that's ideal for web, mobile, and Internet of Things applications requiring terabytes to
 * petabytes of data. Unlike comparable market offerings, Cloud Bigtable doesn't require you to
 * sacrifice speed, scale, or cost efficiency when your applications grow. Cloud Bigtable has been
 * battle-tested at Google for more than 10 years—it's the database driving major applications such
 * as Google Analytics and Gmail.
 * </p>
 * <p>
 * To use {@link CloudBigtableIO}, users must use gcloud to get a credential for Cloud Bigtable:
 *
 * <pre>
 * $ gcloud auth login
 * </pre>
 * <p>
 * To read a {@link PCollection} from a table, with an optional
 * {@link Scan}, use {@link CloudBigtableIO#read(CloudBigtableScanConfiguration)}. For example:
 * </p>
 *
 * <pre>
 * {@code
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Pipeline p = Pipeline.create(options);
 * PCollection<Result> = p.apply(
 *   Read.from(CloudBigtableIO.read(
 *      new CloudBigtableScanConfiguration.Builder()
 *          .withProjectId("bigtable-project")
 *          .withZoneId("bigtable-zone")
 *          .withClusterId("cluster-id")
 *          .withTableId("table-id")
 *          .build())));
 * }
 * </pre>
 * <p>
 * To write a {@link PCollection} to a table, use
 * {@link CloudBigtableIO#writeToTable(CloudBigtableTableConfiguration)}, specifying the table to
 * write to:
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
 *          .withProjectId("bigtable-project")
 *          .withZoneId("bigtable-zone")
 *          .withClusterId("cluster-id")
 *          .withTableId("table-id")
 *          .build()));
 * }
 * </pre>
 */

public class CloudBigtableIO {

  private static Logger LOG = LoggerFactory.getLogger(CloudBigtableIO.class);

  /**
   * A {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially filtered by a
   * {@link Scan}.
   */
  static class Source extends BoundedSource<Result> {
    private static final long serialVersionUID = -5580115943635114126L;
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    /**
     * Configuration for a Cloud Bigtable connection, a table, and an optional scan.
     */
    private final CloudBigtableScanConfiguration configuration;
    private transient List<SampleRowKeysResponse> sampleRowKeys;

    /**
     * A {@link BoundedSource} for a Cloud Bigtable {@link Table} with a start/stop key range, along
     * with a potential filter via a {@link Scan}.
     */
    public class SourceWithKeys extends BoundedSource<Result> {
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
       * An estimate of the size of the source. NOTE: This value is a guesstimate. It could be be
       * significantly off, especially if there is a Scan selected in the configuration. It will
       * also be off if the start and stop keys are calculated via
       * {@link CloudBigtableIO.Source#splitIntoBundles(long, PipelineOptions)}.
       */
      private final long estimatedSize;

      public SourceWithKeys(byte[] startKey, byte[] stopKey, long size) {
        if (stopKey.length > 0) {
          Preconditions.checkState(Bytes.compareTo(startKey, stopKey) < 0,
            "[startKey, stopKey]", new String(startKey), new String(stopKey));
          Preconditions.checkState(size > 0);
        }
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.estimatedSize = size;
        LOG.debug("Creating split: {}.", this);
      }

      /**
       * Gets an estimate of the size of the source. NOTE: This value is a guesstimate. It could be
       * significantly off, especially if there is a {@link Scan} selected in the configuration. It
       * will also be off if the start and stop keys are calculated via
       * {@link Source#splitIntoBundles(long, PipelineOptions)}.
       * @param options The pipeline options.
       * @return The estimated size of the source, in bytes.
       */
      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return estimatedSize;
      }

      /**
       * Checks whether the pipeline produces sorted keys. NOTE: HBase supports reverse scans, but
       * Cloud Bigtable does not.
       * @param options The pipeline options.
       * @return Whether the pipeline produces sorted keys.
       */
      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return true;
      }

      /**
       * Creates a coder for HBase {@link Result} objects. See {@link HBaseResultCoder} for more
       * info.
       * @return A coder for {@link Result} objects.
       */
      @Override
      public Coder<Result> getDefaultOutputCoder() {
        return new HBaseResultCoder();
      }

      // TODO: Add a method on the server side that will be a more precise split based on server-
      // side statistics
      /**
       * <p>Splits the bundle based on the assumption that the data is distributed evenly between
       * {@link #startKey} and {@link #stopKey}. That assumption may not be correct for any specific
       * start/stop key combination.</p>
       * <p>This method is called internally by Cloud Dataflow. Do not call it directly.</p>
       * @param desiredBundleSizeBytes The desired size for each bundle, in bytes.
       * @param options The pipeline options.
       * @return A list of sources split into groups.
       */
      @Override
      public List<SourceWithKeys> splitIntoBundles(long desiredBundleSizeBytes,
          PipelineOptions options) throws Exception {
        List<SourceWithKeys> newSplits = split(estimatedSize, desiredBundleSizeBytes, startKey, stopKey);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Splitting split {} into {}", this, newSplits);
        }
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
      public BoundedSource.BoundedReader<Result> createReader(PipelineOptions options)
          throws IOException {
        Scan scan = new Scan(configuration.getScan());
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
        return new CloudBigtableIO.Source.Reader(this, configuration, scan);
      }

      /**
       * Gets the start key for the range to be scanned.
       * @return The start key.
       */
      public byte[] getStartKey() {
        return startKey;
      }

      /**
       * Gets the stop key for the range to be scanned.
       * @return The stop key.
       */
      public byte[] getStopKey() {
        return stopKey;
      }

      /**
       * Gets an estimate of the size of the source. NOTE: This value is a guesstimate. It could be
       * significantly off, especially if there is a {@link Scan} selected in the configuration. It
       * will also be off if the start and stop keys are calculated via
       * {@link Source#splitIntoBundles(long, PipelineOptions)}.
       * @return The estimated size of the source, in bytes.
       */
      public long getEstimatedSize() {
        return estimatedSize;
      }

      @Override
      public String toString() {
        return String.format("Split start: '%s', end: '%s', size: %d", Bytes.toString(startKey),
          Bytes.toString(getStopKey()), getEstimatedSize());
      }
    }

    /**
     * Reads rows for a specific {@link Table}, usually filtered by a {@link Scan}.
     */
    private static class Reader extends BoundedReader<Result> {
      private final BoundedSource<Result> source;
      private final Scan scan;
      private final CloudBigtableScanConfiguration config;

      private volatile Connection connection;
      private ResultScanner scanner;
      private Table table;
      private Result current;

      private Reader(
          BoundedSource<Result> source, CloudBigtableScanConfiguration config, Scan scan) {
        this.source = source;
        this.config = config;
        this.scan = scan;
      }

      /**
       * Calls {@link ResultScanner#next()}.
       */
      @Override
      public boolean advance() throws IOException {
        current = scanner.next();
        return current != null;
      }

      /**
       * Closes the {@link ResultScanner}, {@link Table}, and {@link Connection}.
       */
      @Override
      public void close() throws IOException {
        scanner.close();
        table.close();
        connection.close();
      }

      @Override
      public Result getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      public BoundedSource<Result> getCurrentSource() {
        return source;
      }

      /**
       * Creates a {@link Connection}, {@link Table} and {@link ResultScanner}.
        */
      @Override
      public boolean start() throws IOException {
        connection = new BigtableConnection(config.toHBaseConfig());
        table = connection.getTable(TableName.valueOf(config.getTableId()));
        scanner = table.getScanner(scan);
        return advance();
      }
    }

    Source(CloudBigtableScanConfiguration configuration) {
      this.configuration = configuration;
    }

    /**
     * Creates a coder for HBase {@link Result} objects. See {@link HBaseResultCoder} for more info.
     * @return A coder for {@link Result} objects.
     */
    @Override
    public Coder<Result> getDefaultOutputCoder() {
      return new HBaseResultCoder();
    }

    // TODO: Add a method on the server side that will be a more precise split based on server-side
    // statistics
    /**
     * <p>
     * Splits the table based on keys that belong to tablets, known as "regions" in the HBase API.
     * The current implementation uses the HBase {@link RegionLocator} interface, which calls
     * {@link BigtableService#sampleRowKeys(com.google.bigtable.v1.SampleRowKeysRequest, io.grpc.stub.StreamObserver)
     * under the covers. A {@link SourceWithKeys} may correspond to a single region or a portion of
     * a region.
     * </p>
     * <p>
     * If a split is smaller than a single region, the split is calculated based on the assumption
     * that the data is distributed evenly between the region's startKey and stopKey. That
     * assumption may not be correct for any specific start/stop key combination.
     * </p>
     * <p>
     * This method is called internally by Cloud Dataflow. Do not call it directly.
     * </p>
     * @param desiredBundleSizeBytes The desired size for each bundle, in bytes.
     * @param options The pipeline options.
     * @return A list of sources split into groups.
     */
    @Override
    public List<SourceWithKeys> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      Scan scan = configuration.getScan();
      byte[] scanStartKey = scan.getStartRow();

      byte[] scanEndKey = scan.getStopRow();
      List<SourceWithKeys> splits = new ArrayList<>();
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
        splits.add(new SourceWithKeys(startKey, endKey, 0));
      }
      LOG.info("Creating {} splits.", splits.size());
      LOG.debug("Created splits {}.", splits);
      return splits;
    }

    /**
     * Is the range of the region within the range of the scan?
     */
    private static boolean isWithinRange(byte[] scanStartKey, byte[] scanEndKey,
        byte[] startKey, byte[] endKey) {
      return (scanStartKey.length == 0 || endKey.length == 0
              || Bytes.compareTo(scanStartKey, endKey) < 0)
          && (scanEndKey.length == 0 || Bytes.compareTo(scanEndKey, startKey) > 0);
    }

    /**
     * Perform a call to the {@link BigtableDataClient#sampleRowKeys(SampleRowKeysRequest)} that
     * gives information about tablet key boundaries and estimated sizes.
     */
    synchronized List<SampleRowKeysResponse> getSampleRowKeys() throws IOException {
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
     * Gets an estimated size based on data returned from {@link Exception#getSampleRowKeys}. The
     * estimate will be high if a Scan is set on the {@link CloudBigtableScanConfiguration}; in such
     * cases, the estimate will not take the Scan into account, and will return a larger estimate
     * than what the {@link Reader} will actually read.
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
      LOG.info("Estimated size in bytes: " + totalEstimatedSizeBytes);

      return totalEstimatedSizeBytes;
    }

    /**
     * Checks whether the pipeline produces sorted keys. NOTE: HBase supports reverse scans, but
     * Cloud Bigtable does not.
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
    public BoundedSource.BoundedReader<Result> createReader(PipelineOptions options)
        throws IOException {
      return new Source.Reader(this, configuration, configuration.getScan());
    }

    /**
     * Splits the region based on the start and stop key. Uses
     * {@link Bytes#split(byte[], byte[], int)} under the covers.
     */
    private List<SourceWithKeys> split(long regionSize, long desiredBundleSizeBytes,
        byte[] startKey, byte[] stopKey) {
      if (regionSize < desiredBundleSizeBytes || stopKey.length == 0) {
        return Collections.singletonList(new SourceWithKeys(startKey, stopKey, regionSize));
      } else {
        Preconditions.checkState(desiredBundleSizeBytes > 0);
        if (stopKey.length > 0) {
          Preconditions.checkState(Bytes.compareTo(startKey, stopKey) <= 0, "[startKey, stopKey]",
            new String(startKey), new String(stopKey));
          Preconditions.checkState(regionSize > 0);
        }
        int splitCount = (int) Math.ceil((double) (regionSize) / (double) (desiredBundleSizeBytes));
        byte[][] splitKeys = Bytes.split(startKey, stopKey, splitCount - 1);
        Preconditions.checkState(splitCount + 1 == splitKeys.length);
        List<SourceWithKeys> result = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
          result.add(new SourceWithKeys(splitKeys[i], splitKeys[i + 1], regionSize));
        }
        return result;
      }
    }
  }

  ///////////////////// Write Class /////////////////////////////////

  /**
   * Initialize the coders for the Cloud Bigtable Write {@link PTransform}. Sets up {@link Coder}s
   * required to serialize HBase {@link Put}, {@link Delete}, and {@link Mutation} objects.  See
   * {@link HBaseMutationCoder} for additional implementation details.
   *
   * @return The original {@link Pipeline} for chaining convenience.
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

  static abstract class AbstractCloudBigtableTableWriteFn<In, Out> extends DoFn<In, Out> {
    private static final long serialVersionUID = 1L;

    private static final CloudBigtableConnectionPool pool = new CloudBigtableConnectionPool();

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected CloudBigtableConfiguration config;
    protected transient volatile CloudBigtableConnectionPool.PoolEntry connectionEntry;

    public AbstractCloudBigtableTableWriteFn(CloudBigtableConfiguration config) {
      this.config = config;
    }

    protected synchronized Connection getConnection() throws IOException {
      if (connectionEntry == null) {
        connectionEntry = pool.getConnection(config.toHBaseConfig());
      }
      return connectionEntry.getConnection();
    }

    @Override
    public synchronized void finishBundle(DoFn<In, Out>.Context c) throws Exception {
      LOG.debug("Closing the Bigtable connection.");
      if (connectionEntry != null) {
        pool.returnConnection(connectionEntry);
        connectionEntry = null;
      }
    }

    /**
     * Logs the {@link Context} and the exception's
     * {@link RetriesExhaustedWithDetailsException#getExhaustiveDescription()}.
     */
    protected void logExceptions(Context context, RetriesExhaustedWithDetailsException exception) {
      LOG.warn("For context {}: exception occured during bulk writing: {}", context,
        exception.getExhaustiveDescription());
    }

    protected static void retrowException(RetriesExhaustedWithDetailsException exception)
        throws Exception {
      if (exception.getCauses().size() == 1) {
        throw (Exception) exception.getCause(0);
      } else {
        throw exception;
      }
    }
  }

  /**
   * <p>
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of
   * {@link Mutation}s to a table specified via a {@link CloudBigtableTableConfiguration}.
   * </p>
   * <p>
   * NOTE: This will write Puts and Deletes, not Appends and Increments. The limitation exists
   * because the batch can fail half way, and Appends/Increments might be re-run causing the
   * Mutation to be executed twice, which is never the user's intent. Re-running a Delete will not
   * cause any differences. Re-running a Put isn't normally a problem, but might cause problems in
   * some cases when the number of versions in the ColumnFamily is greater than one. In a case where
   * multiple versions could be a problem, it's best to add a timestamp on the Put.
   * </p>
   */
  public static class CloudBigtableSingleTableWriteFn extends
      AbstractCloudBigtableTableWriteFn<Mutation, Void> {
    private static final long serialVersionUID = 2L;
    private transient BufferedMutator mutator;
    private final String tableName;

    public CloudBigtableSingleTableWriteFn(CloudBigtableTableConfiguration config) {
      super(config);
      this.tableName = config.getTableId();
    }

    private synchronized BufferedMutator getBufferedMutator(Context context)
        throws IOException {
      if (mutator == null) {
        ExceptionListener listener = createExceptionListener(context);
        BufferedMutatorParams params =
            new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(
              AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT).listener(listener);
        LOG.debug("Creating the Bigtable bufferedMutator.");
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("Persisting {}", Bytes.toString(mutation.getRow()));
      }
      getBufferedMutator(context).mutate(mutation);
    }

    /**
     * Closes the {@link BufferedMutator} and {@link Connection}.
     */
    @Override
    public void finishBundle(Context context) throws Exception {
      try {
        LOG.debug("Closing the BufferedMutator.");
        if (mutator != null) {
          mutator.close();
        }
      } catch (RetriesExhaustedWithDetailsException exception) {
        logExceptions(context, exception);
        retrowException(exception);
      } finally {
        // Close the connection to clean up resources.
        super.finishBundle(context);
      }
    }
  }

  /**
   * <p>
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of {@link KV}
   * of (String tableName, List of {@link Mutation}s) to the specified table.
   * </p>
   * <p>
   * NOTE: This {@link DoFn} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   * </p>
   */
  public static class CloudBigtableMultiTableWriteFn extends
  AbstractCloudBigtableTableWriteFn<KV<String, Iterable<Mutation>>, Void> {
    private static final long serialVersionUID = 2L;

    public CloudBigtableMultiTableWriteFn(CloudBigtableConfiguration config) {
      super(config);
    }

    /**
     * Use the connection to create a new {@link Table} to write the {@link Mutation}s to. NOTE:
     * This method does not create a new table in Cloud Bigtable. The table must already exist.
     * @param context The context for the {@link DoFn}.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      KV<String, Iterable<Mutation>> element = context.element();
      String tableName = element.getKey();
      try (Table t = getConnection().getTable(TableName.valueOf(tableName))) {
        List<Mutation> mutations = Lists.newArrayList(element.getValue());
        int mutationCount = mutations.size();
        LOG.trace("Persisting {} elements to table {}.", mutationCount, tableName);
        t.batch(mutations, new Object[mutationCount]);
        LOG.trace("Finished persisting {} elements to table {}.", mutationCount, tableName);
      } catch (RetriesExhaustedWithDetailsException exception) {
        logExceptions(context, exception);
        retrowException(exception);
      }
    }
  }

  /**
   * A {@link PTransform} wrapper around a {@link DoFn} that will write {@link Mutation}s to Cloud
   * Bigtable.
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
   * <p>
   * Creates a {@link PTransform} that can write either a bounded or unbounded {@link PCollection}
   * of {@link Mutation}s to a table specified via a {@link CloudBigtableTableConfiguration}.
   * </p>
   * <p>
   * NOTE: This {@link PTransform} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   * </p>
   */
  public static PTransform<PCollection<Mutation>, PDone> writeToTable(
      CloudBigtableTableConfiguration config) {
    validate(config, config.getTableId());
    return new CloudBigtableWriteTransform<>(new CloudBigtableSingleTableWriteFn(config));
  }

   /**
   * <p>
   * Creates a {@link PTransform} that can write either a bounded or unbounded {@link PCollection}
   * of {@link KV} of (String tableName, List of {@link Mutation}s) to the specified table.
   * </p>
   * <p>
   * NOTE: This {@link PTransform} will write {@link Put}s and {@link Delete}s, not {@link
   * org.apache.hadoop.hbase.client.Append}s and {@link org.apache.hadoop.hbase.client.Increment}s.
   * This limitation exists because if the batch fails partway through, Appends/Increments might be
   * re-run, causing the {@link Mutation} to be executed twice, which is never the user's intent.
   * Re-running a Delete will not cause any differences.  Re-running a Put isn't normally a problem,
   * but might cause problems in some cases when the number of versions supported by the column
   * family is greater than one.  In a case where multiple versions could be a problem, it's best to
   * add a timestamp to the {@link Put}.
   * </p>
   */
   public static PTransform<PCollection<KV<String, Iterable<Mutation>>>, PDone>
      writeToMultipleTables(CloudBigtableConfiguration config) {
    validate(config, null);
    return new CloudBigtableWriteTransform<>(new CloudBigtableMultiTableWriteFn(config));
  }

  /**
   * Creates a {@link BoundedSource} for a Cloud Bigtable {@link Table}, which is potentially
   * filtered by a {@link Scan}.
   */
  public static com.google.cloud.dataflow.sdk.io.BoundedSource<Result> read(
      CloudBigtableScanConfiguration config) {
    return new Source(config);
  }

  private static void checkNotNullOrEmpty(String value, String type) {
    checkArgument(
        !isNullOrEmpty(value), "A " + type + " must be set to configure Bigtable properly.");
  }

  private static void validate(CloudBigtableConfiguration configuration, String tableId) {
    checkNotNullOrEmpty(configuration.getProjectId(), "projectId");
    checkNotNullOrEmpty(configuration.getZoneId(), "zoneId");
    checkNotNullOrEmpty(configuration.getClusterId(), "clusterId");
    if (tableId != null) {
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
  }
}
