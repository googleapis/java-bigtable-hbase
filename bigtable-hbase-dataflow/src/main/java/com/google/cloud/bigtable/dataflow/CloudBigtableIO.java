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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
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
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.BigtableServiceGrpc.BigtableService;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
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
 * To use {@link CloudBigtableIO}, users must use gcloud to get credential for Bigtable:
 *
 * <pre>
 * $ gcloud auth login
 * </pre>
 * <p>
 * To read a {@link PCollection} from a Table, with an optional Scan, use
 * {@link CloudBigtableIO#read(CloudBigtableScanConfiguration)}. For example:
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
 * To write a {@link PCollection} to a Bigtable, use
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

  /**
   * A {@link BoundedSource} for a Bigtable {@link Table}, which is potentially filtered by a
   * {@link Scan}.
   */
  static class Source extends BoundedSource<Result> {
    private static final long serialVersionUID = -5580115943635114126L;

    /**
     * Configuration for a Cloud Bigtable connection, a table and an optional scan.
     */
    private final CloudBigtableScanConfiguration configuration;

    /**
     * A {@link BoundedSource} for a Bigtable {@link Table} with a start/stop key range, along with
     * a potential filter via a {@link Scan}.
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
       * An estimate of the size of the source. NOTE: This value is a guestimate. It could be be
       * significantly off, especially if there is a Scan selected in the configuration. It will
       * also be off if the start and stop keys are calculated via
       * {@link Source#splitIntoBundles(long, PipelineOptions)}.
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
      }

      /**
       * An estimate of the size of the source. NOTE: This value is a guestimate. It could be be
       * significantly off, especially if there is a Scan selected in the configuration. It will
       * also be off if the start and stop keys are calculated via
       * {@link Source#splitIntoBundles(long, PipelineOptions)}.
       */
      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return estimatedSize;
      }

      /**
       *NOTE: HBase supports reverse scans, but Cloud Bigtable does not.
       */
      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return true;
      }

      /**
       * Encodes Results - see {@link HBaseResultCoder} for more details.
       */
      @Override
      public Coder<Result> getDefaultOutputCoder() {
        return new HBaseResultCoder();
      }

      /**
       * <p>Splits the bundle based on the assumption that the data is distributed evenly between
       * {@link #startKey} and {@link #stopKey}.  That assumption may not likely to be correct for
       * any specific start/stop key combination.</p>
       *
       * <p>TODO: Add a method on the server side that will be a more precise split based on server
       * side statistics</p>
       */
      @Override
      public List<SourceWithKeys> splitIntoBundles(long desiredBundleSizeBytes,
          PipelineOptions options) throws Exception {
        return split(estimatedSize, desiredBundleSizeBytes, startKey, stopKey);
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

      public byte[] getStartKey() {
        return startKey;
      }

      public byte[] getStopKey() {
        return stopKey;
      }

      public long getEstimatedSize() {
        return estimatedSize;
      }
    }

    /**
     * Reads rows for a specific {@link Table}, usually filtered by a Scan.
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
     * Creates a Coder for HBase {@link Result} objects. See {@link HBaseResultCoder} for more info.
     */
    @Override
    public Coder<Result> getDefaultOutputCoder() {
      return new HBaseResultCoder();
    }

    /**
     * <p>
     * Splits the table based on keys that belong to "regions" a.k.a. tablets via the HBase
     * {@link RegionLocator} interface which calls
     * {@link BigtableService#sampleRowKeys(com.google.bigtable.v1.SampleRowKeysRequest, io.grpc.stub.StreamObserver)
     * under the covers}. A {@link SourceWithKeys} may correspond to a single region or a portion of
     * a region.
     * </p>
     * <p>
     * Splits that are smaller than a single region calculate the split based on the assumption that
     * the data is distributed evenly between the region's startKey and stopKey. That
     * assumption may not likely to be correct for any specific start/stop key combination.
     * </p>
     * <p>
     * TODO: Add a method on the server side that will be a more precise split based on server side
     * statistics
     * </p>
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
    private List<SampleRowKeysResponse> getSampleRowKeys() throws IOException {
      BigtableOptions bigtableOptions = configuration.toBigtableOptions();
      try (BigtableSession session = new BigtableSession(bigtableOptions)) {
        BigtableTableName tableName =
            bigtableOptions.getClusterName().toTableName(configuration.getTableId());
        SampleRowKeysRequest request =
            SampleRowKeysRequest.newBuilder().setTableName(tableName.toString()).build();
        return session.getDataClient().sampleRowKeys(request);
      }
    }

    /**
     * Validates the existence of the table in the configuration.
     */
    @Override
    public void validate() {
      CloudBigtableIO.validate(configuration, configuration.getTableId());
    }

    /**
     * Gets an estimated size based on data returned from {@link Exception#getSampleRowKeys}. If The
     * estimate will be high if a Scan is set on the {@link CloudBigtableScanConfiguration}; in such
     * cases, the estimate will not take the Scan into account, and will return a larger estimate
     * than what the {@link Reader} will actually read.
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
      return totalEstimatedSizeBytes;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return true;
    }

    /**
     * Creates a reader that will scan the entire table based on the scan in the configuration.
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
   * Initialize the coders for the Cloud Bigtable Write PTransform. Sets up {@link Coders}
   * required to serialize HBase {@link Put}, {@link Delete}, and {@link Mutation} objects.  See
   * {@link HBaseMutationCoder} for additional implementation details.
   *
   * @return the original Pipeline - done for chaining convenience.
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
  private static class CloudBigtableSingleTableWriteFn extends DoFn<Mutation, Void> {
    private static final long serialVersionUID = 2L;
    private final CloudBigtableTableConfiguration config;
    private transient Connection conn;
    private transient BufferedMutator mutator;

    private CloudBigtableSingleTableWriteFn(CloudBigtableTableConfiguration config) {
      this.config = config;
    }

    /**
     * Creates a {@link Connection} and a {@link BufferedMutator}.
     */
    @Override
    public void startBundle(Context context) throws Exception {
      conn = new BigtableConnection(config.toHBaseConfig());
      mutator = conn.getBufferedMutator(TableName.valueOf(config.getTableId()));
    }

    /**
     * Performs an asyc mutation via {@link BufferedMutator#mutate(Mutation))}.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      mutator.mutate(context.element());
    }

    /**
     * Closes the {@link BufferedMutator} and {@link Connection}.
     */
    @Override
    public void finishBundle(Context context) throws Exception {
      try {
        mutator.close();
      } catch (RetriesExhaustedWithDetailsException e) {
        List<Throwable> causes = e.getCauses();
        if (causes.size() == 1) {
          throw (Exception) causes.get(0);
        } else {
          throw e;
        }
      }
      conn.close();
    }
  }

  /**
   * <p>
   * A {@link DoFn} that can write either a bounded or unbounded {@link PCollection} of {@link KV}
   * of (String tableName, List of{@link Mutation}s) to the specified table.
   * </p>
   * <p>NOTE: This will write Puts and Deletes, not Appends and Increments.  The limitation exists
   * because the batch can fail half way, and Appends/Increments might be re-run causing the
   * Mutation to be executed twice, which is never the user's intent.  Re-running a Delete will not
   * cause any differences.  Re-running a Put isn't normally a problem, but might cause problems
   * in some cases when the number of versions in the ColumnFamily is greater than one.  In a case
   * where multiple versions could be a problem, it's  best to add a timestamp on the Put.</p>
   */
  static class CloudBigtableMultiTableWriteFn extends
      DoFn<KV<String, Iterable<Mutation>>, Void> {
    private static final long serialVersionUID = 2L;
    private final CloudBigtableConfiguration config;
    private transient Connection conn;

    CloudBigtableMultiTableWriteFn(CloudBigtableConfiguration config) {
      this.config = config;
    }

    /**
     * Create a {@link Connection} from the provided {@link CloudBigtableConfiguration} that will
     * live for the lifetime of the DoFn.
     */
    @Override
    public void startBundle(Context context) throws Exception {
      conn = new BigtableConnection(config.toHBaseConfig());
    }

    /**
     * Use the connection to create a new {@link Table} to write the {@link Mutation}s to.
     */
    @Override
    public void processElement(ProcessContext context) throws Exception {
      KV<String, Iterable<Mutation>> element = context.element();
      try (Table t = conn.getTable(TableName.valueOf(element.getKey()))) {
        List<Mutation> mutations = Lists.newArrayList(element.getValue());
        Object results[] = new Object[mutations.size()];
        t.batch(mutations, results);
      }
    }

    /**
     * Close the {@link Connection}.
     */
    @Override
    public void finishBundle(Context context) throws Exception {
      conn.close();
    }
  }

  /**
   * <p> A {@link PTransform} wrapper around a {@link DoFn} that will write {@link Mutation}s to
   * Cloud Bigtable</p>
   */
  static class CloudBigtableWriteTransform<T> extends PTransform<PCollection<T>, PDone> {
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
   * NOTE: This will write Puts and Deletes, not Appends and Increments. The limitation exists
   * because the batch can fail half way, and Appends/Increments might be re-run causing the
   * Mutation to be executed twice, which is never the user's intent. Re-running a Delete will not
   * cause any differences. Re-running a Put isn't normally a problem, but might cause problems in
   * some cases when the number of versions in the ColumnFamily is greater than one. In a case where
   * multiple versions could be a problem, it's best to add a timestamp on the Put.
   * </p>
   */  public static PTransform<PCollection<Mutation>, PDone> writeToTable(
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
   * NOTE: This will write Puts and Deletes, not Appends and Increments. The limitation exists
   * because the batch can fail half way, and Appends/Increments might be re-run causing the
   * Mutation to be executed twice, which is never the user's intent. Re-running a Delete will not
   * cause any differences. Re-running a Put isn't normally a problem, but might cause problems in
   * some cases when the number of versions in the ColumnFamily is greater than one. In a case where
   * multiple versions could be a problem, it's best to add a timestamp on the Put.
   * </p>
   */
   public static PTransform<PCollection<KV<String, Iterable<Mutation>>>, PDone>
      writeToMultipleTables(CloudBigtableConfiguration config) {
    validate(config, null);
    return new CloudBigtableWriteTransform<>(new CloudBigtableMultiTableWriteFn(config));
  }

  /**
   * Creates a {@link BoundedSource} for a Bigtable {@link Table}, which is potentially filtered by a
   * {@link Scan}.
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
    }
    try (BigtableConnection conn = new BigtableConnection(configuration.toHBaseConfig());
        Admin admin = conn.getAdmin()) {
      if (tableId != null) {
        Preconditions.checkState(admin.tableExists(TableName.valueOf(tableId)));
      } else {
        admin.listTableNames();
      }
    } catch (IOException e) {
      new Logger(CloudBigtableIO.class).warn("Could not validate that the table exists: %s (%s)",
        e.getClass().getName(), e.getMessage());
    }
  }
}