/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.hbase.replication;

import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.BATCH_SIZE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_CBT_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_CBT_QUALIFIER_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_HBASE_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.CBT_REPL_HBASE_QUALIFIER_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_BATCH_SIZE_IN_BYTES;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_DRY_RUN_MODE;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_THREAD_COUNT;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.ENABLE_DRY_RUN_MODE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.INSTANCE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.NUM_REPLICATION_SINK_THREADS_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.PROJECT_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.ONE_WAY_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY;
import static com.google.cloud.bigtable.hbase.replication.metrics.HBaseToCloudBigtableReplicationMetrics.ONE_WAY_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY;
import static java.util.stream.Collectors.groupingBy;

import com.google.bigtable.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapterFactory;
import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudBigtableReplicator {

  private static final Logger LOG = LoggerFactory.getLogger(CloudBigtableReplicator.class);

  /**
   * Shared resources for all CloudBigtableReplicator objects. Everything here is shared with all
   * the objects of CloudBigtableReplicator and should be managed by this class (using reference
   * counting).
   */
  @VisibleForTesting
  static class SharedResources {

    // The singleton Object for this class. This object is lazily created when first
    // CloudBigtableReplicator is created and deleted when there is no CloudBigtableReplicator
    // object referencing it.
    private static SharedResources INSTANCE;

    private final ExecutorService executorService;
    /**
     * Bigtable connection owned by this class and shared by all the instances of {@link
     * CloudBigtableReplicator} class. DO NOT CLOSE THIS CONNECTION from any instance. The state of
     * this connection is maintained by this class via reference counting.
     *
     * <p>Creating a Bigtable connection is expensive as it creates num_cpu * 4 gRpc connections.
     * Hence, it is efficient and recommended re-using the connection. Ref counting is required to
     * properly close the connection.
     */
    private final Connection connection;

    /** Reference count for this instance. */
    private static int numReferences = 0;

    @VisibleForTesting
    SharedResources(Connection connection, ExecutorService executorService) {
      this.connection = connection;
      this.executorService = executorService;
    }

    static synchronized SharedResources getInstance(Configuration conf) {

      numReferences++;
      if (INSTANCE == null) {
        /* Create a bounded cached thread pool with unbounded queue. At any point, we will only have 1
         * replicate() method active per replicationEndpoint object, so we will never have too many
         * tasks in the queue. Having static thread pool bounds the parallelism of CBT replication
         * library.
         */
        int numThreads = conf.getInt(NUM_REPLICATION_SINK_THREADS_KEY, DEFAULT_THREAD_COUNT);
        ExecutorService executorService =
            Executors.newFixedThreadPool(
                numThreads,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("cloud-bigtable-replication-sink-%d")
                    .build());

        // Create a connection to Cloud Bigtable.
        Configuration configurationCopy = new Configuration(conf);

        String projectId = configurationCopy.get(PROJECT_KEY);
        String instanceId = configurationCopy.get(INSTANCE_KEY);

        // Set user agent tag depending on if bidirectional replication was enabled.
        if (configurationCopy.getBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, false)) {
          configurationCopy.set(
              BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseReplicationBidirectional");
        } else {
          configurationCopy.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseReplication");
        }

        // If an App profile is provided, it will be picked automatically by the connection.
        Connection connection = BigtableConfiguration.connect(configurationCopy);
        LOG.info(
            String.format(
                "Created a connection to CBT. projects/%s/instances/%s", projectId, instanceId));

        INSTANCE = new SharedResources(connection, executorService);
      }
      return INSTANCE;
    }

    private static synchronized void decrementReferenceCount() {
      if (--numReferences > 0) {
        return;
      }

      // Clean up resources as no object is referencing the SharedResources instance
      try {
        try {
          // Connection is shared by all the instances of CloudBigtableReplicator, close it only
          // if no one is using it. Closing the connection is required as it owns the underlying
          // gRpc connections and gRpc does not like JVM shutting without closing the gRpc
          // connections.
          INSTANCE.connection.close();
        } catch (Exception e) {
          LOG.warn("Failed to close connection to Cloud Bigtable", e);
        }
        INSTANCE.executorService.shutdown();
        try {
          // Best effort wait for termination. When shutdown is called, there should be no
          // tasks waiting on the service, since all the tasks must finish for replicator to exit
          // replicate method.
          INSTANCE.executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.warn("Failed to shut down the Cloud Bigtable replication thread pool.", e);
        }
      } finally {
        INSTANCE = null;
      }
    }
  }

  private long batchSizeThresholdInBytes;

  private IncompatibleMutationAdapter incompatibleMutationAdapter;

  private boolean isDryRun;

  /** Bidirectional replication variables if the mode is enabled. */
  private boolean bidirectionalReplicationEnabled;

  private byte[] sourceHbaseQualifier;
  private byte[] sourceCbtQualifier;
  private MetricsExporter metricsExporter;

  /**
   * Shared resources that are not tied to an object of this class. Lifecycle of resources in this
   * object is usually tied to the lifecycle of JVM.
   */
  private SharedResources sharedResources;

  /** Common endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. */
  public CloudBigtableReplicator() {
    // TODO: Validate that loggers are correctly configured.
  }

  @VisibleForTesting
  synchronized void start(
      SharedResources sharedResources,
      IncompatibleMutationAdapter incompatibleMutationAdapter,
      long batchSizeThresholdInBytes,
      boolean isDryRun,
      Configuration configuration,
      MetricsExporter metricsExporter) {
    this.sharedResources = sharedResources;
    this.incompatibleMutationAdapter = incompatibleMutationAdapter;
    this.batchSizeThresholdInBytes = batchSizeThresholdInBytes;
    this.isDryRun = isDryRun;

    this.metricsExporter = metricsExporter;
    if (metricsExporter == null) {
      LOG.trace("Error, metrics exporter is not configured. No metrics logging.");
    }

    // Get bidirectional replication configs if configuration enabled it.
    if (configuration != null) {
      this.bidirectionalReplicationEnabled =
          configuration.getBoolean(
              CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE);
      // If enabled, gets source qualifiers from configs.
      if (this.bidirectionalReplicationEnabled) {
        this.sourceHbaseQualifier =
            configuration.get(CBT_REPL_HBASE_QUALIFIER_KEY, CBT_REPL_HBASE_QUALIFIER).getBytes();
        this.sourceCbtQualifier =
            configuration.get(CBT_REPL_CBT_QUALIFIER_KEY, CBT_REPL_CBT_QUALIFIER).getBytes();

        LOG.info(
            String.format(
                "Bidirectional direction enabled.\nCbt qualifier: %s\nHbase qualifier: %s",
                this.bidirectionalReplicationEnabled,
                Bytes.toStringBinary(sourceCbtQualifier),
                Bytes.toStringBinary(sourceHbaseQualifier)));
      }
    }

    if (isDryRun) {
      LOG.info(
          "Replicating to Cloud Bigtable in dry-run mode. No mutations will be applied to Cloud Bigtable.");
    }
  }

  public synchronized void start(Configuration configuration, MetricsExporter metricsExporter) {
    LOG.info("Starting replication to CBT.");

    SharedResources sharedResources = SharedResources.getInstance(configuration);
    IncompatibleMutationAdapter incompatibleMutationAdapter =
        new IncompatibleMutationAdapterFactory(
                configuration, metricsExporter, sharedResources.connection)
            .createIncompatibleMutationAdapter();

    start(
        sharedResources,
        incompatibleMutationAdapter,
        configuration.getLong(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE_IN_BYTES),
        configuration.getBoolean(ENABLE_DRY_RUN_MODE_KEY, DEFAULT_DRY_RUN_MODE),
        configuration,
        metricsExporter);
  }

  public void stop() {
    LOG.info("Stopping replication to CBT.");
    SharedResources.decrementReferenceCount();
  }

  public UUID getPeerUUID() {
    // UUID is used to de-duplicate mutations and avoid the replication loop. In a fully connected
    // replication topology, all the HBase clusters should recognize the CBT with same UUID.
    return UUID.nameUUIDFromBytes("Cloud-bigtable".getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Replicates the WAL entries to Cloud Bigtable via HBase client. Returns true if everything is
   * successfully replicated; returns false if anything fails. On failure, HBase should retry the
   * whole batch again.
   *
   * <p>Does not throw any exceptions to the caller. In case of any errors, should return false.
   *
   * @param walEntriesByTable Map of WALEntries keyed by table name.
   */
  public boolean replicate(Map<String, List<BigtableWALEntry>> walEntriesByTable) {
    long startTime = System.currentTimeMillis();
    boolean succeeded = true;

    List<Future<Boolean>> futures = new ArrayList<>();

    for (Map.Entry<String, List<BigtableWALEntry>> walEntriesForTable :
        walEntriesByTable.entrySet()) {
      futures.addAll(replicateTable(walEntriesForTable.getKey(), walEntriesForTable.getValue()));
    }

    // Check on the result  for all the batches.
    try {
      for (Future<Boolean> future : futures) {
        // replicate method should succeed only when all the entries are successfully replicated.
        // TODO Add to readme about setting some timeouts on CBT writes. MutateRow should not
        //  wait forever, as long as rpc completes, this future will make progress.
        succeeded = future.get() && succeeded;
      }
    } catch (Exception e) {
      LOG.error("Failed to replicate a batch ", e);
      // Suppress the exception here and return false to the replication machinery.
      succeeded = false;
    } finally {
      LOG.trace(
          "Exiting CBT replicate method after {} ms, Succeeded: {} ",
          (System.currentTimeMillis() - startTime),
          succeeded);
    }

    return succeeded;
  }

  private List<Future<Boolean>> replicateTable(
      String tableName, List<BigtableWALEntry> walEntries) {
    List<Future<Boolean>> futures = new ArrayList<>();
    List<Cell> cellsToReplicateForTable = new ArrayList<>();
    int batchSizeInBytes = 0;

    for (BigtableWALEntry walEntry : walEntries) {
      // To prevent loops in bidirectional replication,
      // skip entry if it entry is not eligible for replication.
      if (!isEligibleForReplication(walEntry)) {
        continue;
      }

      // Translate the incompatible mutations.
      List<Cell> compatibleCells = incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry);

      cellsToReplicateForTable.addAll(compatibleCells);
    }

    if (isDryRun) {
      // Always return true in dry-run mode. The incompatibleMutationAdapter has updated all the
      // metrics needed to find the incompatibilities.
      return Arrays.asList(CompletableFuture.completedFuture(true));
    }

    // group the data by the row key before sending it on multiple threads. It is very important
    // to group by rowKey here. This grouping guarantees that mutations from same row are not sent
    // concurrently in different batches. If same row  mutations are sent concurrently, they may
    // be applied out of order. Out of order applies cause divergence between 2 databases and
    // must be avoided as much as possible.
    // ByteRange is required to generate proper hashcode for byte[]
    Map<ByteRange, List<Cell>> cellsToReplicateByRow =
        cellsToReplicateForTable.stream()
            .collect(
                groupingBy(
                    k -> new SimpleByteRange(k.getRowArray(), k.getRowOffset(), k.getRowLength())));

    // Now we have cells to replicate by rows, this list can be big and processing it on a single
    // thread is not efficient. As this thread will have to do proto translation and may need to
    // serialize the proto. So create micro batches at row boundaries (never split a row between
    // threads) and send them on separate threads.
    Map<ByteRange, List<Cell>> batchToReplicate = new HashMap<>();
    int numCellsInBatch = 0;
    for (Map.Entry<ByteRange, List<Cell>> rowCells : cellsToReplicateByRow.entrySet()) {

      // If bidirectional replication is enabled, tag row cells with source info
      // so Bigtable replicator skips this batch and does not replicate batch back to HBase.
      if (bidirectionalReplicationEnabled) {
        appendSourceTagToCells(rowCells);
      }

      // TODO handle the case where a single row has >100K mutations (very rare, but should not
      //  fail)
      numCellsInBatch += rowCells.getValue().size();
      batchSizeInBytes += getRowSize(rowCells);
      batchToReplicate.put(rowCells.getKey(), rowCells.getValue());

      // TODO add tests for batch split on size and cell counts
      if (batchSizeInBytes >= batchSizeThresholdInBytes || numCellsInBatch >= 100_000 - 1) {
        LOG.trace(
            "Replicating a batch of "
                + batchToReplicate.size()
                + " rows and "
                + numCellsInBatch
                + " cells with heap size "
                + batchSizeInBytes
                + " for table: "
                + tableName);

        futures.add(replicateBatch(tableName, batchToReplicate));
        batchToReplicate = new HashMap<>();
        numCellsInBatch = 0;
        batchSizeInBytes = 0;
      }
    }

    // Flush last batch
    if (!batchToReplicate.isEmpty()) {
      futures.add(replicateBatch(tableName, batchToReplicate));
    }
    return futures;
  }

  private int getRowSize(Map.Entry<ByteRange, List<Cell>> rowCells) {
    int rowSizeInBytes = 0;
    for (Cell cell : rowCells.getValue()) {
      rowSizeInBytes += CellUtil.estimatedHeapSizeOf(cell);
    }
    return rowSizeInBytes;
  }

  private Future<Boolean> replicateBatch(
      String tableName, Map<ByteRange, List<Cell>> batchToReplicate) {
    try {
      CloudBigtableReplicationTask replicationTask =
          new CloudBigtableReplicationTask(tableName, sharedResources.connection, batchToReplicate);
      return sharedResources.executorService.submit(replicationTask);
    } catch (Exception ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOG.error("Failed to submit a batch for table: " + tableName, ex);
      return CompletableFuture.completedFuture(false);
    }
  }

  /**
   * Checks if WAL entry is eligible for replication. If the following are both true, then entry is
   * NOT eligible for replication: 1. Bidirectional replication is enabled, AND 2. Last mutation in
   * WAL entry is a special mutation matching SOURCE_CBT qualifier
   *
   * @param walEntry from HBase replication WAL log.
   * @return true if walEntry is from Bigtable.
   */
  private boolean isEligibleForReplication(BigtableWALEntry walEntry) {
    // Always return eligible if bidirectional replication was not enabled
    int mutationCount = walEntry.getCells().size();
    if (!bidirectionalReplicationEnabled) {
      incrementMetric(ONE_WAY_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
      incrementMetric(ONE_WAY_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, mutationCount);
      return true;
    }

    Cell lastCell = walEntry.getCells().get(walEntry.getCells().size() - 1);

    if (Arrays.equals(CellUtil.cloneQualifier(lastCell), sourceCbtQualifier)) {
      // Wrap in statement so row is not needlessly converted.
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Dropping WAL entry as it came from CBT , Row key: "
                + Bytes.toStringBinary(
                    lastCell.getRowArray(), lastCell.getRowOffset(), lastCell.getRowLength()));
      }
      incrementMetric(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
      // We decrement one because the last mutation would've been the special mutation from CBT.
      incrementMetric(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, mutationCount - 1);
      return false;
    } else {
      // Wrap in statement so row is not needlessly converted.
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Replicating WAL entry as it originated on HBase , Row key: "
                + Bytes.toStringBinary(
                    lastCell.getRowArray(), lastCell.getRowOffset(), lastCell.getRowLength()));
      }
      incrementMetric(BIDIRECTIONAL_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
      incrementMetric(BIDIRECTIONAL_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, mutationCount);
      return true;
    }
  }

  private void incrementMetric(String metricName, int delta) {
    if (metricsExporter == null) {
      return;
    }
    metricsExporter.incCounters(metricName, delta);
  }

  /**
   * Tag source info to row cells by appending cells with a special delete mutation with
   * SOURCE_HBASE qualifier.
   *
   * <p>The delete-mutation will not be user-visible nor cause a change on Bigtable because it's
   * trying to delete a non-existent special qualifier column at timestamp 0. The mutation will be
   * picked up by Bigtable change data capture along with other mutations of the batch and allow the
   * Bigtable-HBase replicator to recognize that the mutation batch came from Hbase based on the
   * special qualifier column name.
   *
   * @param rowCells map of <RowKey, [Cell]> mutations.
   */
  private void appendSourceTagToCells(Map.Entry<ByteRange, List<Cell>> rowCells) {
    Cell lastCell = rowCells.getValue().get(rowCells.getValue().size() - 1);

    Cell sourceHbaseMarker =
        CellUtil.createCell(
            CellUtil.cloneRow(lastCell),
            CellUtil.cloneFamily(lastCell),
            sourceHbaseQualifier,
            0L,
            KeyValue.Type.Delete.getCode(),
            null);

    rowCells.getValue().add(sourceHbaseMarker);

    return;
  }
}
