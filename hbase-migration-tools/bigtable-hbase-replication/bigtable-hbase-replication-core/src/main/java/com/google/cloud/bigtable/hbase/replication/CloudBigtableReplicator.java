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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapterFactory;
import com.google.cloud.bigtable.hbase.replication.configuration.TableNameMapper;
import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
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

    /**
     * Reference count for this instance. Used to determine if shared resources should be cleaned
     * up.
     */
    private static int numReferences = 0;

    SharedResources(Connection connection, ExecutorService executorService) {
      this.connection = connection;
      this.executorService = executorService;
    }

    static synchronized SharedResources getInstance(Configuration conf) {

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
        // Note: HBase 2.x ReplicationPeerImpl wraps peer + cluster configuration in a
        // CompoundConfiguration whose internal `properties` field is null, so `new
        // Configuration(conf)`
        // drops all compound entries. Copy via the Configuration iterator to preserve both
        // hbase-site.xml and per-peer ReplicationPeerConfig properties.
        Configuration configurationCopy = new Configuration(conf);
        for (Map.Entry<String, String> entry : conf) {
          configurationCopy.set(entry.getKey(), entry.getValue());
        }

        String projectId = configurationCopy.get(PROJECT_KEY);
        String instanceId = configurationCopy.get(INSTANCE_KEY);

        // Set user agent tag depending on if bidirectional replication was enabled.
        if (configurationCopy.getBoolean(CBT_REPL_BIDIRECTIONAL_REPLICATION_MODE_KEY, false)) {
          configurationCopy.set(
              BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseReplicationBidirectional");
        } else {
          configurationCopy.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseReplication");
        }

        try {
          // If an App profile is provided, it will be picked automatically by the connection.
          Connection connection = BigtableConfiguration.connect(configurationCopy);
          LOG.info("Created a connection to CBT. projects/{}/instances/{}", projectId, instanceId);

          INSTANCE = new SharedResources(connection, executorService);
        } catch (RuntimeException e) {
          executorService.shutdownNow();
          throw e;
        }
      }
      numReferences++;
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

  /** Maps HBase table names to Cloud Bigtable table IDs. Identity unless configured. */
  private TableNameMapper tableNameMapper = TableNameMapper.identity();

  /**
   * Shared resources that are not tied to an object of this class. Lifecycle of resources in this
   * object is usually tied to the lifecycle of JVM.
   */
  private SharedResources sharedResources;

  /** Common endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. */
  public CloudBigtableReplicator() {
    // TODO: Validate that loggers are correctly configured.
  }

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
            configuration
                .get(CBT_REPL_HBASE_QUALIFIER_KEY, CBT_REPL_HBASE_QUALIFIER)
                .getBytes(StandardCharsets.UTF_8);
        this.sourceCbtQualifier =
            configuration
                .get(CBT_REPL_CBT_QUALIFIER_KEY, CBT_REPL_CBT_QUALIFIER)
                .getBytes(StandardCharsets.UTF_8);

        LOG.info(
            "Bidirectional direction enabled. \nCbt qualifier: {}\nHbase qualifier: {}",
            Bytes.toStringBinary(sourceCbtQualifier),
            Bytes.toStringBinary(sourceHbaseQualifier));
      }

      // Load the optional HBase -> Cloud Bigtable table name mapping. A broken mapping must stop
      // the endpoint from starting: replicating to the wrong table is worse than not replicating.
      try {
        this.tableNameMapper = TableNameMapper.fromConfiguration(configuration);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to load the HBase to Cloud Bigtable table name mapping.", e);
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
   * @param walEntriesByTable Map of WALEntries keyed by HBase table name.
   */
  public boolean replicate(Map<String, List<BigtableWALEntry>> walEntriesByTable) {
    long startTime = System.currentTimeMillis();
    boolean succeeded = true;

    if (walEntriesByTable == null || walEntriesByTable.isEmpty()) {
      return true;
    }

    try {
      // Coalesce WAL entries by mapped Cloud Bigtable table name. If multiple HBase source table
      // names map to the same target Bigtable table, their WAL entries must be merged into a single
      // list and replicated in one call to avoid concurrent out-of-order writes for the same rows.
      Map<String, List<BigtableWALEntry>> coalescedWalEntries = new LinkedHashMap<>();
      Map<String, String> targetToFirstCanonicalSource = new HashMap<>();
      for (Map.Entry<String, List<BigtableWALEntry>> walEntriesForTable :
          walEntriesByTable.entrySet()) {
        String hbaseTableName = walEntriesForTable.getKey();
        if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
          LOG.warn("Encountered null or blank HBase table name in WAL entries map. Skipping.");
          continue;
        }
        List<BigtableWALEntry> entries = walEntriesForTable.getValue();
        if (entries == null || entries.isEmpty()) {
          continue;
        }
        String bigtableTableName = tableNameMapper.map(hbaseTableName);
        if (bigtableTableName == null || bigtableTableName.trim().isEmpty()) {
          LOG.warn(
              "TableNameMapper mapped HBase table '{}' to null or blank. Skipping.",
              hbaseTableName);
          continue;
        }

        String canonicalHBaseTable;
        try {
          canonicalHBaseTable = TableName.valueOf(hbaseTableName.trim()).getNameAsString();
        } catch (IllegalArgumentException e) {
          canonicalHBaseTable = hbaseTableName.trim();
        }

        String existingSource =
            targetToFirstCanonicalSource.putIfAbsent(bigtableTableName, canonicalHBaseTable);
        if (existingSource != null && !existingSource.equals(canonicalHBaseTable)) {
          throw new IllegalStateException(
              "Multiple distinct HBase source tables ('"
                  + existingSource
                  + "' and '"
                  + canonicalHBaseTable
                  + "') map to the same Cloud Bigtable table '"
                  + bigtableTableName
                  + "'.");
        }

        if (!bigtableTableName.equals(hbaseTableName) && LOG.isTraceEnabled()) {
          LOG.trace(
              "Replicating HBase table {} to Cloud Bigtable table {}",
              hbaseTableName,
              bigtableTableName);
        }
        coalescedWalEntries
            .computeIfAbsent(bigtableTableName, k -> new ArrayList<>())
            .addAll(entries);
      }

      List<Future<Boolean>> futures = new ArrayList<>();
      for (Map.Entry<String, List<BigtableWALEntry>> entry : coalescedWalEntries.entrySet()) {
        futures.addAll(replicateTable(entry.getKey(), entry.getValue()));
      }

      // Check on the result  for all the batches.
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
                Collectors.groupingBy(
                    k -> new SimpleByteRange(k.getRowArray(), k.getRowOffset(), k.getRowLength()),
                    Collectors.toCollection(ArrayList::new)));

    // Now we have cells to replicate by rows, this list can be big and processing it on a single
    // thread is not efficient. As this thread will have to do proto translation and may need to
    // serialize the proto. So create micro batches at row boundaries (never split a row between
    // threads) and send them on separate threads.
    Map<ByteRange, List<Cell>> batchToReplicate = new HashMap<>();
    int numCellsInBatch = 0;
    int batchSizeInBytes = 0;
    for (Map.Entry<ByteRange, List<Cell>> rowCells : cellsToReplicateByRow.entrySet()) {

      // If bidirectional replication is enabled, tag row cells with source info
      // so Bigtable replicator skips this batch and does not replicate batch back to HBase.
      if (bidirectionalReplicationEnabled) {
        appendSourceTagToCells(rowCells);
      }

      int rowCellCount = rowCells.getValue().size();
      int rowSizeInBytes = getRowSize(rowCells);

      // If adding this row would exceed the batch size threshold or the maximum cell limit,
      // flush the current batch first.
      if (!batchToReplicate.isEmpty()
          && (batchSizeInBytes + rowSizeInBytes > batchSizeThresholdInBytes
              || numCellsInBatch + rowCellCount >= 100_000)) {
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

      // TODO handle the case where a single row has >100K mutations (very rare, but should not
      //  fail)
      numCellsInBatch += rowCellCount;
      batchSizeInBytes += rowSizeInBytes;
      batchToReplicate.put(rowCells.getKey(), rowCells.getValue());

      // If the batch meets or exceeds the threshold, flush it immediately.
      if (batchSizeInBytes >= batchSizeThresholdInBytes || numCellsInBatch >= 100_000) {
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
   * NOT eligible for replication: 1. Bidirectional replication is enabled, AND 2. A mutation in the
   * WAL entry is a special delete marker matching SOURCE_CBT qualifier and timestamp 0.
   *
   * @param walEntry from HBase replication WAL log.
   * @return true if walEntry is eligible for replication to Bigtable.
   */
  private boolean isEligibleForReplication(BigtableWALEntry walEntry) {
    if (walEntry == null || walEntry.getCells() == null || walEntry.getCells().isEmpty()) {
      return false;
    }

    // Always return eligible if bidirectional replication was not enabled
    int mutationCount = walEntry.getCells().size();
    if (!bidirectionalReplicationEnabled) {
      incrementMetric(ONE_WAY_REPL_ELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
      incrementMetric(ONE_WAY_REPL_ELIGIBLE_MUTATIONS_METRIC_KEY, mutationCount);
      return true;
    }

    Cell markerCell = null;
    if (sourceCbtQualifier != null) {
      for (Cell cell : walEntry.getCells()) {
        if (cell != null
            && cell.getTimestamp() == 0L
            && CellUtil.isDelete(cell)
            && CellUtil.matchingQualifier(cell, sourceCbtQualifier)) {
          markerCell = cell;
          break;
        }
      }
    }

    if (markerCell != null) {
      // Wrap in statement so row is not needlessly converted.
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Dropping WAL entry as it came from CBT , Row key: "
                + Bytes.toStringBinary(
                    markerCell.getRowArray(),
                    markerCell.getRowOffset(),
                    markerCell.getRowLength()));
      }
      incrementMetric(BIDIRECTIONAL_REPL_INELIGIBLE_WAL_ENTRY_METRIC_KEY, 1);
      // We decrement one because the special marker mutation from CBT is not a user mutation.
      incrementMetric(BIDIRECTIONAL_REPL_INELIGIBLE_MUTATIONS_METRIC_KEY, mutationCount - 1);
      return false;
    } else {
      // Wrap in statement so row is not needlessly converted.
      if (LOG.isTraceEnabled()) {
        for (Cell cell : walEntry.getCells()) {
          if (cell != null) {
            LOG.trace(
                "Replicating WAL entry as it originated on HBase , Row key: "
                    + Bytes.toStringBinary(
                        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            break;
          }
        }
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
