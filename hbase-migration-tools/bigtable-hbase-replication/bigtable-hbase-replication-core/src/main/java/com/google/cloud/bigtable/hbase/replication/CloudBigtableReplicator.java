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
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_BATCH_SIZE_IN_BYTES;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.DEFAULT_THREAD_COUNT;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.INSTANCE_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.NUM_REPLICATION_SINK_THREADS_KEY;
import static com.google.cloud.bigtable.hbase.replication.configuration.HBaseToCloudBigtableReplicationConfiguration.PROJECT_KEY;
import static java.util.stream.Collectors.groupingBy;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapterFactory;
import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudBigtableReplicator {

  private static final Logger LOG = LoggerFactory.getLogger(CloudBigtableReplicator.class);

  private long batchSizeThresholdInBytes;

  private static ExecutorService executorService;

  private IncompatibleMutationAdapter incompatibleMutationAdapter;

  /**
   * Bigtable connection owned by this class and shared by all the instances of this class. DO NOT
   * CLOSE THIS CONNECTION from an instance of the class. The state of this connection is maintained
   * by this class via reference counting.
   *
   * <p>Creating a Bigtable connection is expensive as it creates num_cpu * 4 gRpc connections.
   * Hence, it is efficient and recommended to re-use the connection. Ref counting is required to
   * properly close the connection.
   */
  private static Connection connection;
  /** Reference count for this connection. */
  private static int numConnectionReference = 0;

  /** Common endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. */
  public CloudBigtableReplicator() {
    // TODO: Validate that loggers are correctly configured.
  }

  /**
   * Creates a bounded cachedThreadPool with unbounded work queue. This method should be called from
   * doStart() as it needs configuration to be initialized.
   */
  private static synchronized void initExecutorService(Configuration conf) {
    if (executorService != null) {
      // Already initialized. Nothing to do.
      return;
    }

    /* Create a bounded cached thread pool with unbounded queue. At any point, we will only have 1
     * replicate() method active per replicationEndpoint object, so we will never have too many
     * tasks in the queue. Having static thread pool bounds the parallelism of CBT replication
     * library.
     */
    int numThreads = conf.getInt(NUM_REPLICATION_SINK_THREADS_KEY, DEFAULT_THREAD_COUNT);
    executorService =
        Executors.newFixedThreadPool(
            numThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("cloud-bigtable-replication-sink-%d")
                .build());
  }

  private static synchronized void getOrCreateBigtableConnection(Configuration configuration) {

    Configuration configurationCopy = new Configuration(configuration);
    if (numConnectionReference == 0) {

      String projectId = configurationCopy.get(PROJECT_KEY);
      String instanceId = configurationCopy.get(INSTANCE_KEY);
      // If an App profile is provided, it will be picked automatically by the connection.
      connection = BigtableConfiguration.connect(configurationCopy);
      LOG.info("Created a connection to CBT. " + projectId + "--" + instanceId);
    }

    numConnectionReference++;
  }

  public synchronized void start(Configuration configuration, MetricsExporter metricsExporter) {
    LOG.info("Starting replication to CBT.");

    getOrCreateBigtableConnection(configuration);
    // Create the executor service for the first time.
    initExecutorService(configuration);
    batchSizeThresholdInBytes = configuration.getLong(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE_IN_BYTES);

    this.incompatibleMutationAdapter =
        new IncompatibleMutationAdapterFactory(configuration, metricsExporter, connection)
            .createIncompatibleMutationAdapter();
  }

  public void stop() {

    LOG.info("Stopping replication to CBT.");

    // Connection is shared by all the instances of this class, close it only if no one is using it.
    // Closing the connection is required as it owns the underlying gRpc connections and gRpc does
    // not like JVM shutting without closing the gRpc connections.
    synchronized (CloudBigtableReplicator.class) {
      if (--numConnectionReference == 0) {
        try {
          LOG.warn("Closing the Bigtable connection.");
          connection.close();
        } catch (IOException e) {
          LOG.error("Failed to close Bigtable connection: ", e);
        }
      }
    }
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

      // Translate the incompatible mutations.
      List<Cell> compatibleCells = incompatibleMutationAdapter.adaptIncompatibleMutations(walEntry);
      cellsToReplicateForTable.addAll(compatibleCells);
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

      // TODO handle the case where a single row has >100K mutations (very rare, but should not
      // fail)
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
          new CloudBigtableReplicationTask(tableName, connection, batchToReplicate);
      return executorService.submit(replicationTask);
    } catch (IOException ex) {
      LOG.error("Failed to submit a batch for table: " + tableName, ex);
      return CompletableFuture.completedFuture(false);
    }
  }
}
