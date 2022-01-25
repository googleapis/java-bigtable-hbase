/*
 * Copyright 2015 Google Inc.
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

import static java.util.stream.Collectors.groupingBy;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapter;
import com.google.cloud.bigtable.hbase.replication.adapters.IncompatibleMutationAdapterFactory;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudBigtableReplicator {

  private static final Logger LOG =
      LoggerFactory.getLogger(CloudBigtableReplicator.class);


  // Config keys to access project id and instance id from.
  public static final String PROJECT_KEY = "google.bigtable.project.id";
  public static final String INSTANCE_KEY = "google.bigtable.instance.id";
  public static final String NUM_REPLICATION_SINK_THREADS_KEY = "google.bigtable.replication.thread_count";
  // TODO maybe it should depend on the number of processors on the VM.
  public static final int DEFAULT_THREAD_COUNT = 10;
  public static final String BATCH_SIZE_KEY = "google.bigtable.replication.batch_size_bytes";
  // TODO: Tune this parameter.
  public static final long DEFAULT_BATCH_SIZE_IN_BYTES = 1_000_000;

  private static String projectId;
  private static String instanceId;
  private static long batchSizeThresholdInBytes;
  private final AtomicInteger replicateMethodCount = new AtomicInteger(0);

  private static ExecutorService executorService;

  // TODO remove debugging code
  private static final AtomicLong concurrentReplications = new AtomicLong();

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
  /**
   * Reference count for this connection.
   */
  private static int numConnectionReference = 0;

  // A replicator factory that creates a TableReplicator per replicated table.
  // private CloudBigtableTableReplicatorFactory replicatorFactory;

  /**
   * Basic endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. This
   * implementation is not very efficient as it is single threaded.
   */
  public CloudBigtableReplicator() {
    // TODO Cleanup the debugging code.
    LOG.error(
        "Creating replication endpoint to CBT.",
        new RuntimeException("Dummy exception for stacktrace"));
    LogManager.getLogger(BigtableClientMetrics.class).setLevel(Level.DEBUG);
    LogManager.getLogger(HbaseToCloudBigtableReplicationEndpoint.class).setLevel(Level.DEBUG);
    LogManager.getLogger("com.google.cloud.bigtable.hbase.replication").setLevel(Level.DEBUG);
    LogManager.getLogger(CloudBigtableReplicationTask.class).setLevel(Level.DEBUG);
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
    executorService = new ThreadPoolExecutor(numThreads, numThreads, 60,
        TimeUnit.SECONDS, new LinkedBlockingDeque<>(), new ThreadFactory() {
      private final AtomicInteger threadCount = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("cloud-bigtable-replication-sink-" + threadCount.incrementAndGet());
        return thread;
      }
    });
  }

  private static synchronized void getOrCreateBigtableConnection(Configuration configuration) {

    if (numConnectionReference == 0) {
      // We may overwrite teh project/instanceId but they are going to be the same throghtout the
      // lifecycle of the JVM.
      projectId = configuration.get(PROJECT_KEY);
      instanceId = configuration.get(INSTANCE_KEY);
      // If an App profile is provided, it will be picked automatically by the connection.
      connection = BigtableConfiguration.connect(configuration);
      LOG.error("Created a connection to CBT. " + projectId + "--" + instanceId);
    }

    numConnectionReference++;
  }

  public synchronized void start(Configuration configuration, MetricsSource metricsSource) {
    LOG.error(
        "Starting replication to CBT. ", new RuntimeException("Dummy exception for stacktrace."));

    getOrCreateBigtableConnection(configuration);
    // Create the executor service for the first time.
    initExecutorService(configuration);
    batchSizeThresholdInBytes = configuration.getLong(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE_IN_BYTES);

    this.incompatibleMutationAdapter = new IncompatibleMutationAdapterFactory(
        configuration, metricsSource, connection).createIncompatibleMutationAdapter();
  }

  protected void stop() {

    LOG.error("Stopping replication to CBT for this EndPoint. ",
        new RuntimeException("Dummy exception for stacktrace"));

    // Connection is shared by all the instances of this class, close it only if no one is using it.
    // Closing the connection is required as it owns the underlying gRpc connections and gRpc does
    // not like JVM shutting without closing the gRpc connections.
    synchronized (HbaseToCloudBigtableReplicationEndpoint.class) {
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

  public boolean replicate(List<WAL.Entry> walsToReplicate) {
    long concurrent = concurrentReplications.incrementAndGet();
    LOG.error(
        " #######  In CBT replicate {" + replicateMethodCount.get() + "} -- Concurrency: "
            + concurrent
            + " wal entry count: "
            + walsToReplicate.size()
            + " in thread ["
            + Thread.currentThread().getName()
            + "]");

    long startTime = System.currentTimeMillis();
    replicateMethodCount.incrementAndGet();

    boolean succeeded = true;

    // Create the batches to replicate
    final Map<String, List<WAL.Entry>> walEntriesByTable =
        walsToReplicate.stream()
            .collect(groupingBy(entry -> entry.getKey().getTablename().getNameAsString()));

    List<Future<Boolean>> futures = new ArrayList<>();
    for (Map.Entry<String, List<WAL.Entry>> walEntriesForTable : walEntriesByTable.entrySet()) {
      List<Cell> cellsToReplicateForTable = new ArrayList<>();
      final String tableName = walEntriesForTable.getKey();
      int batchSizeInBytes = 0;

      for (WAL.Entry walEntry : walEntriesForTable.getValue()) {
        //
        // LOG.warn(
        //     "Processing WALKey: "
        //         + walEntry.getKey().toString()
        //         + " with "
        //         + walEntry.getEdit().getCells().size()
        //         + " cells. Written at: " + walEntry.getKey().getWriteTime());

        // Translate the incompatible mutations.
        List<Cell> compatibleCells = incompatibleMutationAdapter.adaptIncompatibleMutations(
            walEntry);
        cellsToReplicateForTable.addAll(compatibleCells);
      }

      // group the data by the rowkey before sending it on multiple threads. It is very important
      // to group by rowKey here. This grouping guarantees that mutations from same row are not sent
      // concurrently in different batches. If same row  mutations are sent concurrently, they may
      // be applied out of order. Out of order applies cause divergence between 2 databases and
      // must be avoided as much as possible.
      Map<ByteRange, List<Cell>> cellsToReplicateByRow =
          cellsToReplicateForTable.stream()
              .collect(
                  groupingBy(
                      k -> new SimpleByteRange(k.getRowArray(), k.getRowOffset(),
                          k.getRowLength())));

      // Now we have cells to replicate by rows, this list can be big and processing it on a single
      // thread is not efficient. As this thread will have to do proto translation and may need to
      // serialize the proto. So create micro batches at row boundaries (never split a row between
      // threads) and send them on separate threads.
      Map<ByteRange, List<Cell>> batchToReplicate = new HashMap<>();
      int numCellsInBatch = 0;
      for (Map.Entry<ByteRange, List<Cell>> rowCells : cellsToReplicateByRow.entrySet()) {
        LOG.error(
            "Replicating row: " + Bytes.toStringBinary(rowCells.getKey().deepCopyToNewArray()));
        numCellsInBatch += rowCells.getValue().size();
        batchSizeInBytes += getRowSize(rowCells);
        batchToReplicate.put(rowCells.getKey(), rowCells.getValue());

        if (batchSizeInBytes >= batchSizeThresholdInBytes) {
          LOG.debug("Replicating a batch of " + batchToReplicate.size() + " rows and "
              + numCellsInBatch + " cells with heap size " + batchSizeInBytes + " for table: "
              + tableName);
          futures.add(replicateBatch(tableName, batchToReplicate));
          batchToReplicate = new HashMap<>();
          numCellsInBatch = 0;
          batchSizeInBytes = 0;
        }
      } // Rows per table ends here

      // Flush last batch
      if (!batchToReplicate.isEmpty()) {
        futures.add(replicateBatch(tableName, batchToReplicate));
      }

    } // WAL entries by table finishes here.

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
      LOG.error(
          "Exiting CBT replicate method {" + replicateMethodCount.get()
              + "} after {} ms, Succeeded: {} ",
          (System.currentTimeMillis() - startTime), succeeded);
      concurrentReplications.getAndDecrement();
    }

    return succeeded;
  }

  private int getRowSize(Map.Entry<ByteRange, List<Cell>> rowCells) {
    int rowSizeInBytes = 0;
    for (Cell cell : rowCells.getValue()) {
      rowSizeInBytes += CellUtil.estimatedHeapSizeOf(cell);
    }
    return rowSizeInBytes;
  }


  private Future<Boolean> replicateBatch(String tableName,
      Map<ByteRange, List<Cell>> batchToReplicate) {
    try {
      CloudBigtableReplicationTask replicationTask = new CloudBigtableReplicationTask(tableName,
          connection, batchToReplicate);
      return executorService.submit(replicationTask);
    } catch (IOException ex) {
      LOG.error("Failed to crete a table object for table: " + tableName);
      return CompletableFuture.completedFuture(false);
    }

  }
}
