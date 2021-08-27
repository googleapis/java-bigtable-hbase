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
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseToCloudBigtableReplicationEndpoint extends BaseReplicationEndpoint {

  private final Logger LOG = LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);
  private Connection connection;
  private static final AtomicLong concurrentReplications = new AtomicLong();

  private static final String PROJECT_ID_KEY = "google.bigtable.project_id";
  private static final String INSTANCE_ID_KEY = "google.bigtable.instance";
  private static final String TABLE_ID_KEY = "google.bigtable.table";

  /**
   * Basic endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. This
   * implementation is not very efficient as it is single threaded.
   */
  public HbaseToCloudBigtableReplicationEndpoint() {
    super();
    LOG.error("Creating replication endpoint to CBT. ");
    LogManager.getLogger(BigtableClientMetrics.class).setLevel(Level.DEBUG);
  }

  @Override
  protected void doStart() {
    LOG.error("Starting replication to CBT. ");

    String projectId = ctx.getConfiguration().get(PROJECT_ID_KEY);
    String instanceId = ctx.getConfiguration().get(INSTANCE_ID_KEY);
    // Replicates to a single destination table. Consider creating a TableReplicator and using
    // a map <TableName, TableReplicator> here, that can delegate table level WAL entries to it.
    String tableId = ctx.getConfiguration().get(TABLE_ID_KEY);

    LOG.error("Connecting to " + projectId + ":" + instanceId + " table: " + tableId);
    connection = BigtableConfiguration.connect(projectId, instanceId);

    LOG.error("Created a connection to CBT. ");

    notifyStarted();
  }

  @Override
  protected void doStop() {

    LOG.error("Stopping replication to CBT. ");
    try {
      // TODO: Wait for each replicator to flush.
      connection.close();
    } catch (IOException e) {
      LOG.error("Failed to close connection ", e);
    }
    notifyStopped();
  }

  @Override
  public UUID getPeerUUID() {
    // TODO: Should this be fixed and retained across reboots?
    return UUID.randomUUID();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {

    long startTime = System.currentTimeMillis();
    List<WAL.Entry> entries = replicateContext.getEntries();
    long concurrent = concurrentReplications.incrementAndGet();
    LOG.error(
        "In CBT replicate -- Concurrency: "
            + concurrent
            + " wal size: "
            + entries.size()
            + " in thread ["
            + Thread.currentThread().getName()
            + "]");
    AtomicBoolean succeeded = new AtomicBoolean(true);

    // We were using APIFuture (GAX), its a wrapper for Guava future.
    // As of 3 weeks, we don't have to support Java7, we can use anything
    // Igor recommends java8 feature.
    // Can't use java7, client won't work on anything that is below java8.
    // Split the pacakge into HBase1 vs HBase2. The core functionality can be here.
    // Add a wapper on both 1.x paretn and 2.x parent

    try {
      final Map<String, List<WAL.Entry>> walEntriesByTable =
          entries.stream()
              .collect(groupingBy(entry -> entry.getKey().getTablename().getNameAsString()));

      for (Map.Entry<String, List<WAL.Entry>> walEntriesForTable : walEntriesByTable.entrySet()) {
        final String tableName = walEntriesForTable.getKey();
        // TODO: MAKE IT PARALLEL
        try {
          boolean result =
              CloudBigtableTableReplicatorFactory.getReplicator(tableName, connection)
                  .replicateTable(walEntriesForTable.getValue());
          // The overall operation is successful only if all the calls are successful.
          succeeded.set(result & succeeded.get());
        } catch (IOException e) {
          succeeded.set(false);
        }
      } // Top level loop exiting
    } finally {
      LOG.error("Exiting replicate method after {} ms", System.currentTimeMillis() - startTime);
      concurrentReplications.decrementAndGet();
    }
    return succeeded.get();
  }
}
