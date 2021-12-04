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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseToCloudBigtableReplicationEndpoint extends BaseReplicationEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);
  // TODO: Clean up the debugging code
  private static final AtomicLong concurrentReplications = new AtomicLong();

  // Config keys to access project id and instance id from.
  private static final String PROJECT_KEY = "google.bigtable.project.id";
  private static final String INSTANCE_KEY = "google.bigtable.instance.id";
  private static final String APP_PROFILE_KEY = "google.bigtable.app_profile.id";
  // TODO add instance profile
  private static String projectId;
  private static String instanceId;
  private static String appProfileId;

  /**
   * Bigtable connection owned by this class and shared by all the instances of this class. DO NOT
   * CLOSE THIS CONNECTIONS from an instance  of the class. The state of this connection is
   * maintained by this class via reference counting.
   *
   * Creating a Bigtable connection is expensive as it creates num_cpu * 4 gRpc connections. Hence,
   * it is efficient and recommended to re-use the connection. Ref couting is required to properly
   * close the connection.
   */
  private static Connection connection;
  /**
   * Reference count for this connection.
   */
  private static int numConnectionReference = 0;

  // A replicator factory that creates a TableReplicator per replicated table.
  private CloudBigtableTableReplicatorFactory replicatorFactory;

  /**
   * Basic endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. This
   * implementation is not very efficient as it is single threaded.
   */
  public HbaseToCloudBigtableReplicationEndpoint() {
    super();
    // TODO Cleanup the debugging code.
    LOG.error(
        "Creating replication endpoint to CBT.",
        new RuntimeException("Dummy exception for stacktrace"));
    LogManager.getLogger(BigtableClientMetrics.class).setLevel(Level.DEBUG);
    LogManager.getLogger(HbaseToCloudBigtableReplicationEndpoint.class).setLevel(Level.DEBUG);
    LogManager.getLogger("com.google.cloud.bigtable.hbase.replication").setLevel(Level.DEBUG);
    LogManager.getLogger(CloudBigtableTableReplicator.class).setLevel(Level.DEBUG);
  }

  private synchronized static void getOrCreateBigtableConnection(Configuration configuration) {

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


  @Override
  protected synchronized void doStart() {
    LOG.error("Starting replication to CBT. ",
        new RuntimeException("Dummy exception for stacktrace."));

    getOrCreateBigtableConnection(ctx.getConfiguration());
    replicatorFactory = new CloudBigtableTableReplicatorFactory(connection);
    notifyStarted();
  }

  @Override
  protected void doStop() {

    LOG.error("Stopping replication to CBT for this EndPoint. ");
    // TODO: Do we need to call flush on all the TableReplicators? All the state is flushed per
    // replicate call, so if HBase does not call close when a replicate call is active then we don't
    // need to call a flush/close on the tableReplicator.

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
        " #######  In CBT replicate -- Concurrency: "
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
              replicatorFactory.getReplicator(tableName)
                  .replicateTable(walEntriesForTable.getValue());
          if (!result) {
            LOG.error(
                "Table replicator returned false in CBT replicate. " + tableName + " failed.");
          }
          // The overall operation is successful only if all the calls are successful.
          succeeded.set(result & succeeded.get());
        } catch (IOException e) {
          LOG.error("Got IOException from table replicator: ", e);
          // Suppress the exception here and return false to the replication machinery.
          succeeded.set(false);
        }
      } // Top level loop exiting
      // TODO update sink metrics, if everything was successful
    } catch (Exception e) {
      LOG.error("Failed to replicate some  WAL with exception: ", e);
      succeeded.set(false);
    } finally {
      LOG.error(
          "Exiting CBT replicate method after {} ms, Succeeded: {}",
          (System.currentTimeMillis() - startTime),
          succeeded.get());
      concurrentReplications.getAndDecrement();
    }
    return succeeded.get();
  }
}
