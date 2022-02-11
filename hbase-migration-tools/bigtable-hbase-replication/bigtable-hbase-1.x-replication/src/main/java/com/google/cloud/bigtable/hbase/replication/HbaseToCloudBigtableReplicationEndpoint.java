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

import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase.replication.metrics.HBaseMetricsExporter;
import java.util.*;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseToCloudBigtableReplicationEndpoint extends BaseReplicationEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);

  private final CloudBigtableReplicator cloudBigtableReplicator;
  private HBaseMetricsExporter metricsExporter;

  // Config keys to access project id and instance id from.

  /**
   * Basic endpoint that listens to CDC from HBase and replicates to Cloud Bigtable. This
   * implementation is not very efficient as it is single threaded.
   */
  public HbaseToCloudBigtableReplicationEndpoint() {
    super();
    cloudBigtableReplicator = new CloudBigtableReplicator();
    metricsExporter = new HBaseMetricsExporter();
  }

  @Override
  protected synchronized void doStart() {
    LOG.error(
        "Starting replication to CBT. ", new RuntimeException("Dummy exception for stacktrace."));
    metricsExporter.setMetricsSource(ctx.getMetrics());
    cloudBigtableReplicator.start(ctx.getConfiguration(), metricsExporter);
    notifyStarted();
  }

  @Override
  protected void doStop() {

    LOG.error(
        "Stopping replication to CBT for this EndPoint. ",
        new RuntimeException("Dummy exception for stacktrace"));
    cloudBigtableReplicator.stop();
    notifyStopped();
  }

  @Override
  public UUID getPeerUUID() {
    return cloudBigtableReplicator.getPeerUUID();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    Map<String, List<BigtableWALEntry>> walEntriesByTable = new HashMap<>();

    for (WAL.Entry wal : replicateContext.getEntries()) {

      String tableName = wal.getKey().getTablename().getNameAsString();
      BigtableWALEntry bigtableWALEntry =
          new BigtableWALEntry(wal.getKey().getWriteTime(), wal.getEdit().getCells(), tableName);

      if (!walEntriesByTable.containsKey(tableName)) {
        walEntriesByTable.put(tableName, new ArrayList<>());
      }

      walEntriesByTable.get(tableName).add(bigtableWALEntry);
    }
    return cloudBigtableReplicator.replicate(walEntriesByTable);
  }
}
