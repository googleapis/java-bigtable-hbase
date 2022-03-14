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

package com.google.cloud.bigtable.hbase2_x.replication;
// TODO reverse the package names for 1.x and 2.x to com.google.cloud.bigtable.replication.hbase1_x

import static org.apache.hadoop.hbase.replication.BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY;

import com.google.bigtable.repackaged.com.google.api.core.InternalExtensionOnly;
import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicator;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import com.google.cloud.bigtable.hbase2_x.replication.metrics.HBaseMetricsExporter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.NamespaceTableCfWALEntryFilter;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ScopeWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(remove BaseReplicationEndpoint extension).
@InternalExtensionOnly
public class HbaseToCloudBigtableReplicationEndpoint extends AbstractService implements
    ReplicationEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);

  private final CloudBigtableReplicator cloudBigtableReplicator;
  private final HBaseMetricsExporter metricsExporter;
  protected Context ctx;

  public HbaseToCloudBigtableReplicationEndpoint() {
    cloudBigtableReplicator = new CloudBigtableReplicator();
    metricsExporter = new HBaseMetricsExporter();
  }

  @Override
  public void init(Context context) throws IOException {
    this.ctx = context;

    if (this.ctx != null){
      ReplicationPeer peer = this.ctx.getReplicationPeer();
      if (peer != null){
        peer.registerPeerConfigListener(this);
      } else {
        LOG.warn("Not tracking replication peer config changes for Peer Id " + this.ctx.getPeerId() +
            " because there's no such peer");
      }
    }
  }

  @Override
  public boolean canReplicateToSameCluster() {
    return false;
  }

  @Override
  public UUID getPeerUUID() {
    return cloudBigtableReplicator.getPeerUUID();
  }

  @Override
  public WALEntryFilter getWALEntryfilter() {
    ArrayList<WALEntryFilter> filters = Lists.newArrayList();
    WALEntryFilter scopeFilter = getScopeWALEntryFilter();
    if (scopeFilter != null) {
      filters.add(scopeFilter);
    }
    WALEntryFilter tableCfFilter = getNamespaceTableCfWALEntryFilter();
    if (tableCfFilter != null) {
      filters.add(tableCfFilter);
    }
    if (ctx != null && ctx.getPeerConfig() != null) {
      String filterNameCSV = ctx.getPeerConfig().getConfiguration().get(REPLICATION_WALENTRYFILTER_CONFIG_KEY);
      if (filterNameCSV != null && !filterNameCSV.isEmpty()) {
        String[] filterNames = filterNameCSV.split(",");
        for (String filterName : filterNames) {
          try {
            Class<?> clazz = Class.forName(filterName);
            filters.add((WALEntryFilter) clazz.getDeclaredConstructor().newInstance());
          } catch (Exception e) {
            LOG.error("Unable to create WALEntryFilter " + filterName, e);
          }
        }
      }
    }
    return filters.isEmpty() ? null : new ChainWALEntryFilter(filters);
  }

  /** Returns a WALEntryFilter for checking the scope. Subclasses can
   * return null if they don't want this filter */
  protected WALEntryFilter getScopeWALEntryFilter() {
    return new ScopeWALEntryFilter();
  }

  /** Returns a WALEntryFilter for checking replication per table and CF. Subclasses can
   * return null if they don't want this filter */
  protected WALEntryFilter getNamespaceTableCfWALEntryFilter() {
    return new NamespaceTableCfWALEntryFilter(ctx.getReplicationPeer());
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    Map<String, List<BigtableWALEntry>> walEntriesByTable = new HashMap<>();
    for (WAL.Entry wal : replicateContext.getEntries()) {
      String tableName = wal.getKey().getTableName().getNameAsString();
      BigtableWALEntry bigtableWALEntry =
          new BigtableWALEntry(wal.getKey().getWriteTime(), wal.getEdit().getCells(), tableName);
      if (!walEntriesByTable.containsKey(tableName)) {
        walEntriesByTable.put(tableName, new ArrayList<>());
      }
      walEntriesByTable.get(tableName).add(bigtableWALEntry);
    }
    return cloudBigtableReplicator.replicate(walEntriesByTable);
  }

  @Override
  public boolean isStarting() {
    return state() == State.STARTING;
  }

  @Override
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  @Override
  protected void doStart() {
    metricsExporter.setMetricsSource(ctx.getMetrics());
    cloudBigtableReplicator.start(ctx.getConfiguration(), metricsExporter);
    notifyStarted();
  }

  @Override
  protected void doStop() {
    cloudBigtableReplicator.stop();
    notifyStopped();
  }

  @Override
  public void peerConfigUpdated(ReplicationPeerConfig replicationPeerConfig) {

  }
}
