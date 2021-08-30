/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.CommonConnection;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;

/**
 * This is an hbase 1.x implementation of {@link AbstractBigtableAdmin}. Most methods in this class
 * unsupported
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
@SuppressWarnings("deprecation")
public class BigtableAdmin extends AbstractBigtableAdmin {

  public BigtableAdmin(CommonConnection connection) throws IOException {
    super(connection);
  }

  /** {@inheritDoc} */
  @Override
  public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    snapshotTable(snapshot.getName(), TableName.valueOf(snapshot.getTable()));
    LOG.warn(
        "isSnapshotFinished() is not currently supported by BigtableAdmin.\n"
            + "You may poll for existence of the snapshot with listSnapshots(snapshotName)");
    return MasterProtos.SnapshotResponse.newBuilder()
        .setExpectedTimeout(TimeUnit.MINUTES.toMillis(5))
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public void disableTableAsync(TableName tableName) throws IOException {
    disableTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void enableTableAsync(TableName tableName) throws IOException {
    enableTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(TableName tableName)
      throws IOException {
    throw new UnsupportedOperationException("getCompactionState");
  }

  /** {@inheritDoc} */
  @Override
  public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(byte[] bytes)
      throws IOException {
    throw new UnsupportedOperationException("getCompactionStateForRegion");
  }

  /**
   * {@inheritDoc}
   *
   * <p>The snapshot will be created with the ttl configured by {@link
   * com.google.cloud.bigtable.hbase.BigtableOptionsFactory#BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY}
   * key in the configuration. If not configured, the ttl will be set to serverside default.
   */
  @Override
  public void snapshot(
      String snapshotId, TableName tableName, HBaseProtos.SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotId, tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void snapshot(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshot.getName(), TableName.valueOf(snapshot.getTable()));
  }

  /** {@inheritDoc} */
  @Override
  public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    throw new UnsupportedOperationException("isSnapshotFinished"); // TODO
  }

  @Override
  public void restoreSnapshot(String s, boolean b, boolean b1)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  @Override
  public void cloneSnapshot(String s, TableName tableName, boolean b)
      throws IOException, TableExistsException, RestoreSnapshotException {
    if (!b) {
      this.cloneSnapshot(s, tableName);
    }
    throw new UnsupportedOperationException("cloneSnapshot"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
    List<String> backups =
        Futures.getChecked(
            adminClientWrapper.listBackupsAsync(getBackupClusterId()), IOException.class);

    List<HBaseProtos.SnapshotDescription> response = new ArrayList<>();

    for (String snapshot : backups) {
      response.add(HBaseProtos.SnapshotDescription.newBuilder().setName(snapshot).build());
    }
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  /** {@inheritDoc} */
  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    if (pattern == null || pattern.matcher("").matches()) {
      return ImmutableList.of();
    }

    List<HBaseProtos.SnapshotDescription> response = new ArrayList<>();
    for (HBaseProtos.SnapshotDescription description : listSnapshots()) {
      if (pattern.matcher(description.getName()).matches()) {
        response.add(description);
      }
    }
    return response;
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(
      String tableNameRegex, String snapshotNameRegex) throws IOException {
    throw new UnsupportedOperationException("Unsupported - please use listSnapshots");
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(
      Pattern tableNamePattern, Pattern snapshotNamePattern) throws IOException {
    throw new UnsupportedOperationException("Unsupported - please use listSnapshots");
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    throw new UnsupportedOperationException("isBalancerEnabled"); // TODO
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("getLastMajorCompactionTimestamp"); // TODO
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    throw new UnsupportedOperationException("getLastMajorCompactionTimestampForRegion"); // TODO
  }

  @Override
  public void setQuota(QuotaSettings quota) throws IOException {
    throw new UnsupportedOperationException("setQuota"); // TODO
  }

  @Override
  public QuotaRetriever getQuotaRetriever(QuotaFilter filter) throws IOException {
    throw new UnsupportedOperationException("getQuotaRetriever"); // TODO
  }

  @Override
  public boolean normalize() throws IOException {
    throw new UnsupportedOperationException("normalize"); // TODO
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    throw new UnsupportedOperationException("isNormalizerEnabled"); // TODO
  }

  @Override
  public boolean setNormalizerRunning(boolean on) throws IOException {
    throw new UnsupportedOperationException("setNormalizerRunning"); // TODO
  }

  @Override
  public boolean setCleanerChoreRunning(boolean b) throws IOException {
    throw new UnsupportedOperationException("setCleanerChoreRunning"); // TODO
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    throw new UnsupportedOperationException("runCleanerChore"); // TODO
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    throw new UnsupportedOperationException("isCleanerChoreEnabled"); // TODO
  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    throw new UnsupportedOperationException("isMasterInMaintenanceMode"); // TODO
  }

  @Override
  public boolean abortProcedure(long procId, boolean mayInterruptIfRunning) throws IOException {
    throw new UnsupportedOperationException("abortProcedure"); // TODO
  }

  @Override
  public ProcedureInfo[] listProcedures() throws IOException {
    throw new UnsupportedOperationException("listProcedures"); // TODO
  }

  @Override
  public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning)
      throws IOException {
    throw new UnsupportedOperationException("abortProcedureAsync"); // TODO
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    throw new UnsupportedOperationException("getSecurityCapabilities"); // TODO
  }

  @Override
  public boolean balancer(boolean arg0) throws IOException {
    throw new UnsupportedOperationException("balancer"); // TODO
  }

  @Override
  public boolean isSplitOrMergeEnabled(MasterSwitchType arg0) throws IOException {
    throw new UnsupportedOperationException("isSplitOrMergeEnabled"); // TODO
  }

  @Override
  public List<ServerName> listDeadServers() throws IOException {
    throw new UnsupportedOperationException("listDeadServers"); // TODO
  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> list) throws IOException {
    throw new UnsupportedOperationException("clearDeadServers"); // TODO
  }

  @Override
  public boolean[] setSplitOrMergeEnabled(boolean arg0, boolean arg1, MasterSwitchType... arg2)
      throws IOException {
    throw new UnsupportedOperationException("setSplitOrMergeEnabled"); // TODO
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return new ClusterStatus() {
      @Override
      public Collection<ServerName> getServers() {
        return Collections.emptyList();
      }
    };
  }
}
