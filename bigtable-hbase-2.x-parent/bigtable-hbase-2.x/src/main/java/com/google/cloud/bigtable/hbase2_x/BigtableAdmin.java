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
package com.google.cloud.bigtable.hbase2_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.util.FutureUtil;
import com.google.cloud.bigtable.hbase.util.ModifyTableBuilder;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.TableAdapter2x;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.LogQueryFilter;
import org.apache.hadoop.hbase.client.OnlineLogRecord;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 2.x specific implementation of {@link AbstractBigtableAdmin}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAdmin extends AbstractBigtableAdmin {

  private final BigtableAsyncAdmin asyncAdmin;

  public BigtableAdmin(AbstractBigtableConnection connection) throws IOException {
    super(connection);
    asyncAdmin = new BigtableAsyncAdmin(connection);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableDescriptor desc) throws IOException {
    createTable(desc, null);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    createTable(desc, createSplitKeys(startKey, endKey, numRegions));
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
    createTable(desc.getTableName(), TableAdapter2x.adapt(desc, splitKeys));
  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor tableDescriptor) throws IOException {
    return asyncAdmin.createTable(tableDescriptor);
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    return asyncAdmin.createTable(desc, splitKeys);
  }

  /** {@inheritDoc} */
  @Override
  public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  /** {@inheritDoc} */
  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    if (pattern == null || pattern.matcher("").matches()) {
      return ImmutableList.of();
    }

    List<SnapshotDescription> response = new ArrayList<>();
    for (SnapshotDescription description : listSnapshots()) {
      if (pattern.matcher(description.getName()).matches()) {
        response.add(description);
      }
    }
    return response;
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    List<String> backups =
        Futures.getChecked(
            adminClientWrapper.listBackupsAsync(getBackupClusterId()), IOException.class);
    List<SnapshotDescription> response = new ArrayList<>();
    for (String backup : backups) {
      response.add(new SnapshotDescription(backup));
    }
    return response;
  }

  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException("use deleteSnapshot instead");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Calling {@link #addColumn(TableName, ColumnFamilyDescriptor)} was causing stackoverflow.
   * Copying the same code here. //TODO - need to find a better way
   */
  @Override
  public void addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDesc)
      throws IOException {
    modifyColumns(
        tableName,
        columnFamilyDesc.getNameAsString(),
        "add",
        ModifyTableBuilder.newBuilder(tableName)
            .add(TableAdapter2x.toHColumnDescriptor(columnFamilyDesc)));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Calling {@link #addColumn(TableName, ColumnFamilyDescriptor)} was causing stackoverflow.
   * Copying the same code here. //TODO - need to find a better way
   */
  @Override
  public void modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDesc)
      throws IOException {
    modifyColumns(
        tableName,
        columnFamilyDesc.getNameAsString(),
        "modify",
        ModifyTableBuilder.newBuilder(tableName)
            .modify(TableAdapter2x.toHColumnDescriptor(columnFamilyDesc)));
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> deleteNamespaceAsync(String name) throws IOException {
    deleteNamespace(name);
    // TODO Consider better options after adding support for async hbase2
    return CompletableFuture.runAsync(() -> {});
  }

  @Override
  public String[] listNamespaces() throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> disableTableAsync(TableName tableName) throws IOException {
    return asyncAdmin.disableTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> enableTableAsync(TableName tableName) throws IOException {
    return asyncAdmin.enableTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public TableDescriptor getDescriptor(TableName tableName) throws IOException {
    return getTableDescriptor(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void snapshot(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    Objects.requireNonNull(snapshot);
    snapshot(snapshot.getName(), snapshot.getTableName());
  }

  /** {@inheritDoc} */
  @Override
  public void snapshot(String snapshotId, TableName tableName, SnapshotType ignored)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotId, tableName);
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public Future<Void> snapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    LOG.warn(
        "isSnapshotFinished() is not currently supported by BigtableAdmin.\n"
            + "You may poll for existence of the snapshot with listSnapshots(snapshotName)");
    return asyncAdmin.snapshot(snapshot);
  }

  @Override
  public Future<Void> addColumnFamilyAsync(
      TableName tableName, ColumnFamilyDescriptor columnFamily) {
    return asyncAdmin.addColumnFamily(tableName, columnFamily);
  }

  @Override
  public void deleteColumnFamily(TableName tableName, byte[] columnName) throws IOException {
    deleteColumn(tableName, columnName);
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnName) {
    return asyncAdmin.deleteColumnFamily(tableName, columnName);
  }

  protected CompletableFuture<Void> deleteTableAsyncInternal(TableName tableName) {
    return ApiFutureUtils.toCompletableFuture(
        adminClientWrapper.deleteTableAsync(tableName.getNameAsString()));
  }

  @Override
  public Future<Void> deleteTableAsync(TableName tableName) throws IOException {
    return deleteTableAsyncInternal(tableName);
  }

  @Override
  public int getSyncWaitTimeout() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<TableDescriptor> listTableDescriptors() throws IOException {
    return Arrays.asList(listTables());
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern) throws IOException {
    return Arrays.asList(listTables(pattern));
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
    List<TableDescriptor> response = new ArrayList<TableDescriptor>();
    for (TableName tableName : tableNames) {
      TableDescriptor desc = getTableDescriptor(tableName);
      if (desc != null) {
        response.add(desc);
      }
    }
    return response;
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
      throws IOException {
    return Arrays.asList(listTables(pattern, includeSysTables));
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] namespace)
      throws IOException {
    final String namespaceStr = Bytes.toString(namespace);
    return Arrays.asList(listTableDescriptorsByNamespace(namespaceStr));
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(String tableName, String snapshotId)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported - please use listSnapshots");
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern tableName, Pattern snapshotName)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported - please use listSnapshots");
  }

  @Override
  public Future<Void> modifyColumnFamilyAsync(
      TableName tableName, ColumnFamilyDescriptor columnFamily) throws IOException {
    return asyncAdmin.modifyColumnFamily(tableName, columnFamily);
  }

  @Override
  public void modifyTable(TableDescriptor tableDescriptor) throws IOException {
    modifyTable(tableDescriptor.getTableName(), tableDescriptor);
  }

  @Override
  public void modifyTable(TableName tableName, TableDescriptor tableDescriptor) throws IOException {
    FutureUtil.unwrap(modifyTableAsync(tableName, tableDescriptor));
  }

  @Override
  public Future<Void> modifyTableAsync(TableDescriptor tableDescriptor) throws IOException {
    return modifyTableAsync(tableDescriptor.getTableName(), tableDescriptor);
  }

  @Override
  public Future<Void> modifyTableAsync(TableName tableName, TableDescriptor newDescriptor) {
    return asyncAdmin
        .getDescriptor(tableName)
        .thenApply(
            descriptor ->
                ModifyTableBuilder.buildModifications(
                    new HTableDescriptor(newDescriptor), new HTableDescriptor(descriptor)))
        .thenApply(
            modifications -> {
              try {
                return modifyColumns(tableName, null, "modifyTableAsync", modifications);
              } catch (IOException e) {
                throw new CompletionException(e);
              }
            });
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.Admin#truncateTableAsync(org.apache.hadoop.hbase.TableName, boolean)
   */
  @Override
  public Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits)
      throws IOException {
    if (!preserveSplits) {
      LOG.info("truncate will preserveSplits. The passed in variable is ignored.");
    }
    return ApiFutureUtils.toCompletableFuture(
        adminClientWrapper.dropAllRowsAsync(tableName.getNameAsString()));
  }
  /* ******* Unsupported methods *********** */

  @Override
  public boolean abortProcedure(long arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("abortProcedure");
  }

  @Override
  public Future<Boolean> abortProcedureAsync(long arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("abortProcedureAsync");
  }

  @Override
  public boolean balance() throws IOException {
    throw new UnsupportedOperationException("balance");
  }

  @Override
  public boolean balance(boolean arg0) throws IOException {
    throw new UnsupportedOperationException("balance");
  }

  @Override
  public boolean balancerSwitch(boolean arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("balancerSwitch");
  }

  @Override
  public boolean catalogJanitorSwitch(boolean arg0) throws IOException {
    throw new UnsupportedOperationException("catalogJanitorSwitch");
  }

  @Override
  public boolean cleanerChoreSwitch(boolean arg0) throws IOException {
    throw new UnsupportedOperationException("cleanerChoreSwitch");
  }

  @Override
  public void clearCompactionQueues(ServerName arg0, Set<String> arg1)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("clearCompactionQueues");
  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> arg0) throws IOException {
    throw new UnsupportedOperationException("clearDeadServers");
  }

  @Override
  public void cloneSnapshot(String arg0, TableName arg1, boolean arg2)
      throws IOException, TableExistsException, RestoreSnapshotException {
    throw new UnsupportedOperationException("cloneSnapshot");
  }

  @Override
  public Future<Void> cloneSnapshotAsync(String arg0, TableName arg1)
      throws IOException, TableExistsException {
    throw new UnsupportedOperationException("cloneSnapshotAsync");
  }

  @Override
  public Future<Void> cloneSnapshotAsync(String s, TableName tableName, boolean b)
      throws IOException, TableExistsException, RestoreSnapshotException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void cloneTableSchema(TableName tableName, TableName tableName1, boolean b) {
    throw new UnsupportedOperationException("cloneTableSchema"); // TODO
  }

  @Override
  public boolean switchRpcThrottle(boolean enable) throws IOException {
    throw new UnsupportedOperationException("switchRpcThrottle");
  }

  @Override
  public boolean isRpcThrottleEnabled() throws IOException {
    throw new UnsupportedOperationException("isRpcThrottleEnabled");
  }

  @Override
  public boolean exceedThrottleQuotaSwitch(boolean b) throws IOException {
    throw new UnsupportedOperationException("exceedThrottleQuotaSwitch");
  }

  @Override
  public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
    throw new UnsupportedOperationException("getSpaceQuotaTableSizes");
  }

  @Override
  public Map<TableName, ? extends SpaceQuotaSnapshotView> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) throws IOException {
    throw new UnsupportedOperationException("getRegionServerSpaceQuotaSnapshots");
  }

  @Override
  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
    throw new UnsupportedOperationException("getCurrentSpaceQuotaSnapshot");
  }

  @Override
  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(TableName tableName)
      throws IOException {
    throw new UnsupportedOperationException("getCurrentSpaceQuotaSnapshot");
  }

  @Override
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
      throws IOException {
    throw new UnsupportedOperationException("grant");
  }

  @Override
  public void revoke(UserPermission userPermission) throws IOException {
    throw new UnsupportedOperationException("revoke");
  }

  @Override
  public List<UserPermission> getUserPermissions(
      GetUserPermissionsRequest getUserPermissionsRequest) throws IOException {
    throw new UnsupportedOperationException("getUserPermissions");
  }

  @Override
  public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
      throws IOException {
    throw new UnsupportedOperationException("hasUserPermissions");
  }

  @Override
  public boolean snapshotCleanupSwitch(boolean b, boolean b1) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isSnapshotCleanupEnabled() throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<OnlineLogRecord> getSlowLogResponses(
      Set<ServerName> set, LogQueryFilter logQueryFilter) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<Boolean> clearSlowLogResponses(Set<ServerName> set) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void compact(TableName arg0, CompactType arg1) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("compact");
  }

  @Override
  public void compact(TableName arg0, byte[] arg1, CompactType arg2)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("compact");
  }

  @Override
  public Future<Void> createNamespaceAsync(NamespaceDescriptor arg0) throws IOException {
    throw new UnsupportedOperationException("createNamespaceAsync");
  }

  @Override
  public void decommissionRegionServers(List<ServerName> arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("decommissionRegionServers");
  }

  @Override
  public void disableTableReplication(TableName arg0) throws IOException {
    throw new UnsupportedOperationException("disableTableReplication");
  }

  @Override
  public void enableTableReplication(TableName arg0) throws IOException {
    throw new UnsupportedOperationException("enableTableReplication");
  }

  @Override
  public Future<Void> enableReplicationPeerAsync(String s) {
    throw new UnsupportedOperationException("enableTableReplication");
  }

  @Override
  public Future<Void> disableReplicationPeerAsync(String s) {
    throw new UnsupportedOperationException("disableReplicationPeerAsync");
  }

  @Override
  public byte[] execProcedureWithReturn(String arg0, String arg1, Map<String, String> arg2)
      throws IOException {
    throw new UnsupportedOperationException("execProcedureWithReturn");
  }

  @Override
  public CompactionState getCompactionState(TableName arg0) throws IOException {
    throw new UnsupportedOperationException("getCompactionState");
  }

  @Override
  public CompactionState getCompactionState(TableName arg0, CompactType arg1) throws IOException {
    throw new UnsupportedOperationException("getCompactionState");
  }

  @Override
  public CompactionState getCompactionStateForRegion(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException("getCompactionStateForRegion");
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName arg0) throws IOException {
    throw new UnsupportedOperationException("getLastMajorCompactionTimestamp");
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException("getLastMajorCompactionTimestamp");
  }

  @Override
  public String getLocks() throws IOException {
    // TODO : new in 2.0
    throw new UnsupportedOperationException("getLocks");
  }

  @Override
  public String getProcedures() throws IOException {
    // TODO : new in 2.0
    throw new UnsupportedOperationException("getProcedures");
  }

  @Override
  public QuotaRetriever getQuotaRetriever(QuotaFilter arg0) throws IOException {
    throw new UnsupportedOperationException("getQuotaRetriever");
  }

  @Override
  public List<RegionInfo> getRegions(ServerName arg0) throws IOException {
    throw new UnsupportedOperationException("getRegions");
  }

  @Override
  public void flushRegionServer(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException("flushRegionServer");
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    List<RegionInfo> regionInfo = new ArrayList<>();
    for (HRegionInfo hRegionInfo : getTableRegions(tableName)) {
      regionInfo.add(hRegionInfo);
    }
    return regionInfo;
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    throw new UnsupportedOperationException("getSecurityCapabilities");
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    throw new UnsupportedOperationException("isBalancerEnabled");
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    throw new UnsupportedOperationException("isCleanerChoreEnabled");
  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    throw new UnsupportedOperationException("isMasterInMaintenanceMode");
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    throw new UnsupportedOperationException("isNormalizerEnabled");
  }

  @Override
  public boolean isSnapshotFinished(SnapshotDescription arg0)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    throw new UnsupportedOperationException("isSnapshotFinished");
  }

  @Override
  public List<ServerName> listDeadServers() throws IOException {
    throw new UnsupportedOperationException("listDeadServers");
  }

  @Override
  public List<ServerName> listDecommissionedRegionServers() throws IOException {
    throw new UnsupportedOperationException("listDecommissionedRegionServers");
  }

  @Override
  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    throw new UnsupportedOperationException("listReplicatedTableCFs");
  }

  @Override
  public void majorCompact(TableName arg0, CompactType arg1)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("majorCompact"); // TODO
  }

  @Override
  public void majorCompact(TableName arg0, byte[] arg1, CompactType arg2)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("majorCompact"); // TODO
  }

  @Override
  public Map<ServerName, Boolean> compactionSwitch(
      boolean switchState, List<String> serverNamesList) throws IOException {
    throw new UnsupportedOperationException("compactionSwitch");
  }

  @Override
  public Future<Void> mergeRegionsAsync(byte[][] arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("mergeRegionsAsync"); // TODO
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
    throw new UnsupportedOperationException("splitRegionAsync");
  }

  @Override
  public Future<Void> mergeRegionsAsync(byte[] arg0, byte[] arg1, boolean arg2) throws IOException {
    throw new UnsupportedOperationException("mergeRegionsAsync"); // TODO
  }

  @Override
  public Future<Void> modifyNamespaceAsync(NamespaceDescriptor arg0) throws IOException {
    throw new UnsupportedOperationException("modifyNamespaceAsync"); // TODO
  }

  @Override
  public boolean normalize() throws IOException {
    throw new UnsupportedOperationException("normalize"); // TODO
  }

  @Override
  public boolean normalizerSwitch(boolean arg0) throws IOException {
    throw new UnsupportedOperationException("normalizerSwitch"); // TODO
  }

  @Override
  public void recommissionRegionServer(ServerName arg0, List<byte[]> arg1) throws IOException {
    throw new UnsupportedOperationException("recommissionRegionServer"); // TODO
  }

  @Override
  public void restoreSnapshot(String arg0, boolean arg1, boolean arg2)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  @Override
  public Future<Void> restoreSnapshotAsync(String arg0)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshotAsync"); // TODO
  }

  @Override
  public int runCatalogJanitor() throws IOException {
    throw new UnsupportedOperationException("runCatalogJanitor"); // TODO
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    throw new UnsupportedOperationException("runCleanerChore"); // TODO
  }

  @Override
  public void setQuota(QuotaSettings arg0) throws IOException {
    throw new UnsupportedOperationException("setQuota"); // TODO
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] arg0, byte[] arg1) throws IOException {
    throw new UnsupportedOperationException("splitRegionAsync"); // TODO
  }

  @Override
  public void addReplicationPeer(String arg0, ReplicationPeerConfig arg1, boolean arg2)
      throws IOException {
    throw new UnsupportedOperationException("addReplicationPeer"); // TODO
  }

  @Override
  public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig) {
    throw new UnsupportedOperationException("addReplicationPeerAsync"); // TODO
  }

  @Override
  public Future<Void> addReplicationPeerAsync(
      String s, ReplicationPeerConfig replicationPeerConfig, boolean b) {
    throw new UnsupportedOperationException("addReplicationPeerAsync"); // TODO
  }

  @Override
  public void appendReplicationPeerTableCFs(String arg0, Map<TableName, List<String>> arg1)
      throws ReplicationException, IOException {
    throw new UnsupportedOperationException("appendReplicationPeerTableCFs"); // TODO
  }

  @Override
  public CacheEvictionStats clearBlockCache(TableName arg0) throws IOException {
    throw new UnsupportedOperationException("clearBlockCache"); // TODOv
  }

  @Override
  public void compactRegionServer(ServerName arg0) throws IOException {
    throw new UnsupportedOperationException("splitRegionAsync"); // TODO
  }

  @Override
  public void disableReplicationPeer(String arg0) throws IOException {
    throw new UnsupportedOperationException("disableReplicationPeer"); // TODO
  }

  @Override
  public void enableReplicationPeer(String arg0) throws IOException {
    throw new UnsupportedOperationException("enableReplicationPeer"); // TODO
  }

  @Override
  public ClusterMetrics getClusterMetrics(EnumSet<Option> arg0) throws IOException {
    return getClusterStatus(); // TODO
  }

  @Override
  public List<QuotaSettings> getQuota(QuotaFilter arg0) throws IOException {
    throw new UnsupportedOperationException("getQuota"); // TODO
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName arg0, TableName arg1) throws IOException {
    throw new UnsupportedOperationException("getRegionMetrics"); // TODO
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String arg0) throws IOException {
    throw new UnsupportedOperationException("getReplicationPeerConfig"); // TODO
  }

  @Override
  public boolean isMergeEnabled() throws IOException {
    throw new UnsupportedOperationException("isMergeEnabled"); // TODO
  }

  @Override
  public boolean isSplitEnabled() throws IOException {
    throw new UnsupportedOperationException("isSplitEnabled"); // TODO
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(Pattern arg0) throws IOException {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  @Override
  public void majorCompactRegionServer(ServerName arg0) throws IOException {
    throw new UnsupportedOperationException("majorCompactRegionServer"); // TODO
  }

  @Override
  public void move(byte[] encodedRegionName) throws IOException {
    throw new UnsupportedOperationException("move");
  }

  @Override
  public void move(byte[] encodedRegionName, ServerName destServerName) throws IOException {
    throw new UnsupportedOperationException("move");
  }

  @Override
  public boolean mergeSwitch(boolean arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("mergeSwitch"); // TODO
  }

  @Override
  public void removeReplicationPeer(String arg0) throws IOException {
    throw new UnsupportedOperationException("removeReplicationPeer"); // TODO
  }

  @Override
  public void removeReplicationPeerTableCFs(String arg0, Map<TableName, List<String>> arg1)
      throws ReplicationException, IOException {
    throw new UnsupportedOperationException("removeReplicationPeerTableCFs"); // TODO
  }

  @Override
  public Future<Void> removeReplicationPeerAsync(String s) {
    throw new UnsupportedOperationException("removeReplicationPeerAsync"); // TODO
  }

  @Override
  public boolean splitSwitch(boolean arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("splitSwitch"); // TODO
  }

  @Override
  public void updateReplicationPeerConfig(String arg0, ReplicationPeerConfig arg1)
      throws IOException {
    throw new UnsupportedOperationException("updateReplicationPeerConfig"); // TODO
  }

  @Override
  public Future<Void> updateReplicationPeerConfigAsync(
      String s, ReplicationPeerConfig replicationPeerConfig) {
    throw new UnsupportedOperationException("updateReplicationPeerConfigAsync"); // TODO
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return new ClusterStatus(
        "hbaseVersion",
        "clusterid",
        new HashMap(),
        new ArrayList(),
        null,
        new ArrayList(),
        new ArrayList(),
        new String[0],
        false,
        -1);
  }
}
