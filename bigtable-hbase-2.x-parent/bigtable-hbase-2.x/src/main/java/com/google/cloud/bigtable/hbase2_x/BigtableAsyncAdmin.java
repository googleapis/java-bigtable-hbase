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

import static com.google.cloud.bigtable.hbase2_x.ApiFutureUtils.failedFuture;
import static com.google.cloud.bigtable.hbase2_x.ApiFutureUtils.toCompletableFuture;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.Cluster;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.util.ModifyTableBuilder;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.TableAdapter2x;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CommonConnection;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.threeten.bp.Instant;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * Bigtable implementation of {@link AsyncAdmin}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAsyncAdmin implements AsyncAdmin {
  private final Logger LOG = new Logger(getClass());

  private final Set<TableName> disabledTables;
  private final AdminClientWrapper bigtableTableAdminClient;
  private final BigtableHBaseSettings settings;
  private final CommonConnection asyncConnection;
  private final String bigtableInstanceName;
  private BigtableClusterName bigtableSnapshotClusterName;
  private final int ttlSeconds;

  public BigtableAsyncAdmin(CommonConnection asyncConnection) throws IOException {
    LOG.debug("Creating BigtableAsyncAdmin");
    this.settings = asyncConnection.getBigtableSettings();
    this.bigtableTableAdminClient = asyncConnection.getBigtableApi().getAdminClient();
    this.bigtableInstanceName = asyncConnection.getBigtableSettings().getInstanceId();
    this.disabledTables = asyncConnection.getDisabledTables();
    this.asyncConnection = asyncConnection;

    Configuration configuration = asyncConnection.getConfiguration();
    String clusterId =
        configuration.get(BigtableOptionsFactory.BIGTABLE_SNAPSHOT_CLUSTER_ID_KEY, null);
    if (clusterId != null) {
      BigtableInstanceName bigtableInstanceName =
          new BigtableInstanceName(
              asyncConnection.getBigtableSettings().getProjectId(),
              asyncConnection.getBigtableSettings().getInstanceId());
      bigtableSnapshotClusterName = bigtableInstanceName.toClusterName(clusterId);
    }
    this.ttlSeconds =
        configuration.getInt(
            BigtableOptionsFactory.BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY,
            BigtableOptionsFactory.BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_VALUE);

    if (this.ttlSeconds <= 0) {
      throw new IllegalArgumentException(
          BigtableOptionsFactory.BIGTABLE_SNAPSHOT_DEFAULT_TTL_SECS_KEY + " must be > 0");
    }
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys) {
    // wraps exceptions in a CF (CompletableFuture). No null check here on desc to match Hbase impl
    if (desc.getTableName() == null) {
      return failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }

    CreateTableRequest request = TableAdapter2x.adapt(desc, splitKeys);
    return toCompletableFuture(bigtableTableAdminClient.createTableAsync(request))
        .handle(
            (resp, ex) -> {
              if (ex != null) {
                throw new CompletionException(
                    AbstractBigtableAdmin.convertToTableExistsException(desc.getTableName(), ex));
              }
              return null;
            });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> createTable(
      TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) {
    return CompletableFuture.supplyAsync(
            () -> AbstractBigtableAdmin.createSplitKeys(startKey, endKey, numRegions))
        .thenCompose(keys -> createTable(desc, keys));
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#createTable(org.apache.hadoop.hbase.client.TableDescriptor)
   */
  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc) {
    return createTable(desc, null);
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    return tableExists(tableName)
        .thenApply(
            exists -> {
              if (!exists) {
                throw new CompletionException(new TableNotFoundException(tableName));
              } else if (disabledTables.contains(tableName)) {
                throw new CompletionException(new TableNotEnabledException(tableName));
              } else {
                disabledTables.add(tableName);
                LOG.warn("Table " + tableName + " was disabled in memory only.");
                return null;
              }
            });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> enableTable(TableName tableName) {
    return tableExists(tableName)
        .thenApply(
            exists -> {
              if (!exists) {
                throw new CompletionException(new TableNotFoundException(tableName));
              } else if (!disabledTables.contains(tableName)) {
                throw new CompletionException(new TableNotDisabledException(tableName));
              } else {
                disabledTables.remove(tableName);
                LOG.warn("Table " + tableName + " was enabled in memory only.");
                return null;
              }
            });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return toCompletableFuture(
            bigtableTableAdminClient.deleteTableAsync(tableName.getNameAsString()))
        .thenAccept(r -> disabledTables.remove(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return listTableNames(Optional.of(Pattern.compile(tableName.getNameAsString())))
        .thenApply(r -> r.stream().anyMatch(e -> e.equals(tableName)));
  }

  private CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> tableNamePattern) {
    return toCompletableFuture(bigtableTableAdminClient.listTablesAsync())
        .thenApply(
            r ->
                r.stream()
                    .filter(
                        e ->
                            !tableNamePattern.isPresent()
                                || tableNamePattern.get().matcher(e).matches())
                    .map(TableName::valueOf)
                    .collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableName>> listTableNames(boolean includeSysTables) {
    return listTableNames(Optional.empty());
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableName>> listTableNames(
      Pattern tableNamePattern, boolean includeSysTables) {
    return listTableNames(Optional.of(tableNamePattern));
  }

  private CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> tableNamePattern) {
    // TODO: returns table name as descriptor, Refactor it to return full descriptors.
    return toCompletableFuture(bigtableTableAdminClient.listTablesAsync())
        .thenApply(
            r ->
                r.stream()
                    .filter(
                        t ->
                            !tableNamePattern.isPresent()
                                || tableNamePattern.get().matcher(t).matches())
                    .map(
                        m ->
                            com.google.bigtable.admin.v2.Table.newBuilder()
                                .setName(
                                    NameUtil.formatTableName(
                                        settings.getProjectId(), settings.getInstanceId(), m))
                                .build())
                    .map(Table::fromProto)
                    .map(TableAdapter2x::adapt)
                    .collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(boolean includeSysTables) {
    return listTables(Optional.empty());
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(
      Pattern pattern, boolean includeSysTables) {
    return listTables(Optional.of(pattern));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(List<TableName> tableNames) {
    Preconditions.checkNotNull(tableNames, "tableNames is null");
    if (tableNames.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    return toCompletableFuture(bigtableTableAdminClient.listTablesAsync())
        .thenApply(
            t ->
                tableNames.stream()
                    .filter(inputTableName -> t.contains(inputTableName.getNameAsString()))
                    .map(
                        tbName -> {
                          try {
                            return getDescriptor(tbName).join();
                          } catch (CompletionException ex) {
                            if (ex.getCause() instanceof TableNotFoundException) {
                              // If table not found then remove it from the list.
                              return null;
                            }
                            throw ex;
                          }
                        })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    // TODO: this might require a tableExists() check, and throw an exception if it doesn't.
    return CompletableFuture.completedFuture(disabledTables.contains(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    // TODO: this might require a tableExists() check, and throw an exception if it doesn't.
    return CompletableFuture.completedFuture(!disabledTables.contains(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<TableDescriptor> getDescriptor(TableName tableName) {
    if (tableName == null) {
      return CompletableFuture.completedFuture(null);
    }

    return toCompletableFuture(bigtableTableAdminClient.getTableAsync(tableName.getNameAsString()))
        .handle(
            (resp, ex) -> {
              if (ex != null) {
                if (Status.fromThrowable(ex).getCode() == Status.Code.NOT_FOUND) {
                  throw new CompletionException(new TableNotFoundException(tableName));
                } else {
                  throw new CompletionException(ex);
                }
              } else {
                return TableAdapter2x.adapt(resp);
              }
            });
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String snapshotId) {
    return toCompletableFuture(
        bigtableTableAdminClient.deleteBackupAsync(
            getBackupClusterName().getClusterId(), snapshotId));
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(
      Pattern tableNamePattern, Pattern snapshotNamePattern) {
    throw new UnsupportedOperationException("Unsupported - please use deleteSnapshots");
  }

  //  ******************* START COLUMN FAMILY MODIFICATION  ************************

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> addColumnFamily(
      TableName tableName, ColumnFamilyDescriptor columnFamilyDesc) {
    return modifyColumns(
        ModifyTableBuilder.newBuilder(tableName)
            .add(TableAdapter2x.toHColumnDescriptor(columnFamilyDesc)));
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName tableName, byte[] columnName) {
    return modifyColumns(
        ModifyTableBuilder.newBuilder(tableName).delete(Bytes.toString(columnName)));
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(
      TableName tableName, ColumnFamilyDescriptor columnFamilyDesc) {
    return modifyColumns(
        ModifyTableBuilder.newBuilder(tableName)
            .modify(TableAdapter2x.toHColumnDescriptor(columnFamilyDesc)));
  }

  @Override
  public CompletableFuture<Void> modifyTable(TableDescriptor newDescriptor) {
    return getDescriptor(newDescriptor.getTableName())
        .thenApply(
            descriptor ->
                ModifyTableBuilder.buildModifications(
                    new HTableDescriptor(newDescriptor), new HTableDescriptor(descriptor)))
        .thenCompose(this::modifyColumns);
  }

  /**
   * modifyColumns.
   *
   * @param modifications a {@link ModifyTableBuilder} object.
   */
  private CompletableFuture<Void> modifyColumns(ModifyTableBuilder modifications) {
    return toCompletableFuture(bigtableTableAdminClient.modifyFamiliesAsync(modifications.build()))
        .thenApply(r -> null);
  }

  //  ******************* END COLUMN FAMILY MODIFICATION  ************************

  /** In place table restorations are not supported. Please use cloneSnapshot */
  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
    throw new UnsupportedOperationException(
        "In place table restorations are not supported. Please use cloneSnapshot");
  }

  /** In place table restorations are not supported. Please use cloneSnapshot */
  @Override
  public CompletableFuture<Void> restoreSnapshot(
      String snapshotName, boolean takeFailSafeSnapshot) {
    throw new UnsupportedOperationException(
        "In place table restorations are not supported. Please use cloneSnapshot");
  }

  /** Same table restorations are not supported. Please use cloneSnapshot */
  @Override
  public CompletableFuture<Void> restoreSnapshot(
      String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl) {
    throw new UnsupportedOperationException(
        "In place table restorations are not supported. Please use cloneSnapshot");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#truncateTable(org.apache.hadoop.hbase.TableName, boolean)
   */
  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
    if (!preserveSplits) {
      LOG.info("truncate will preserveSplits. The passed in variable is ignored.");
    }

    return toCompletableFuture(
        bigtableTableAdminClient.dropAllRowsAsync(tableName.getNameAsString()));
  }

  /*
   * deleteSnapshots is not supported due to backup write quota limits - please use deleteSnapshot
   */
  @Override
  public CompletableFuture<Void> deleteSnapshots() {
    throw new UnsupportedOperationException("use deleteSnapshot instead");
  }

  /*
   * deleteSnapshots is not supported due to backup write quota limits - please use deleteSnapshot
   */
  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern pattern) {
    throw new UnsupportedOperationException("use deleteSnapshot instead");
  }

  /*
   * deleteSnapshots is not supported due to backup write quota limits - please use deleteSnapshot
   */
  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern) {
    throw new UnsupportedOperationException("use deleteSnapshot instead");
  }

  @Override
  public CompletableFuture<Void> snapshot(String snapshotId, TableName tableName) {
    Instant expireTime = Instant.now().plus(ttlSeconds, ChronoUnit.SECONDS);
    if (Strings.isNullOrEmpty(snapshotId)) {
      throw new IllegalArgumentException("Snapshot name cannot be null");
    }
    if (Strings.isNullOrEmpty(tableName.getNameAsString())) {
      throw new IllegalArgumentException("Table name cannot be null");
    }

    return toCompletableFuture(
            bigtableTableAdminClient.createBackupAsync(
                CreateBackupRequest.of(getBackupClusterName().getClusterId(), snapshotId)
                    .setExpireTime(expireTime)
                    .setSourceTableId(tableName.getNameAsString())))
        .thenAccept(backup -> {});
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotId, TableName tableName) {
    return toCompletableFuture(
            bigtableTableAdminClient.restoreTableAsync(
                RestoreTableRequest.of(getBackupClusterName().getClusterId(), snapshotId)
                    .setTableId(tableName.getNameAsString())))
        .thenAccept(backup -> {});
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(
      String snapshotName, TableName tableName, boolean restoreAcl) {
    throw new UnsupportedOperationException("cloneSnapshot");
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    return CompletableFuture.supplyAsync(() -> getBackupClusterName().getClusterId())
        .thenCompose(
            c ->
                toCompletableFuture(bigtableTableAdminClient.listBackupsAsync(c))
                    .thenApply(
                        r ->
                            r.stream()
                                .map(b -> new SnapshotDescription(b))
                                .collect(Collectors.toList())));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern pattern) {
    Preconditions.checkNotNull(pattern);
    if (pattern.matcher("").matches()) {
      return CompletableFuture.completedFuture(ImmutableList.of());
    }
    return listSnapshots().thenApply(r -> filter(r, d -> pattern.matcher(d.getName()).matches()));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(
      Pattern tableNamePattern, Pattern snapshotPattern) {
    throw new UnsupportedOperationException("Unsupported - please use listSnapshots");
  }

  private static <T> List<T> filter(Collection<T> r, Predicate<T> predicate) {
    return r.stream().filter(predicate).collect(Collectors.toList());
  }

  private synchronized BigtableClusterName getBackupClusterName() {
    if (this.bigtableSnapshotClusterName == null) {
      List<Cluster> clusters =
          asyncConnection
              .getBigtableApi()
              .getAdminClient()
              .listClusters(asyncConnection.getBigtableSettings().getInstanceId());
      Preconditions.checkState(
          clusters.size() == 1,
          String.format(
              "Project '%s' / Instance '%s' has %d clusters. There must be exactly 1 for this operation to work.",
              asyncConnection.getBigtableSettings().getProjectId(),
              asyncConnection.getBigtableSettings().getInstanceId(),
              clusters.size()));
      String clusterName =
          NameUtil.formatClusterName(
              asyncConnection.getBigtableSettings().getProjectId(),
              asyncConnection.getBigtableSettings().getInstanceId(),
              clusters.get(0).getId());
      bigtableSnapshotClusterName = new BigtableClusterName(clusterName);
    }
    return bigtableSnapshotClusterName;
  }

  // ****** TO BE IMPLEMENTED [start] ******

  @Override
  public CompletableFuture<Void> cloneTableSchema(
      TableName tableName, TableName tableName1, boolean preserveSplits) {
    throw new UnsupportedOperationException("cloneTableSchema"); // TODO
  }

  @Override
  public CompletableFuture<Map<ServerName, Boolean>> compactionSwitch(
      boolean switchState, List<String> serverNamesList) {
    throw new UnsupportedOperationException("compactionSwitch");
  }

  @Override
  public CompletableFuture<Boolean> switchRpcThrottle(boolean enable) {
    throw new UnsupportedOperationException("switchRpcThrottle");
  }

  @Override
  public CompletableFuture<Boolean> isRpcThrottleEnabled() {
    throw new UnsupportedOperationException("isRpcThrottleEnabled");
  }

  @Override
  public CompletableFuture<Boolean> exceedThrottleQuotaSwitch(boolean enable) {
    throw new UnsupportedOperationException("exceedThrottleQuotaSwitch");
  }

  @Override
  public CompletableFuture<Map<TableName, Long>> getSpaceQuotaTableSizes() {
    throw new UnsupportedOperationException("getSpaceQuotaTableSizes");
  }

  @Override
  public CompletableFuture<? extends Map<TableName, ? extends SpaceQuotaSnapshotView>>
      getRegionServerSpaceQuotaSnapshots(ServerName serverName) {
    throw new UnsupportedOperationException("getRegionServerSpaceQuotaSnapshots");
  }

  @Override
  public CompletableFuture<? extends SpaceQuotaSnapshotView> getCurrentSpaceQuotaSnapshot(
      String namespace) {
    throw new UnsupportedOperationException("getCurrentSpaceQuotaSnapshot");
  }

  @Override
  public CompletableFuture<? extends SpaceQuotaSnapshotView> getCurrentSpaceQuotaSnapshot(
      TableName tableName) {
    throw new UnsupportedOperationException("getCurrentSpaceQuotaSnapshot");
  }

  @Override
  public CompletableFuture<Void> grant(
      UserPermission userPermission, boolean mergeExistingPermissions) {
    throw new UnsupportedOperationException("grant");
  }

  @Override
  public CompletableFuture<Void> revoke(UserPermission userPermission) {
    throw new UnsupportedOperationException("revoke");
  }

  @Override
  public CompletableFuture<List<UserPermission>> getUserPermissions(
      GetUserPermissionsRequest getUserPermissionsRequest) {
    throw new UnsupportedOperationException("getUserPermissions");
  }

  @Override
  public CompletableFuture<List<Boolean>> hasUserPermissions(
      String userName, List<Permission> permissions) {
    throw new UnsupportedOperationException("hasUserPermissions");
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern) {
    throw new UnsupportedOperationException("Unsupported - please use listSnapshots");
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    return tableExists(tableName)
        .handle(
            (exists, ex) -> {
              if (ex != null) {
                throw new CompletionException(ex);
              } else if (!exists) {
                throw new CompletionException(new TableNotFoundException(tableName));
              } else {
                return true;
              }
            });
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(TableName tableName) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return new CopyOnWriteArrayList<RegionInfo>(
                asyncConnection.getAllRegionInfos(tableName));
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        });
  }

  // ****** TO BE IMPLEMENTED [end] ******

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> abortProcedure(long arg0, boolean arg1) {
    throw new UnsupportedOperationException("abortProcedure"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> addReplicationPeer(String arg0, ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("addReplicationPeer"); // TODO
  }

  @Override
  public CompletableFuture<Void> assign(byte[] arg0) {
    throw new UnsupportedOperationException("assign"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> balance(boolean arg0) {
    throw new UnsupportedOperationException("balance"); // TODO
  }

  @Override
  public CompletableFuture<Void> clearCompactionQueues(ServerName arg0, Set<String> arg1) {
    throw new UnsupportedOperationException("clearCompactionQueues"); // TODO
  }

  @Override
  public CompletableFuture<List<ServerName>> clearDeadServers(List<ServerName> arg0) {
    throw new UnsupportedOperationException("clearDeadServers"); // TODO
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("compactRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<Void> createNamespace(NamespaceDescriptor arg0) {
    throw new UnsupportedOperationException("createNamespace"); // TODO
  }

  @Override
  public CompletableFuture<Void> decommissionRegionServers(List<ServerName> arg0, boolean arg1) {
    throw new UnsupportedOperationException("decommissionRegionServers"); // TODO
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String arg0) {
    throw new UnsupportedOperationException("deleteNamespace"); // TODO
  }

  @Override
  public CompletableFuture<Void> disableReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("disableReplicationPeer"); // TODO
  }

  @Override
  public CompletableFuture<Void> enableReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("enableReplicationPeer"); // TODO
  }

  @Override
  public CompletableFuture<Void> execProcedure(String arg0, String arg1, Map<String, String> arg2) {
    throw new UnsupportedOperationException("execProcedure"); // TODO
  }

  @Override
  public CompletableFuture<Void> flush(TableName arg0) {
    throw new UnsupportedOperationException("flush"); // TODO
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] arg0) {
    throw new UnsupportedOperationException("flushRegion"); // TODO
  }

  @Override
  public CompletableFuture<Void> flushRegionServer(ServerName serverName) {
    throw new UnsupportedOperationException("flushRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName arg0) {
    throw new UnsupportedOperationException("getCompactionState"); // TODO
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] arg0) {
    throw new UnsupportedOperationException("getCompactionStateForRegion"); // TODO
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestamp(TableName arg0) {
    throw new UnsupportedOperationException("getLastMajorCompactionTimestamp"); // TODO
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestampForRegion(byte[] arg0) {
    throw new UnsupportedOperationException("getLastMajorCompactionTimestampForRegion"); // TODO
  }

  @Override
  public CompletableFuture<String> getLocks() {
    throw new UnsupportedOperationException("getLocks"); // TODO
  }

  @Override
  public CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String arg0) {
    throw new UnsupportedOperationException("getNamespaceDescriptor"); // TODO
  }

  @Override
  public CompletableFuture<String> getProcedures() {
    throw new UnsupportedOperationException("getProcedures"); // TODO
  }

  @Override
  public CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter arg0) {
    throw new UnsupportedOperationException("getQuota"); // TODO
  }

  @Override
  public CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(String arg0) {
    throw new UnsupportedOperationException("getReplicationPeerConfig"); // TODO
  }

  @Override
  public CompletableFuture<List<SecurityCapability>> getSecurityCapabilities() {
    throw new UnsupportedOperationException("getSecurityCapabilities"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    throw new UnsupportedOperationException("isMasterInMaintenanceMode"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isProcedureFinished(
      String arg0, String arg1, Map<String, String> arg2) {
    throw new UnsupportedOperationException("isProcedureFinished"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription arg0) {
    throw new UnsupportedOperationException("isSnapshotFinished"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName arg0, byte[][] arg1) {
    throw new UnsupportedOperationException("isTableAvailable"); // TODO
  }

  @Override
  public CompletableFuture<List<ServerName>> listDeadServers() {
    throw new UnsupportedOperationException("listDeadServers"); // TODO
  }

  @Override
  public CompletableFuture<List<ServerName>> listDecommissionedRegionServers() {
    throw new UnsupportedOperationException("listDecommissionedRegionServers"); // TODO
  }

  @Override
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    throw new UnsupportedOperationException("abortProcedure"); // TODO
  }

  @Override
  public CompletableFuture<List<TableCFs>> listReplicatedTableCFs() {
    throw new UnsupportedOperationException("listReplicatedTableCFs"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("majorCompactRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] arg0, byte[] arg1, boolean arg2) {
    throw new UnsupportedOperationException("mergeRegions"); // TODO
  }

  @Override
  public CompletableFuture<Void> mergeRegions(List<byte[]> nameOfRegionsToMerge, boolean forcible) {
    throw new UnsupportedOperationException("mergeRegions");
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor arg0) {
    throw new UnsupportedOperationException("modifyNamespace"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> normalize() {
    throw new UnsupportedOperationException("normalize"); // TODO
  }

  @Override
  public CompletableFuture<Void> offline(byte[] arg0) {
    throw new UnsupportedOperationException("offline"); // TODO
  }

  @Override
  public CompletableFuture<Void> recommissionRegionServer(ServerName arg0, List<byte[]> arg1) {
    throw new UnsupportedOperationException("recommissionRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("removeReplicationPeer"); // TODO
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName arg0) {
    throw new UnsupportedOperationException("rollWALWriter"); // TODO
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    throw new UnsupportedOperationException("runCatalogJanitor"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    throw new UnsupportedOperationException("runCleanerChore"); // TODO
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings arg0) {
    throw new UnsupportedOperationException("setQuota"); // TODO
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    throw new UnsupportedOperationException("shutdown"); // TODO
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshot) {
    Objects.requireNonNull(snapshot);
    return snapshot(snapshot.getName(), snapshot.getTableName());
  }

  @Override
  public CompletableFuture<Void> split(TableName arg0) {
    throw new UnsupportedOperationException("split"); // TODO
  }

  @Override
  public CompletableFuture<Void> split(TableName arg0, byte[] arg1) {
    throw new UnsupportedOperationException("split"); // TODO
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    throw new UnsupportedOperationException("stopMaster"); // TODO
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("stopRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] arg0, boolean arg1) {
    throw new UnsupportedOperationException("unassign"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#updateConfiguration()
   */
  @Override
  public CompletableFuture<Void> updateConfiguration() {
    throw new UnsupportedOperationException("updateConfiguration"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#updateConfiguration(org.apache.hadoop.hbase.ServerName)
   */
  @Override
  public CompletableFuture<Void> updateConfiguration(ServerName arg0) {
    throw new UnsupportedOperationException("updateConfiguration"); // TODO
  }

  @Override
  public CompletableFuture<Void> updateReplicationPeerConfig(
      String arg0, ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("updateReplicationPeerConfig");
  }

  @Override
  public CompletableFuture<Void> addReplicationPeer(
      String arg0, ReplicationPeerConfig arg1, boolean arg2) {
    throw new UnsupportedOperationException("addReplicationPeer"); // TODO
  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(
      String arg0, Map<TableName, List<String>> arg1) {
    throw new UnsupportedOperationException("appendReplicationPeerTableCFs"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> balancerSwitch(boolean arg0) {
    throw new UnsupportedOperationException("balancerSwitch"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> balancerSwitch(boolean on, boolean drainRITs) {
    throw new UnsupportedOperationException("balancerSwitch");
  }

  @Override
  public CompletableFuture<Boolean> catalogJanitorSwitch(boolean arg0) {
    throw new UnsupportedOperationException("catalogJanitorSwitch"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> cleanerChoreSwitch(boolean arg0) {
    throw new UnsupportedOperationException("cleanerChoreSwitch"); // TODO
  }

  @Override
  public CompletableFuture<CacheEvictionStats> clearBlockCache(TableName arg0) {
    throw new UnsupportedOperationException("clearBlockCache"); // TODO
  }

  @Override
  public CompletableFuture<Void> compact(TableName arg0, CompactType arg1) {
    throw new UnsupportedOperationException("compact"); // TODO
  }

  @Override
  public CompletableFuture<Void> compact(TableName arg0, byte[] arg1, CompactType arg2) {
    throw new UnsupportedOperationException("compact"); // TODO
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] arg0) {
    throw new UnsupportedOperationException("compactRegion"); // TODO
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] arg0, byte[] arg1) {
    throw new UnsupportedOperationException("compactRegion"); // TODO
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(
      Function<RpcChannel, S> arg0, ServiceCaller<S, R> arg1) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(
      Function<RpcChannel, S> arg0, ServiceCaller<S, R> arg1, ServerName arg2) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  @Override
  public CompletableFuture<Void> disableTableReplication(TableName arg0) {
    throw new UnsupportedOperationException("disableTableReplication"); // TODO
  }

  @Override
  public CompletableFuture<Void> enableTableReplication(TableName arg0) {
    throw new UnsupportedOperationException("enableTableReplication"); // TODO
  }

  @Override
  public CompletableFuture<byte[]> execProcedureWithReturn(
      String arg0, String arg1, Map<String, String> arg2) {
    throw new UnsupportedOperationException("execProcedureWithReturn"); // TODO
  }

  @Override
  public CompletableFuture<ClusterMetrics> getClusterMetrics() {
    throw new UnsupportedOperationException("getClusterMetrics"); // TODO
  }

  @Override
  public CompletableFuture<ClusterMetrics> getClusterMetrics(
      EnumSet<org.apache.hadoop.hbase.ClusterMetrics.Option> arg0) {
    throw new UnsupportedOperationException("getClusterMetrics"); // TODO
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName arg0, CompactType arg1) {
    throw new UnsupportedOperationException("getCompactionState"); // TODO
  }

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName arg0) {
    throw new UnsupportedOperationException("getRegionMetrics"); // TODO
  }

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName arg0, TableName arg1) {
    throw new UnsupportedOperationException("getRegionMetrics"); // TODO
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(ServerName arg0) {
    throw new UnsupportedOperationException("getRegions"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isBalancerEnabled() {
    throw new UnsupportedOperationException("isBalancerEnabled"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorEnabled() {
    throw new UnsupportedOperationException("isCatalogJanitorEnabled"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreEnabled() {
    throw new UnsupportedOperationException("isCleanerChoreEnabled"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isMergeEnabled() {
    throw new UnsupportedOperationException("isMergeEnabled"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerEnabled() {
    throw new UnsupportedOperationException("isNormalizerEnabled"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isSplitEnabled() {
    throw new UnsupportedOperationException("isSplitEnabled"); // TODO
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers() {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Pattern arg0) {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptorsByNamespace(String arg0) {
    throw new UnsupportedOperationException("listTableDescriptorsByNamespace"); // TODO
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNamesByNamespace(String arg0) {
    throw new UnsupportedOperationException("listTableNamesByNamespace"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName arg0, CompactType arg1) {
    throw new UnsupportedOperationException("majorCompact"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName arg0, byte[] arg1, CompactType arg2) {
    throw new UnsupportedOperationException("majorCompact"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] arg0) {
    throw new UnsupportedOperationException("majorCompactRegion"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] arg0, byte[] arg1) {
    throw new UnsupportedOperationException("majorCompactRegion"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> mergeSwitch(boolean arg0) {
    throw new UnsupportedOperationException("mergeSwitch"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> mergeSwitch(boolean enabled, boolean drainMerges) {
    throw new UnsupportedOperationException("mergeSwitch");
  }

  @Override
  public CompletableFuture<Void> move(byte[] arg0) {
    throw new UnsupportedOperationException("move"); // TODO
  }

  @Override
  public CompletableFuture<Void> move(byte[] arg0, ServerName arg1) {
    throw new UnsupportedOperationException("move"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> normalizerSwitch(boolean arg0) {
    throw new UnsupportedOperationException("normalizerSwitch"); // TODO
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(
      String arg0, Map<TableName, List<String>> arg1) {
    throw new UnsupportedOperationException("removeReplicationPeerTableCFs"); // TODO
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] arg0) {
    throw new UnsupportedOperationException("splitRegion"); // TODO
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] arg0, byte[] arg1) {
    throw new UnsupportedOperationException("splitRegion"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> splitSwitch(boolean arg0) {
    throw new UnsupportedOperationException("splitSwitch"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> splitSwitch(boolean enabled, boolean drainSplits) {
    throw new UnsupportedOperationException("splitSwitch");
  }
}
