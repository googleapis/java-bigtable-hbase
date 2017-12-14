/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.CreateTableRequest.Split;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest.Builder;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.ColumnDescriptorAdapter2x;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.TableAdapter2x;
import com.google.protobuf.ByteString;
import io.grpc.Status;

/**
 * Bigtable implementation of {@link AsyncAdmin}
 * 
 * @author spollapally
 */
public class BigtableAsyncAdmin implements AsyncAdmin {
  private final Logger LOG = new Logger(getClass());

  private final Set<TableName> disabledTables;
  private final BigtableOptions options;
  private final BigtableTableAdminClient bigtableTableAdminClient;

  private BigtableInstanceName bigtableInstanceName;
  private final ColumnDescriptorAdapter2x columnDescriptorAdapter = new ColumnDescriptorAdapter2x();
  private final TableAdapter2x tableAdapter;

  public BigtableAsyncAdmin(BigtableAsyncConnection asyncConnection) throws IOException {
    LOG.debug("Creating BigtableAsyncAdmin");
    this.options = asyncConnection.getOptions();
    this.bigtableTableAdminClient = asyncConnection.getSession().getTableAdminClient();
    this.disabledTables = asyncConnection.getDisabledTables();
    this.bigtableInstanceName = options.getInstanceName();
    this.tableAdapter = new TableAdapter2x(options, columnDescriptorAdapter);
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, Optional<byte[][]> splitKeys) {
    if (desc.getTableName() == null) {
      return FutureUtils.failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }

    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setParent(bigtableInstanceName.toString());
    builder.setTableId(desc.getTableName().getQualifierAsString());
    builder.setTable(tableAdapter.adapt(desc));
    if (splitKeys.isPresent()) {
      for (byte[] splitKey : splitKeys.get()) {
        builder.addInitialSplits(Split.newBuilder().setKey(ByteString.copyFrom(splitKey)).build());
      }
    }

    CompletableFuture<Void> asyncFuture = new CompletableFuture<>();
    FutureUtils.toCompletableFuture(bigtableTableAdminClient.createTableAsync(builder.build()))
        .whenComplete((r, e) -> {
          if (e != null) {
            asyncFuture.completeExceptionally(e);
            return;
          }
          asyncFuture.complete(null);
        });
    return asyncFuture;
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      return createTable(desc, Optional.ofNullable(new byte[][] {startKey, endKey}));
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    return createTable(desc, Optional.ofNullable(splitKeys));
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    tableExists(tableName).whenComplete((exists, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (!exists) {
        future.completeExceptionally(new TableNotFoundException(tableName));
      } else {
        isTableDisabled(tableName).whenComplete((tableDisabled, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else if (!tableDisabled) {
            future.completeExceptionally(new TableNotEnabledException(tableName));
          } else {
            disabledTables.add(tableName);
          }
        });
      }
      future.complete(null);
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    Builder deleteBuilder = DeleteTableRequest.newBuilder();
    deleteBuilder.setName(bigtableInstanceName.toTableNameStr(tableName.getNameAsString()));
    CompletableFuture<Void> asyncFuture = new CompletableFuture<>();
    FutureUtils
        .toCompletableFuture(bigtableTableAdminClient.deleteTableAsync(deleteBuilder.build()))
        .whenComplete((r, e) -> {
          if (e != null) {
            asyncFuture.completeExceptionally(e);
            return;
          }
          disabledTables.remove(tableName);
          asyncFuture.complete(null);
        });
    return asyncFuture;
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    Objects.requireNonNull(tableName, "tableName cannot be null");
    CompletableFuture<Boolean> asyncFuture = new CompletableFuture<>();
    listTableNames(Optional.of(Pattern.compile(tableName.getNameAsString())), false)
        .whenComplete((r, e) -> {
          if (e != null) {
            asyncFuture.completeExceptionally(e);
          } else {
            for (TableName existingTableName : r) {
              if (existingTableName.equals(tableName)) {
                asyncFuture.complete(true);
                return;
              }
            }
          }
          asyncFuture.complete(false);
        });
    return asyncFuture;
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> tableNamePattern,
      boolean includeSysTables) {
    CompletableFuture<List<TableName>> asyncFuture = new CompletableFuture<>();
    requestTableList().whenComplete((r, e) -> {
      List<TableName> result = new ArrayList<>();
      if (e != null) {
        asyncFuture.completeExceptionally(e);
      } else {
        tableNamePattern.ifPresent((p) -> {
          for (Table table : r) {
            String tableName = bigtableInstanceName.toTableId(table.getName());
            if (tableNamePattern.get().matcher(tableName).matches()) {
              result.add(TableName.valueOf(tableName));
            }
          }
        });
      }
      asyncFuture.complete(result);
    });
    return asyncFuture;
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> tableNamePattern,
      boolean includeSysTables) {
    CompletableFuture<List<TableDescriptor>> asyncFuture = new CompletableFuture<>();
    requestTableList().whenComplete((r, e) -> {
      List<TableDescriptor> result = new ArrayList<>();
      if (e != null) {
        asyncFuture.completeExceptionally(e);
      } else {
        tableNamePattern.ifPresent((p) -> {
          for (Table table : r) {
            try {
              result.add(tableAdapter.adapt(table));
            } catch (Throwable throwable) {
              if (Status.fromThrowable(throwable).getCode() == Status.Code.NOT_FOUND) {
                asyncFuture.completeExceptionally(new TableNotFoundException(table.getName()));
              }
              asyncFuture.completeExceptionally(new IOException(
                  "Failed to getTableDescriptor() for" + table.getName(), throwable));
            }
          }
        });
      }
      asyncFuture.complete(result);
    });
    return asyncFuture;
  }

  private CompletableFuture<List<Table>> requestTableList() {
    ListTablesRequest.Builder builder = ListTablesRequest.newBuilder();
    builder.setParent(bigtableInstanceName.toString());
    CompletableFuture<List<Table>> future = new CompletableFuture<>();
    FutureUtils.toCompletableFuture(bigtableTableAdminClient.listTablesAsync((builder.build())))
        .whenComplete((r, e) -> {
          if (e != null) {
            future.completeExceptionally(e);
          }
          future.complete(r.getTablesList());
        });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    return CompletableFuture.completedFuture(disabledTables.contains(tableName));
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    return CompletableFuture.completedFuture(!disabledTables.contains(tableName));
  }

  // -------****//

  @Override
  public CompletableFuture<Boolean> abortProcedure(long arg0, boolean arg1) {
    throw new UnsupportedOperationException("abortProcedure"); // TODO
  }

  @Override
  public CompletableFuture<Void> addColumnFamily(TableName arg0, ColumnFamilyDescriptor arg1) {
    throw new UnsupportedOperationException("addColumnFamily"); // TODO
  }

  @Override
  public CompletableFuture<Void> addReplicationPeer(String arg0, ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("addReplicationPeer"); // TODO

  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(String arg0,
      Map<TableName, ? extends Collection<String>> arg1) {
    throw new UnsupportedOperationException("appendReplicationPeerTableCFs"); // TODO
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
  public CompletableFuture<Void> cloneSnapshot(String arg0, TableName arg1) {
    throw new UnsupportedOperationException("cloneSnapshot"); // TODO
  }

  @Override
  public CompletableFuture<Void> compact(TableName arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("compact"); // TODO

  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("compactRegion"); // TODO
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("compactRegionServer"); // TODO
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> arg0,
      CoprocessorCallable<S, R> arg1) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> arg0,
      CoprocessorCallable<S, R> arg1, ServerName arg2) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
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
  public CompletableFuture<Void> deleteColumnFamily(TableName arg0, byte[] arg1) {
    throw new UnsupportedOperationException("deleteColumnFamily"); // TODO
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String arg0) {
    throw new UnsupportedOperationException("deleteNamespace"); // TODO
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String arg0) {
    throw new UnsupportedOperationException("deleteSnapshot"); // TODO

  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern arg0, Pattern arg1) {
    throw new UnsupportedOperationException("deleteTableSnapshots"); // TODO
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
  public CompletableFuture<Void> enableTable(TableName arg0) {
    throw new UnsupportedOperationException("enableTable"); // TODO
  }

  @Override
  public CompletableFuture<Void> execProcedure(String arg0, String arg1, Map<String, String> arg2) {
    throw new UnsupportedOperationException("execProcedure"); // TODO
  }

  @Override
  public CompletableFuture<byte[]> execProcedureWithRet(String arg0, String arg1,
      Map<String, String> arg2) {
    throw new UnsupportedOperationException("execProcedureWithRet"); // TODO
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
  public CompletableFuture<ClusterStatus> getClusterStatus() {
    throw new UnsupportedOperationException("getClusterStatus"); // TODO
  }

  @Override
  public CompletableFuture<ClusterStatus> getClusterStatus(EnumSet<Option> arg0) {
    throw new UnsupportedOperationException("getClusterStatus"); // TODO
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
  public CompletableFuture<List<RegionInfo>> getOnlineRegions(ServerName arg0) {
    throw new UnsupportedOperationException("getOnlineRegions"); // TODO
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
  public CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName arg0,
      Optional<TableName> arg1) {
    throw new UnsupportedOperationException("getRegionLoads"); // TODO
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
  public CompletableFuture<TableDescriptor> getTableDescriptor(TableName arg0) {
    throw new UnsupportedOperationException("getTableDescriptor"); // TODO
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getTableRegions(TableName arg0) {
    throw new UnsupportedOperationException("getTableRegions"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isBalancerOn() {
    throw new UnsupportedOperationException("isBalancerOn"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorOn() {
    throw new UnsupportedOperationException("isCatalogJanitorOn"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreOn() {
    throw new UnsupportedOperationException("isCleanerChoreOn"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    throw new UnsupportedOperationException("isMasterInMaintenanceMode"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isMergeOn() {
    throw new UnsupportedOperationException("isMergeOn"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerOn() {
    throw new UnsupportedOperationException("isNormalizerOn"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isProcedureFinished(String arg0, String arg1,
      Map<String, String> arg2) {
    throw new UnsupportedOperationException("isProcedureFinished"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription arg0) {
    throw new UnsupportedOperationException("isSnapshotFinished"); // TODO

  }

  @Override
  public CompletableFuture<Boolean> isSplitOn() {
    throw new UnsupportedOperationException("isSplitOn"); // TODO

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
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(
      Optional<Pattern> arg0) {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Optional<Pattern> arg0) {
    throw new UnsupportedOperationException("listSnapshots"); // TODO
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern arg0,
      Pattern arg1) {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("majorCompact"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("majorCompactRegion"); // TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("majorCompactRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] arg0, byte[] arg1, boolean arg2) {
    throw new UnsupportedOperationException("mergeRegions"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName arg0, ColumnFamilyDescriptor arg1) {
    throw new UnsupportedOperationException("modifyColumnFamily"); // TODO impl
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor arg0) {
    throw new UnsupportedOperationException("modifyNamespace"); // TODO
  }

  @Override
  public CompletableFuture<Void> move(byte[] arg0, Optional<ServerName> arg1) {
    throw new UnsupportedOperationException("move"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> normalize() {
    throw new UnsupportedOperationException("normalize"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> offline(byte[] arg0) {
    throw new UnsupportedOperationException("offline"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> recommissionRegionServer(ServerName arg0, List<byte[]> arg1) {
    throw new UnsupportedOperationException("recommissionRegionServer"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("removeReplicationPeer"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String arg0,
      Map<TableName, ? extends Collection<String>> arg1) {
    throw new UnsupportedOperationException("removeReplicationPeerTableCFs"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String arg0) {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String arg0, boolean arg1) {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName arg0) {
    throw new UnsupportedOperationException("rollWALWriter"); // TODO ?
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    throw new UnsupportedOperationException("runCatalogJanitor"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    throw new UnsupportedOperationException("runCleanerChore"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setBalancerOn(boolean arg0) {
    throw new UnsupportedOperationException("setBalancerOn"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setCatalogJanitorOn(boolean arg0) {
    throw new UnsupportedOperationException("setCatalogJanitorOn"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setCleanerChoreOn(boolean arg0) {
    throw new UnsupportedOperationException("abosetCleanerChoreOnrtProcedure"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setMergeOn(boolean arg0) {
    throw new UnsupportedOperationException("setMergeOn"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setNormalizerOn(boolean arg0) {
    throw new UnsupportedOperationException("setNormalizerOn"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings arg0) {
    throw new UnsupportedOperationException("setQuota"); // TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setSplitOn(boolean arg0) {
    throw new UnsupportedOperationException("setSplitOn"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    throw new UnsupportedOperationException("shutdown"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription arg0) {
    throw new UnsupportedOperationException("snapshot"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> split(TableName arg0) {
    throw new UnsupportedOperationException("split"); // TODO impl
  }

  @Override
  public CompletableFuture<Void> split(TableName arg0, byte[] arg1) {
    throw new UnsupportedOperationException("split"); // TODO impl
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("splitRegion"); // TODO impl
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    throw new UnsupportedOperationException("stopMaster"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("stopRegionServer"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName arg0, boolean arg1) {
    throw new UnsupportedOperationException("truncateTable"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] arg0, boolean arg1) {
    throw new UnsupportedOperationException("unassign"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> updateConfiguration() {
    throw new UnsupportedOperationException("updateConfiguration"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> updateConfiguration(ServerName arg0) {
    throw new UnsupportedOperationException("updateConfiguration"); // TODO ?
  }

  @Override
  public CompletableFuture<Void> updateReplicationPeerConfig(String arg0,
      ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("updateReplicationPeerConfig");
  }

}
