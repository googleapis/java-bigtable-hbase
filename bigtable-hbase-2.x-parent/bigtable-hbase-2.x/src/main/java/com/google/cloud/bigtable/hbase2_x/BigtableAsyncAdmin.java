/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
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
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.CreateTableRequest.Split;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest.Builder;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
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
  private final TableAdapter2x tableAdapter2x;

  public BigtableAsyncAdmin(BigtableAsyncConnection asyncConnection) throws IOException {
    LOG.debug("Creating BigtableAsyncAdmin");
    this.options = asyncConnection.getOptions();
    this.bigtableTableAdminClient = asyncConnection.getSession().getTableAdminClient();
    this.disabledTables = asyncConnection.getDisabledTables();
    this.bigtableInstanceName = options.getInstanceName();
    this.tableAdapter2x = new TableAdapter2x(options, new ColumnDescriptorAdapter());
  }

  private CompletableFuture<Void> createTable(TableDescriptor desc, Optional<byte[][]> splitKeys) {
    // wraps exceptions in a CF (CompletableFuture). No null check here on desc to match Hbase impl
    if (desc.getTableName() == null) {
      return FutureUtils.failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }

    // Using this pattern of wrapping inexpensive prep code is required to keep all exception
    // handing within a CF, that way clients don't have to handle them differently.
    return CompletableFuture.supplyAsync(() -> {
      CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
      builder.setParent(bigtableInstanceName.toString());
      builder.setTableId(desc.getTableName().getQualifierAsString());
      builder.setTable(tableAdapter2x.adapt(desc));
      if (splitKeys.isPresent()) {
        for (byte[] splitKey : splitKeys.get()) {
          builder
              .addInitialSplits(Split.newBuilder().setKey(ByteString.copyFrom(splitKey)).build());
        }
      }
      return builder;
    }).thenCompose(
        r -> FutureUtils.toCompletableFuture(bigtableTableAdminClient.createTableAsync(r.build())))
        .thenAccept(r -> {
        });
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys) {
    return createTable(desc, Optional.of(splitKeys));
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
                                             int numRegions) {

    return CompletableFuture.supplyAsync(() -> {
      Optional<byte[][]> splitKeys = Optional.empty();
      if (numRegions < 3) {
        throw new IllegalArgumentException("Must create at least three regions");
      } else if (Bytes.compareTo(startKey, endKey) >= 0) {
        throw new IllegalArgumentException("Start key must be smaller than end key");
      }
      if (numRegions == 3) {
        splitKeys = Optional.ofNullable(new byte[][]{startKey, endKey});
      } else {
        splitKeys = Optional.ofNullable(Bytes.split(startKey, endKey, numRegions - 3));
        if (!splitKeys.isPresent() || splitKeys.get().length != numRegions - 1) {
          throw new IllegalArgumentException("Unable to split key range into enough regions");
        }
      }
      return splitKeys;
    }).thenCompose(skeys -> createTable(desc, skeys));
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#createTable(org.apache.hadoop.hbase.client.TableDescriptor)
   */
  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc) {
    return createTable(desc, Optional.empty());
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    tableExists(tableName).thenAccept(exists -> {
      if (!exists) {
        cf.completeExceptionally(new TableNotFoundException(tableName));
      } else if (disabledTables.contains(tableName)) {
        cf.completeExceptionally(new TableNotEnabledException(tableName));
      } else {
        disabledTables.add(tableName);
        LOG.warn("Table " + tableName + " was disabled in memory only.");
        cf.complete(null);
      }
    });

    return cf;
  }

  @Override
  public CompletableFuture<Void> enableTable(TableName tableName) {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    tableExists(tableName).thenAccept(exists -> {
      if (!exists) {
        cf.completeExceptionally(new TableNotFoundException(tableName));
      } else if (!disabledTables.contains(tableName)) {
        cf.completeExceptionally(new TableNotDisabledException(tableName));
      } else {
        disabledTables.remove(tableName);
        LOG.warn("Table " + tableName + " was enabled in memory only.");
        cf.complete(null);
      }
    });

    return cf;
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return CompletableFuture.supplyAsync(() -> {
      Builder deleteBuilder = DeleteTableRequest.newBuilder();
      deleteBuilder.setName(bigtableInstanceName.toTableNameStr(tableName.getNameAsString()));
      return deleteBuilder;
    }).thenCompose(
        d -> FutureUtils.toCompletableFuture(bigtableTableAdminClient.deleteTableAsync(d.build())))
        .thenAccept(r -> {
          disabledTables.remove(tableName);
        });
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return listTableNames(Optional.of(Pattern.compile(tableName.getNameAsString())), false)
        .thenApply(r -> r.stream().anyMatch(e -> e.equals(tableName)));
  }

  private CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> tableNamePattern,
                                                            boolean includeSysTables) {
    return requestTableList().thenApply(r -> {
      Stream<TableName> result;
      if (tableNamePattern.isPresent()) {
        result = r.stream().map(e -> bigtableInstanceName.toTableId(e.getName()))
            .filter(e -> tableNamePattern.get().matcher(e).matches())
            .map(e -> TableName.valueOf(e));
      } else {
        result = r.stream().map(e -> bigtableInstanceName.toTableId(e.getName()))
            .map(e -> TableName.valueOf(e));
      }
      return result.collect(Collectors.toList());
    });
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(boolean includeSysTables) {
    return listTableNames(Optional.empty(), includeSysTables);
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(Pattern tableNamePattern, boolean includeSysTables) {
    return listTableNames(Optional.of(tableNamePattern), includeSysTables);
  }

  private CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> tableNamePattern,
                                                              boolean includeSysTables) {
    return requestTableList().thenApply(r -> {
      List<TableDescriptor> result = new ArrayList<>();
      Boolean hasNonEmptyPattern = tableNamePattern.isPresent();
      for (Table table : r) {
        String tableName = bigtableInstanceName.toTableId(table.getName());
        if (hasNonEmptyPattern) {
          if (tableNamePattern.get().matcher(tableName).matches()) {
            result.add(tableAdapter2x.adapt(table));
          }
        } else {
          result.add(tableAdapter2x.adapt(table));
        }
      }
      return result;
    });
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(boolean includeSysTables) {
    return listTables(Optional.empty(), includeSysTables);
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(Pattern pattern, boolean includeSysTables) {
    return listTables(Optional.of(pattern), includeSysTables);
  }

  private CompletableFuture<List<Table>> requestTableList() {
    return CompletableFuture.supplyAsync(() -> {
      return ListTablesRequest.newBuilder().setParent(bigtableInstanceName.toString());
    }).thenCompose(
        b -> FutureUtils.toCompletableFuture(bigtableTableAdminClient.listTablesAsync(b.build())))
        .thenApply(r -> r.getTablesList());
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    return CompletableFuture.completedFuture(disabledTables.contains(tableName));
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    return CompletableFuture.completedFuture(!disabledTables.contains(tableName));
  }

  @Override
  public CompletableFuture<TableDescriptor> getDescriptor(TableName tableName) {
    if (tableName == null) {
      return CompletableFuture.completedFuture(null);
    }

    String bigtableTableName = bigtableInstanceName.toTableNameStr(tableName.getNameAsString());
    GetTableRequest request = GetTableRequest.newBuilder().setName(bigtableTableName).build();

    return FutureUtils.toCompletableFuture(bigtableTableAdminClient.getTableAsync(request)).handle((resp, ex) -> {
      if (ex != null) {
        if (Status.fromThrowable(ex).getCode() == Status.Code.NOT_FOUND) {
          throw new CompletionException(new TableNotFoundException(tableName));
        }
      }

      return tableAdapter2x.adapt(resp);
    });
  }

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
  public CompletableFuture<Boolean> isProcedureFinished(String arg0, String arg1,
                                                        Map<String, String> arg2) {
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
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern arg0,
                                                                         Pattern arg1) {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO ?
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
  public CompletableFuture<Void> modifyColumnFamily(TableName arg0, ColumnFamilyDescriptor arg1) {
    throw new UnsupportedOperationException("modifyColumnFamily"); // TODO
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
  public CompletableFuture<Void> restoreSnapshot(String arg0) {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String arg0, boolean arg1) {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
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
  public CompletableFuture<Void> snapshot(SnapshotDescription arg0) {
    throw new UnsupportedOperationException("snapshot"); // TODO
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
  public CompletableFuture<Void> truncateTable(TableName arg0, boolean arg1) {
    throw new UnsupportedOperationException("truncateTable"); // TODO
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
  public CompletableFuture<Void> updateReplicationPeerConfig(String arg0,
                                                             ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("updateReplicationPeerConfig");
  }

  @Override
  public CompletableFuture<Void> addReplicationPeer(String arg0, ReplicationPeerConfig arg1, boolean arg2) {
    throw new UnsupportedOperationException("addReplicationPeer"); // TODO
  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(String arg0, Map<TableName, List<String>> arg1) {
    throw new UnsupportedOperationException("appendReplicationPeerTableCFs"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> balancerSwitch(boolean arg0) {
    throw new UnsupportedOperationException("balancerSwitch"); // TODO
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
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> arg0, ServiceCaller<S, R> arg1) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> arg0, ServiceCaller<S, R> arg1,
                                                        ServerName arg2) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#deleteSnapshots()
   */
  @Override
  public CompletableFuture<Void> deleteSnapshots() {
    throw new UnsupportedOperationException("deleteSnapshots"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#deleteSnapshots(java.util.regex.Pattern)
   */
  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern arg0) {
    throw new UnsupportedOperationException("deleteSnapshots"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#deleteTableSnapshots(java.util.regex.Pattern)
   */
  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern arg0) {
    throw new UnsupportedOperationException("deleteTableSnapshots"); // TODO
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
  public CompletableFuture<byte[]> execProcedureWithReturn(String arg0, String arg1, Map<String, String> arg2) {
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
  public CompletableFuture<List<RegionInfo>> getRegions(TableName arg0) {
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
  public CompletableFuture<Boolean> isTableAvailable(TableName arg0) {
    throw new UnsupportedOperationException("isTableAvailable"); // TODO
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers() {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Pattern arg0) {
    throw new UnsupportedOperationException("listReplicationPeers"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#listSnapshots()
   */
  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    throw new UnsupportedOperationException("listSnapshots"); // TODO
  }

  /*
   * This method should be implemented.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#listSnapshots(java.util.regex.Pattern)
   */
  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern arg0) {
    throw new UnsupportedOperationException("listSnapshots"); // TODO
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
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern arg0) {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO
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
  public CompletableFuture<Void> modifyTable(TableDescriptor arg0) {
    throw new UnsupportedOperationException("modifyTable"); // TODO
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
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String arg0, Map<TableName, List<String>> arg1) {
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
}
