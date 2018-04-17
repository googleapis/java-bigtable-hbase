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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
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
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.TableAdapter2x;

import io.grpc.Status;

import static com.google.cloud.bigtable.hbase2_x.FutureUtils.*;

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
  private final BigtableInstanceName bigtableInstanceName;
  private final TableAdapter2x tableAdapter2x;
  private final BigtableAsyncConnection asyncConnection;
  private BigtableClusterName bigtableSnapshotClusterName;
  private final Configuration configuration;
  
  
  public BigtableAsyncAdmin(BigtableAsyncConnection asyncConnection) throws IOException {
    LOG.debug("Creating BigtableAsyncAdmin");
    this.options = asyncConnection.getOptions();
    this.bigtableTableAdminClient = new BigtableTableAdminClient(
        asyncConnection.getSession().getTableAdminClient());
    this.disabledTables = asyncConnection.getDisabledTables();
    this.bigtableInstanceName = options.getInstanceName();
    this.tableAdapter2x = new TableAdapter2x(options, new ColumnDescriptorAdapter());
    this.asyncConnection = asyncConnection;
    this.configuration = asyncConnection.getConfiguration();
    
    String clusterId = configuration.get(BigtableOptionsFactory.BIGTABLE_SNAPSHOT_CLUSTER_ID_KEY, null);
    if (clusterId != null) {
      bigtableSnapshotClusterName = bigtableInstanceName.toClusterName(clusterId);
    }
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys) {
    // wraps exceptions in a CF (CompletableFuture). No null check here on desc to match Hbase impl
    if (desc.getTableName() == null) {
      return failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }

    CreateTableRequest.Builder builder = tableAdapter2x.adapt(desc, splitKeys);
    builder.setParent(bigtableInstanceName.toString());
    return bigtableTableAdminClient.createTableAsync(builder.build())
        .handle((resp, ex) -> {
          if (ex != null) {
            throw new CompletionException(
                AbstractBigtableAdmin.convertToTableExistsException(desc.getTableName(), ex));
          }
          return null;
        });
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    return CompletableFuture
        .supplyAsync(() -> AbstractBigtableAdmin.createSplitKeys(startKey, endKey, numRegions))
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
    return tableExists(tableName).thenApply(exists -> {
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
    return tableExists(tableName).thenApply(exists -> {
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
    DeleteTableRequest request = DeleteTableRequest.newBuilder()
        .setName(bigtableInstanceName.toTableNameStr(tableName.getNameAsString()))
        .build();
    return bigtableTableAdminClient.deleteTableAsync(request)
        .thenAccept(r -> disabledTables.remove(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return listTableNames(Optional.of(Pattern.compile(tableName.getNameAsString())))
        .thenApply(r -> r.stream().anyMatch(e -> e.equals(tableName)));
  }

  private CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> tableNamePattern) {
    return requestTableList().thenApply(r ->
      r.stream().map(e -> bigtableInstanceName.toTableId(e.getName()))
          .filter(e -> !tableNamePattern.isPresent() || tableNamePattern.get().matcher(e).matches())
          .map(TableName::valueOf)
          .collect(Collectors.toList())
    );
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableName>> listTableNames(boolean includeSysTables) {
    return listTableNames(Optional.empty());
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableName>> listTableNames(Pattern tableNamePattern, boolean includeSysTables) {
    return listTableNames(Optional.of(tableNamePattern));
  }

  private CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> tableNamePattern) {
    return requestTableList().thenApply(r ->
         r.stream()
            .filter(t -> !tableNamePattern.isPresent() ||
                tableNamePattern.get().matcher(bigtableInstanceName.toTableId(t.getName())).matches())
            .map(tableAdapter2x::adapt)
            .collect(Collectors.toList())
      );
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(boolean includeSysTables) {
    return listTables(Optional.empty());
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(Pattern pattern, boolean includeSysTables) {
    return listTables(Optional.of(pattern));
  }

  private CompletableFuture<List<Table>> requestTableList() {
    ListTablesRequest  request = ListTablesRequest.newBuilder().setParent(bigtableInstanceName.toString()).build();
    return bigtableTableAdminClient.listTablesAsync(request)
        .thenApply(r -> r.getTablesList());
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

    GetTableRequest request = GetTableRequest
        .newBuilder()
        .setName(bigtableInstanceName.toTableNameStr(tableName.getNameAsString()))
        .build();

    return bigtableTableAdminClient.getTableAsync(request).handle((resp, ex) -> {
      if (ex != null) {
        if (Status.fromThrowable(ex).getCode() == Status.Code.NOT_FOUND) {
          throw new CompletionException(new TableNotFoundException(tableName));
        }
      }

      return tableAdapter2x.adapt(resp);
    });
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName tableName,
      byte[] columnName) {
    return modifyColumn(tableName, Modification
        .newBuilder()
        .setId(Bytes.toString(columnName))
        .setDrop(true));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String snapshotName) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return DeleteSnapshotRequest.newBuilder()
          .setName(getClusterName().toSnapshotName(snapshotName))
          .build();
      } catch (IOException e) {
        throw new CompletionException(e); 
      }
    }).thenCompose(
        d -> bigtableTableAdminClient.deleteSnapshotAsync(d).thenApply(r -> null));
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    return listTableSnapshots(tableNamePattern, snapshotNamePattern).thenApply(response->{
      for (SnapshotDescription snapshotDescription : response) {
        deleteSnapshot(snapshotDescription.getName());
      }
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDesc) {
    return modifyColumn(tableName, Modification
        .newBuilder()
        .setId(columnFamilyDesc.getNameAsString())
        .setUpdate(new ColumnDescriptorAdapter().adapt(
            TableAdapter2x.toHColumnDescriptor(columnFamilyDesc))));
  }

  /**
   * Restore the specified snapshot on the original table.
   *  
   * {@inheritDoc} */
  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
  	boolean takeFailSafeSnapshot = BigtableConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT;
    return restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  /**
   * Restore the specified snapshot on the original table.
   *  
   * {@inheritDoc} */
  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) {
  	CompletableFuture<Void> future = new CompletableFuture<>();
    listSnapshots(Pattern.compile(snapshotName)).whenComplete(
      (snapshotDescriptions, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        final TableName tableName = snapshotExists(snapshotName,snapshotDescriptions);
        if (tableName == null) {
          future.completeExceptionally(new RestoreSnapshotException(
              "Unable to find the table name for snapshot=" + snapshotName));
          return;
        }
        tableExists(tableName)
            .whenComplete((exists, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else if (!exists) {
                // if table does not exist, then just clone snapshot into new table.
              completeConditionalOnFuture(future,cloneSnapshot(snapshotName, tableName));
            } else {
              isTableDisabled(tableName).whenComplete(
                (disabled, err4) -> {
                  if (err4 != null) {
                    future.completeExceptionally(err4);
                  } else if (!disabled) {
                    future.completeExceptionally(new TableNotDisabledException(tableName));
                  } else {
                    completeConditionalOnFuture(future,
                      restoreSnapshot(snapshotName,takeFailSafeSnapshot));
                  }
                });
            }
          });
        });
    return future;
  }

  /**
   * To check Snapshot exists or not.
   * 
   * @param snapshotName
   * @param snapshotDescriptions
   * @return
   */
  private TableName snapshotExists(String snapshotName,
      List<SnapshotDescription> snapshotDescriptions) {
    TableName tableName = null;
    if (snapshotDescriptions != null && !snapshotDescriptions.isEmpty()) {
      for (SnapshotDescription snap : snapshotDescriptions) {
        if (snap.getName().equals(snapshotName)) {
          tableName = snap.getTableName();
          break;
        }
      }
    }
    return tableName;
  }

  private <T> void completeConditionalOnFuture(CompletableFuture<T> dependentFuture,
      CompletableFuture<T> parentFuture) {
    parentFuture.whenComplete((res, err) -> {
      if (err != null) {
        dependentFuture.completeExceptionally(err);
      } else {
        dependentFuture.complete(res);
      }
    });
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.AsyncAdmin#truncateTable(org.apache.hadoop.hbase.TableName, boolean)
   */
  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
  	if (!preserveSplits) {
      LOG.info("truncate will preserveSplits. The passed in variable is ignored.");
  	}
  	DropRowRangeRequest request = DropRowRangeRequest
        .newBuilder()
        .setDeleteAllDataFromTable(true)
        .setName(bigtableInstanceName.toTableNameStr(tableName.getNameAsString()))
        .build();
    return bigtableTableAdminClient.dropRowRangeAsync(request).thenApply(r -> null);
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots() {
  	return listSnapshots().thenApply(r->{
      for (SnapshotDescription snapshotDescription : r) {
        deleteSnapshot(snapshotDescription.getName());
      }
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern pattern) {
  	return listSnapshots(pattern).thenApply(r->{
      for (SnapshotDescription snapshotDescription : r) {
        deleteSnapshot(snapshotDescription.getName());
      }
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern) {
    return listSnapshots(tableNamePattern).thenApply(r-> {
        for (SnapshotDescription snapshotDescription : r) {
          deleteSnapshot(snapshotDescription.getName());
        }
        return null;
      });	
  }

  @Override
  public CompletableFuture<Void> snapshot(String snapshotName, TableName tableName) {
	  
	  return CompletableFuture.supplyAsync(() -> {
	      try {
	    	  return SnapshotTableRequest.newBuilder()
	    		        .setCluster(getSnapshotClusterName().toString())
	    		        .setSnapshotId(snapshotName)
	    		        .setName(options.getInstanceName().toTableNameStr(tableName.getNameAsString())).build();
	      } catch (IOException e) {
	        throw new CompletionException(e); 
	      }
	    }).thenCompose(
	        c -> bigtableTableAdminClient.snapshotTableAsync(c).thenApply(r -> null));
	  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName) {     
	  return CompletableFuture.supplyAsync(() -> {
	      try {
	    	  return CreateTableFromSnapshotRequest.newBuilder()
	    		        .setParent(options.getInstanceName().toString())
	    		        .setTableId(tableName.getNameAsString())
	    		        .setSourceSnapshot(getClusterName().toSnapshotName(snapshotName)).build();
	      } catch (IOException e) {
	        throw new CompletionException(e); 
	      }
	    }).thenCompose(
	        c -> bigtableTableAdminClient.createTableFromSnapshotAsync(c).thenApply(r -> null));
  }


  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return ListSnapshotsRequest.newBuilder()
            .setParent(getSnapshotClusterName().toString())
            .build();
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    }).thenCompose(request ->
        bigtableTableAdminClient.listSnapshotsAsync(request)
            .thenApply(r -> r.getSnapshotsList()
                .stream()
                .map(BigtableAsyncAdmin::toSnapshotDesscription)
                .collect(Collectors.toList())
            )
    );
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern pattern) {
    return listSnapshots().thenApply(r ->
        filter(r, d -> pattern.matcher(d.getName()).matches()));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern pattern) {
    return listSnapshots(pattern).thenApply(r ->
       filter(r, d -> pattern.matcher(d.getTableNameAsString()).matches())
    );
  }

  private static SnapshotDescription toSnapshotDesscription(Snapshot snapshot) {
    return new SnapshotDescription(
        snapshot.getName(),
        TableName.valueOf(snapshot.getSourceTable().getName()));
  }

  private static <T> List<T> filter(Collection<T> r, Predicate<T> predicate) {
    return r.stream().filter(predicate).collect(Collectors.toList());
  }

  // ****** TO BE IMPLEMENTED [start] ******

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern arg0) {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO
  }

  @Override
  public CompletableFuture<Void> modifyTable(TableDescriptor arg0) {
    throw new UnsupportedOperationException("modifyTable"); // TODO
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    CompletableFuture<Boolean> cf = new CompletableFuture<>();
    tableExists(tableName).thenAccept(exists -> {
      if (!exists) {
        cf.completeExceptionally(new TableNotFoundException(tableName));
      } else {
        cf.complete(true);
      }
    });
    return cf;
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(TableName arg0) {
    throw new UnsupportedOperationException("getRegions"); // TODO
  }

  // ****** TO BE IMPLEMENTED [end] ******

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Boolean> abortProcedure(long arg0, boolean arg1) {
    throw new UnsupportedOperationException("abortProcedure"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public CompletableFuture<Void> addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamilyDesc) {
    return modifyColumn(tableName, Modification
        .newBuilder()
        .setId(columnFamilyDesc.getNameAsString())
        .setCreate(new ColumnDescriptorAdapter().adapt(
            TableAdapter2x.toHColumnDescriptor(columnFamilyDesc))));
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

  @Override public CompletableFuture<Void> flushRegionServer(ServerName serverName) {
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
  public CompletableFuture<Void> majorCompactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("majorCompactRegionServer"); // TODO
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] arg0, byte[] arg1, boolean arg2) {
    throw new UnsupportedOperationException("mergeRegions"); // TODO
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
  
  private BigtableClusterName getSnapshotClusterName() throws IOException {
    if (bigtableSnapshotClusterName == null) {
      try {
        bigtableSnapshotClusterName = getClusterName();
      } catch (IllegalStateException e) {
        throw new IllegalStateException(
            "Failed to determine which cluster to use for snapshots, please configure it using "
                + BigtableOptionsFactory.BIGTABLE_SNAPSHOT_CLUSTER_ID_KEY);
      }
    }
    return bigtableSnapshotClusterName;
  }
  
  /**
   * <p>modifyColumn.</p>
   *
   * @param tableName a {@link org.apache.hadoop.hbase.TableName} object.
   * @param modification a {@link com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification.Builder} object.
   */
  private CompletableFuture<Void> modifyColumn(TableName tableName, Modification.Builder modification) {
    ModifyColumnFamiliesRequest request = ModifyColumnFamiliesRequest.newBuilder()
        .addModifications(modification)
        .setName(toBigtableName(tableName))
        .build();
    return bigtableTableAdminClient.modifyColumnFamilyAsync(request).thenApply(r -> null);
  }
  
  private BigtableClusterName getClusterName() throws IOException {
    return asyncConnection.getSession().getClusterName();
  }
  
  /**
   * <p>toBigtableName.</p>
   *
   * @param tableName a {@link org.apache.hadoop.hbase.TableName} object.
   * @return a {@link java.lang.String} object.
   */
  private String toBigtableName(TableName tableName) {
    return bigtableInstanceName.toTableNameStr(tableName.getNameAsString());
  }
}
