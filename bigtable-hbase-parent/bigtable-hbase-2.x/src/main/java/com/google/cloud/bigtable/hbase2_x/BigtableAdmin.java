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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;

import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;

/**
 * HBase 2.x specific implementation of {@link AbstractBigtableAdmin}.
 * @author spollapally
 */
@SuppressWarnings("deprecation")
public class BigtableAdmin extends AbstractBigtableAdmin {

  public BigtableAdmin(AbstractBigtableConnection connection) throws IOException {
    super(connection);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableDescriptor desc) throws IOException {
    createTable((HTableDescriptor) desc);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
    createTable((HTableDescriptor) desc, splitKeys);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    createTable((HTableDescriptor) desc, startKey, endKey, numRegions);
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    createTableAsync((HTableDescriptor) desc, splitKeys);
    // TODO Consider better options after adding support for async hbase2
    return CompletableFuture.runAsync(() -> {
    });
  }

  /** {@inheritDoc} */
  @Override
  public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  /** {@inheritDoc} */
  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern)
      throws IOException {
    List<SnapshotDescription> response = new ArrayList<>();
    for (SnapshotDescription description : listSnapshots()) {
      if (pattern.matcher(description.getName()).matches()) {
        response.add(description);
      }
    }
    return response;

  }

  /**
   * {@inheritDoc}
   * 
   * Calling {@link #addColumn(TableName, ColumnFamilyDescriptor)} was causing stackoverflow.
   * Copying the same code here. //TODO - need to find a better way
   */
  @Override
  public void addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    String columnName = columnFamily.getNameAsString();
    Modification.Builder modification = Modification.newBuilder().setId(columnName)
        .setCreate(new ColumnDescriptorAdapter().adapt((HColumnDescriptor) columnFamily).build());
    modifyColumn(tableName, columnName, "add", modification);

  }

  /**
   * {@inheritDoc}
   * 
   * Calling {@link #addColumn(TableName, ColumnFamilyDescriptor)} was causing stackoverflow.
   * Copying the same code here. //TODO - need to find a better way 
   */
  @Override
  public void modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    String columnName = columnFamily.getNameAsString();
    Modification.Builder modification = Modification.newBuilder().setId(columnName)
        .setUpdate(new ColumnDescriptorAdapter().adapt((HColumnDescriptor) columnFamily).build());
    modifyColumn(tableName, columnName, "update", modification);

  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> deleteNamespaceAsync(String name) throws IOException {
    deleteNamespace(name);
    // TODO Consider better options after adding support for async hbase2
    return CompletableFuture.runAsync(() -> {});
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> disableTableAsync(TableName tableName) throws IOException {
    disableTable(tableName);
    // TODO Consider better options after adding support for async hbase2
    return CompletableFuture.runAsync(() -> {});
  }

  /** {@inheritDoc} */
  @Override
  public Future<Void> enableTableAsync(TableName tableName) throws IOException {
    enableTable(tableName);
    // TODO Consider better options after adding support for async hbase2
    return CompletableFuture.runAsync(() -> {});
  }

  /** {@inheritDoc} */
  @Override
  public TableDescriptor getDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    HTableDescriptor[] descriptors =
        getTableDescriptorsByTableName(new ArrayList<TableName>(Arrays.asList(tableName)));
    if (descriptors != null && descriptors.length > 0) {
      return descriptors[0];
    }
    return null;
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
  public void snapshot(String snapshotName, TableName tableName, SnapshotType arg2)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void snapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    snapshotTable(snapshot.getName(), snapshot.getTableName());
    LOG.warn("isSnapshotFinished() is not currently supported by BigtableAdmin.\n"
        + "You may poll for existence of the snapshot with listSnapshots(snpashotName)");
  }

  @Override
  public Future<Void> addColumnFamilyAsync(TableName arg0, ColumnFamilyDescriptor arg1)
      throws IOException {
    throw new UnsupportedOperationException("addColumnFamilyAsync"); 
  }

  @Override
  public List<SnapshotDescription> listSnapshots()
      throws IOException {
    throw new IllegalArgumentException("listSnapshots is not supported");
  }


  @Override
  public void deleteColumnFamily(TableName arg0, byte[] arg1) throws IOException {
    throw new UnsupportedOperationException("deleteColumnFamily"); // TODO
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(TableName arg0, byte[] arg1) throws IOException {
    throw new UnsupportedOperationException("deleteColumnFamily"); // TODO
  }

  @Override
  public Future<Void> deleteTableAsync(TableName arg0) throws IOException {
    throw new UnsupportedOperationException("deleteTableAsync"); // TODO
  }

  @Override
  public List<TableDescriptor> listTableDescriptors() throws IOException {
    throw new UnsupportedOperationException("listTableDescriptors"); // TODO
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern arg0) throws IOException {
    throw new UnsupportedOperationException("listTableDescriptors"); // TODO
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(List<TableName> arg0) throws IOException {
    throw new UnsupportedOperationException("listTableDescriptors"); // TODO
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("listTableDescriptors"); // TODO
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(String arg0,
      String arg1) throws IOException {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern arg0,
      Pattern arg1) throws IOException {
    throw new UnsupportedOperationException("listTableSnapshots"); // TODO
  }


  @Override
  public Future<Void> modifyColumnFamilyAsync(TableName arg0, ColumnFamilyDescriptor arg1)
      throws IOException {
    // TODO - implementable with async hbase2
    throw new UnsupportedOperationException("modifyColumnFamilyAsync");
  }

  @Override
  public void modifyTable(TableDescriptor arg0) throws IOException {
    throw new UnsupportedOperationException("modifyTable"); // TODO
  }

  @Override
  public void modifyTable(TableName arg0, TableDescriptor arg1) throws IOException {
    throw new UnsupportedOperationException("modifyTable"); // TODO
  }

  @Override
  public Future<Void> modifyTableAsync(TableDescriptor arg0) throws IOException {
    // TODO - implementable with async hbase2
    throw new UnsupportedOperationException("modifyTableAsync");
  }

  @Override
  public Future<Void> modifyTableAsync(TableName arg0, TableDescriptor arg1) throws IOException {
    // TODO - implementable with async hbase2
    throw new UnsupportedOperationException("modifyTableAsync");
  }

  @Override
  public Future<Void> truncateTableAsync(TableName arg0, boolean arg1) throws IOException {
    // TODO - implementable with async hbase2
    throw new UnsupportedOperationException("truncateTableAsync");
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
  public byte[] execProcedureWithReturn(String arg0, String arg1, Map<String, String> arg2)
      throws IOException {
    throw new UnsupportedOperationException("execProcedureWithReturn");
  }

  @Override
  public ClusterStatus getClusterStatus(EnumSet<Option> arg0) throws IOException {
    return getClusterStatus(); // TODO
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
  public Map<byte[], RegionLoad> getRegionLoad(ServerName arg0) throws IOException {
    // TODO : new in 2.0
    throw new UnsupportedOperationException("getRegionLoad");
  }

  @Override
  public Map<byte[], RegionLoad> getRegionLoad(ServerName arg0, TableName arg1) throws IOException {
    // TODO : new in 2.0
    throw new UnsupportedOperationException("getRegionLoad");

  }

  @Override
  public List<RegionInfo> getRegions(ServerName arg0) throws IOException {
    // TODO : new in 2.0
    throw new UnsupportedOperationException("getRegions");
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    // TODO : new in 2.0
    throw new UnsupportedOperationException("getRegions");
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
  public Future<Void> mergeRegionsAsync(byte[][] arg0, boolean arg1) throws IOException {
    throw new UnsupportedOperationException("mergeRegionsAsync"); // TODO
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
  public boolean splitOrMergeEnabledSwitch(MasterSwitchType arg0) throws IOException {
    throw new UnsupportedOperationException("splitOrMergeEnabledSwitch"); // TODO
  }

  @Override
  public boolean[] splitOrMergeEnabledSwitch(boolean arg0, boolean arg1, MasterSwitchType... arg2)
      throws IOException {
    throw new UnsupportedOperationException("splitOrMergeEnabledSwitch"); // TODO
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] arg0, byte[] arg1) throws IOException {
    throw new UnsupportedOperationException("splitRegionAsync"); // TODO
  }
}