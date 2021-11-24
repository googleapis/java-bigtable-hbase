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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CommonConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 2.x specific implementation of {@link AbstractBigtableAdmin}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class BigtableAdmin extends AbstractBigtableAdmin {

  private final BigtableAsyncAdmin asyncAdmin;

  public BigtableAdmin(AbstractBigtableConnection connection) throws IOException {
    super(connection);
    try {
      BigtableAsyncAdmin admin =
          new ByteBuddy()
              .subclass(BigtableAsyncAdmin.class)
              .defineConstructor(Visibility.PUBLIC)
              .intercept(
                  MethodCall.invoke(
                          BigtableAsyncAdmin.class.getDeclaredConstructor(CommonConnection.class))
                      .with(connection))
              .method(ElementMatchers.isAbstract())
              .intercept(
                  InvocationHandlerAdapter.of(
                      new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
              .make()
              .load(BigtableAsyncAdmin.class.getClassLoader())
              .getLoaded()
              .getDeclaredConstructor()
              .newInstance();
      asyncAdmin = admin;
    } catch (Exception e) {
      throw new IOException(e);
    }
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

  /** {@inheritDoc} */
  @Override
  public void snapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    asyncAdmin.snapshot(snapshot);
    LOG.warn(
        "isSnapshotFinished() is not currently supported by BigtableAdmin.\n"
            + "You may poll for existence of the snapshot with listSnapshots(snapshotName)");
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
    super.modifyTable(tableName, new HTableDescriptor(tableDescriptor));
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

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    List<RegionInfo> regionInfo = new ArrayList<>();
    for (HRegionInfo hRegionInfo : getTableRegions(tableName)) {
      regionInfo.add(hRegionInfo);
    }
    return regionInfo;
  }

  @Override
  public ClusterMetrics getClusterMetrics(EnumSet<Option> arg0) throws IOException {
    return getClusterStatus(); // TODO
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
