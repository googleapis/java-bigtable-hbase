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
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.util.ModifyTableBuilder;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.TableAdapter2x;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.Size.Unit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CommonConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.threeten.bp.Instant;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * Bigtable implementation of {@link AsyncAdmin}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class BigtableAsyncAdmin implements AsyncAdmin {
  private final Logger LOG = new Logger(getClass());

  private final Set<TableName> disabledTables;
  private final AdminClientWrapper bigtableTableAdminClient;
  private final BigtableHBaseSettings settings;
  private final CommonConnection asyncConnection;
  private final String bigtableInstanceName;
  private String snapshotClusterId;
  private final int ttlSeconds;

  public BigtableAsyncAdmin(CommonConnection asyncConnection) throws IOException {
    LOG.debug("Creating BigtableAsyncAdmin");
    this.settings = asyncConnection.getBigtableSettings();
    this.bigtableTableAdminClient = asyncConnection.getBigtableApi().getAdminClient();
    this.bigtableInstanceName = asyncConnection.getBigtableSettings().getInstanceId();
    this.disabledTables = asyncConnection.getDisabledTables();
    this.asyncConnection = asyncConnection;

    Configuration configuration = asyncConnection.getConfiguration();

    snapshotClusterId = configuration.get(BigtableOptionsFactory.BIGTABLE_SNAPSHOT_CLUSTER_ID_KEY);

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
        bigtableTableAdminClient.deleteBackupAsync(getBackupClusterId(), snapshotId));
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
                CreateBackupRequest.of(getBackupClusterId(), snapshotId)
                    .setExpireTime(expireTime)
                    .setSourceTableId(tableName.getNameAsString())))
        .thenAccept(backup -> {});
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotId, TableName tableName) {
    return toCompletableFuture(
            bigtableTableAdminClient.restoreTableAsync(
                RestoreTableRequest.of(getBackupClusterId(), snapshotId)
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
    return CompletableFuture.supplyAsync(this::getBackupClusterId)
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

  private synchronized String getBackupClusterId() {
    if (this.snapshotClusterId == null) {
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
      snapshotClusterId = clusters.get(0).getId();
    }
    return snapshotClusterId;
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

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(
      ServerName ignored, TableName tableName) {
    // TODO: implement caching
    CompletableFuture<List<KeyOffset>> keyOffsetsFuture =
        toCompletableFuture(
            asyncConnection
                .getBigtableApi()
                .getDataClient()
                .sampleRowKeysAsync(tableName.getNameAsString()));

    return keyOffsetsFuture.thenApply(
        keyOffsets -> {
          long now = System.currentTimeMillis();
          List<RegionMetrics> metrics = new ArrayList<>();
          ByteString lastKey = ByteString.EMPTY;
          long lastOffset = 0;
          for (KeyOffset keyOffset : keyOffsets) {
            byte[] regionName =
                RegionInfo.createRegionName(
                    tableName, lastKey.toByteArray(), Bytes.toBytes(now), false);
            metrics.add(
                BasicRegionMetrics.create(regionName, keyOffset.getOffsetBytes() - lastOffset));
            lastKey = keyOffset.getKey();
            lastOffset = keyOffset.getOffsetBytes();
          }
          return metrics;
        });
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshot) {
    Objects.requireNonNull(snapshot);
    return snapshot(snapshot.getName(), snapshot.getTableName());
  }

  private static Class<? extends BigtableAsyncAdmin> asyncAdminClass = null;

  /**
   * This is a workaround for incompatible changes in hbase minor versions. Dynamically generates a
   * class that extends BigtableAsyncAdmin so incompatible methods won't be accessed unless the
   * methods are called. If a method is implemented by BigtableAsyncAdmin, the generated class will
   * invoke the implementation in BigtableAsyncAdmin. Otherwise it'll throw {@link
   * UnsupportedOperationException}.
   */
  private static synchronized Class<? extends BigtableAsyncAdmin> getSubclass() {
    if (asyncAdminClass == null) {
      asyncAdminClass =
          new ByteBuddy()
              .subclass(BigtableAsyncAdmin.class)
              .method(ElementMatchers.isAbstract())
              .intercept(
                  InvocationHandlerAdapter.of(
                      new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
              .make()
              .load(BigtableAsyncAdmin.class.getClassLoader())
              .getLoaded();
    }
    return asyncAdminClass;
  }

  public static BigtableAsyncAdmin createInstance(CommonConnection connection) throws IOException {
    try {
      return getSubclass().getDeclaredConstructor(CommonConnection.class).newInstance(connection);
    } catch (InvocationTargetException e) {
      // Unwrap and throw IOException or RuntimeException as is, and convert all other exceptions to
      // IOException because
      // org.apache.hadoop.hbase.client.Connection#getAdmin() only throws
      // IOException
      Throwables.throwIfInstanceOf(e.getTargetException(), IOException.class);
      Throwables.throwIfInstanceOf(e.getTargetException(), RuntimeException.class);
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static Class<? extends RegionMetrics> regionMetricsClass = null;

  private static synchronized Class<? extends RegionMetrics> getRegionMetricsSubclass()
      throws NoSuchMethodException {
    if (regionMetricsClass == null) {
      regionMetricsClass =
          new ByteBuddy()
              .subclass(BasicRegionMetrics.class)
              .name(BasicRegionMetrics.class.getName() + "Impl")
              .method(ElementMatchers.isAbstract())
              .intercept(InvocationHandlerAdapter.of(new UnsupportedOperationsHandler()))
              .make()
              .load(BigtableAsyncAdmin.class.getClassLoader())
              .getLoaded();
    }
    return regionMetricsClass;
  }

  public abstract static class BasicRegionMetrics implements RegionMetrics {
    private final byte[] regionName;
    private final long size;

    static RegionMetrics create(byte[] regionName, long size) {
      try {
        return getRegionMetricsSubclass()
            .getConstructor(byte[].class, long.class)
            .newInstance(regionName, size);
      } catch (NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalStateException("Failed to instantiate RegionMetrics subclass", e);
      }
    }

    public BasicRegionMetrics(byte[] regionName, long size) {
      this.regionName = regionName;
      this.size = size;
    }

    @Override
    public byte[] getRegionName() {
      return regionName;
    }

    @Override
    public int getStoreFileCount() {
      return 1;
    }

    @Override
    public Size getStoreFileSize() {
      return new Size(size, Unit.BYTE);
    }
  }
  /** Handler for unsupported operations for generating Admin class at runtime. */
  public static class UnsupportedOperationsHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      throw new UnsupportedOperationException(method.getName());
    }
  }
}
