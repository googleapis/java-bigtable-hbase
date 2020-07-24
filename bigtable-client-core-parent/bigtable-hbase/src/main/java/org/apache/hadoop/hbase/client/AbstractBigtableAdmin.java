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
package org.apache.hadoop.hbase.client;

import static com.google.cloud.bigtable.hbase.util.ModifyTableBuilder.buildModifications;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.cloud.bigtable.admin.v2.models.Backup;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.admin.TableAdapter;
import com.google.cloud.bigtable.hbase.util.ModifyTableBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.threeten.bp.Instant;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * Abstract AbstractBigtableAdmin class.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
@SuppressWarnings("deprecation")
public abstract class AbstractBigtableAdmin implements Admin {

  protected final Logger LOG = new Logger(getClass());

  /**
   * Bigtable doesn't require disabling tables before deletes or schema changes. Some clients do
   * call disable first, and then check for disable before deletes or schema changes. We're keeping
   * track of that state in memory on so that those clients can proceed with the delete/schema
   * change
   */
  private final Set<TableName> disabledTables;

  private final Configuration configuration;
  private final BigtableOptions options;
  protected final CommonConnection connection;
  protected final IBigtableTableAdminClient tableAdminClientWrapper;
  protected final BigtableInstanceName bigtableInstanceName;
  private BigtableClusterName bigtableSnapshotClusterName;
  protected final TableAdapter tableAdapter;

  /**
   * Constructor for AbstractBigtableAdmin.
   *
   * @param connection a {@link CommonConnection} object.
   * @throws IOException if any.
   */
  public AbstractBigtableAdmin(CommonConnection connection) throws IOException {
    LOG.debug("Creating BigtableAdmin");
    configuration = connection.getConfiguration();
    options = connection.getOptions();
    this.connection = connection;
    disabledTables = connection.getDisabledTables();
    bigtableInstanceName = options.getInstanceName();
    tableAdapter = new TableAdapter(bigtableInstanceName);
    tableAdminClientWrapper = connection.getSession().getTableAdminClientWrapper();

    String clusterId =
        configuration.get(BigtableOptionsFactory.BIGTABLE_BACKUP_CLUSTER_ID_KEY, null);
    if (clusterId != null) {
      bigtableSnapshotClusterName = bigtableInstanceName.toClusterName(clusterId);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Connection getConnection() {
    return (Connection) connection;
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    for (TableName existingTableName : listTableNames(tableName.getNameAsString())) {
      if (existingTableName.equals(tableName)) {
        return true;
      }
    }
    return false;
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * tableExists.
   *
   * @param tableName a {@link java.lang.String} object.
   * @return a boolean.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public boolean tableExists(String tableName) throws IOException {
    return tableExists(TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] listTables() throws IOException {
    // NOTE: We don't have systables.
    return getTableDescriptorsIgnoreFailure(listTableNames());
  }

  private HTableDescriptor[] getTableDescriptors(TableName[] tableNames) throws IOException {
    HTableDescriptor[] response = new HTableDescriptor[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      response[i] = getTableDescriptor(tableNames[i]);
    }
    return response;
  }

  // BigtableTableAdmin#listTables doesn't include table details (like column families), so the
  // table descriptors must be fetched into 2 phases:
  // 1. list all of the names
  // 2. fetch the table details
  //
  // Due to the non-atomic listing, tables in the list might disappear before fetching the details.
  // This method will suppress those tables.
  private HTableDescriptor[] getTableDescriptorsIgnoreFailure(TableName[] tableNames)
      throws IOException {
    List<HTableDescriptor> descriptors = new ArrayList<>();
    for (TableName tableName : tableNames) {
      try {
        descriptors.add(getTableDescriptor(tableName));
      } catch (IOException ex) {

        // This suppresses TableNotFoundException, which is for consistency with HBase layers,
        // and FailedPreconditionException or Status.Code.FAILED_PRECONDITION, which comes from
        // Bigtable table creation internal state. Both of these can occur due to race condition.
        if (ex instanceof TableNotFoundException
            || ex.getCause() instanceof FailedPreconditionException
            || Status.Code.FAILED_PRECONDITION == Status.fromThrowable(ex.getCause()).getCode()) {
          continue;
        }
        throw ex;
      }
    }
    return descriptors.toArray(new HTableDescriptor[0]);
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    // NOTE: We don't have systables.
    return getTableDescriptorsIgnoreFailure(listTableNames(pattern));
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] listTables(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return listTables(pattern);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /** {@inheritDoc} */
  @Override
  @Deprecated
  public TableName[] listTableNames(String patternStr) throws IOException {
    return listTableNames(Pattern.compile(patternStr));
  }

  /** {@inheritDoc} */
  @Override
  public TableName[] listTableNames(Pattern pattern) throws IOException {
    if (pattern == null) {
      return listTableNames();
    }
    List<TableName> result = new ArrayList<>();

    for (TableName tableName : listTableNames()) {
      if (pattern.matcher(tableName.getNameAsString()).matches()) {
        result.add(tableName);
      }
    }

    return result.toArray(new TableName[result.size()]);
  }

  /** {@inheritDoc} */
  @Override
  public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
    return listTableNames(pattern);
  }

  /** {@inheritDoc} */
  @Override
  public TableName[] listTableNames(String regex, boolean includeSysTables) throws IOException {
    return listTableNames(regex);
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex));
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] listTables(String regex, boolean includeSysTables) throws IOException {
    return listTables(regex);
  }

  /** Lists all table names for the cluster provided in the configuration. */
  @Override
  public TableName[] listTableNames() throws IOException {
    // tablesList contains list of tableId.
    List<String> tablesList = tableAdminClientWrapper.listTables();

    TableName[] result = new TableName[tablesList.size()];
    for (int i = 0; i < tablesList.size(); i++) {
      result[i] = TableName.valueOf(tablesList.get(i));
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    if (tableName == null) {
      return null;
    }

    try {
      return tableAdapter.adapt(tableAdminClientWrapper.getTable(tableName.getNameAsString()));
    } catch (Throwable throwable) {
      if (Status.fromThrowable(throwable).getCode() == Status.Code.NOT_FOUND) {
        throw new TableNotFoundException(tableName);
      }
      throw new IOException("Failed to getTableDescriptor() on " + tableName, throwable);
    }
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * getTableNames.
   *
   * @param regex a {@link String} object.
   * @return an array of {@link String} objects.
   * @throws IOException if any.
   */
  @Deprecated
  public String[] getTableNames(String regex) throws IOException {
    TableName[] tableNames = listTableNames(regex);
    String[] tableIds = new String[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      tableIds[i] = tableNames[i].getNameAsString();
    }
    return tableIds;
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(HTableDescriptor desc) throws IOException {
    createTable(desc, null);
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    createTable(desc, createSplitKeys(startKey, endKey, numRegions));
  }

  public static byte[][] createSplitKeys(byte[] startKey, byte[] endKey, int numRegions) {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    byte[][] splitKeys;
    if (numRegions == 3) {
      splitKeys = new byte[][] {startKey, endKey};
    } else {
      splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
      if (splitKeys == null || splitKeys.length != numRegions - 1) {
        throw new IllegalArgumentException("Unable to split key range into enough regions");
      }
    }
    return splitKeys;
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    createTable(desc.getTableName(), TableAdapter.adapt(desc, splitKeys));
  }

  /**
   * Creates a Table.
   *
   * @param tableName a {@link TableName} object.
   * @param request a {@link CreateTableRequest} object to send.
   * @throws java.io.IOException if any.
   */
  protected void createTable(TableName tableName, CreateTableRequest request) throws IOException {
    try {
      tableAdminClientWrapper.createTable(request);
    } catch (Throwable throwable) {
      throw convertToTableExistsException(tableName, throwable);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void createTableAsync(final HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    LOG.warn("Creating the table synchronously");
    createTableAsync(desc.getTableName(), TableAdapter.adapt(desc, splitKeys));
  }

  /**
   * @param tableName a {@link TableName} object for exception identification.
   * @param request a {@link CreateTableRequest} object to send.
   * @return a {@link ListenableFuture} object.
   * @throws java.io.IOException if any.
   */
  protected ListenableFuture<Table> createTableAsync(
      final TableName tableName, CreateTableRequest request) throws IOException {
    ApiFuture<Table> future = tableAdminClientWrapper.createTableAsync(request);
    final SettableFuture<Table> settableFuture = SettableFuture.create();
    ApiFutures.addCallback(
        future,
        new ApiFutureCallback<Table>() {
          @Override
          public void onSuccess(@Nullable Table result) {
            settableFuture.set(result);
          }

          @Override
          public void onFailure(Throwable t) {
            settableFuture.setException(convertToTableExistsException(tableName, t));
          }
        },
        MoreExecutors.directExecutor());
    return settableFuture;
  }

  public static IOException convertToTableExistsException(
      TableName tableName, Throwable throwable) {
    if (Status.fromThrowable(throwable).getCode() == Status.Code.ALREADY_EXISTS) {
      return new TableExistsException(tableName);
    } else {
      return new IOException(String.format("Failed to create table '%s'", tableName), throwable);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteTable(TableName tableName) throws IOException {
    try {
      tableAdminClientWrapper.deleteTable(tableName.getNameAsString());
    } catch (Throwable throwable) {
      throw new IOException(
          String.format("Failed to delete table '%s'", tableName.getNameAsString()), throwable);
    }
    disabledTables.remove(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      try {
        deleteTable(table.getTableName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getTableName(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  /** {@inheritDoc} */
  @Override
  public void enableTable(TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
    disabledTables.remove(tableName);
    LOG.warn("Table " + tableName + " was enabled in memory only.");
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * enableTable.
   *
   * @param tableName a {@link java.lang.String} object.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public void enableTable(String tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(regex);
    for (HTableDescriptor descriptor : tableDescriptors) {
      enableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(pattern);
    for (HTableDescriptor descriptor : tableDescriptors) {
      enableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  /** {@inheritDoc} */
  @Override
  public void disableTable(TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
    if (isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName);
    }
    disabledTables.add(tableName);
    LOG.warn("Table " + tableName + " was disabled in memory only.");
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * disableTable.
   *
   * @param tableName a {@link java.lang.String} object.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public void disableTable(String tableName) throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(regex);
    for (HTableDescriptor descriptor : tableDescriptors) {
      disableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(pattern);
    for (HTableDescriptor descriptor : tableDescriptors) {
      disableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return !isTableDisabled(tableName);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * isTableEnabled.
   *
   * @param tableName a {@link java.lang.String} object.
   * @return a boolean.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    Preconditions.checkNotNull(tableName, "TableName cannot be null");
    return disabledTables.contains(tableName);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * isTableDisabled.
   *
   * @param tableName a {@link java.lang.String} object.
   * @return a boolean.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public boolean isTableDisabled(String tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return tableExists(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {
    modifyColumns(
        tableName,
        column.getNameAsString(),
        "add",
        ModifyTableBuilder.newBuilder(tableName).add(column));
  }

  /** {@inheritDoc} */
  @Override
  public void modifyColumn(TableName tableName, HColumnDescriptor column) throws IOException {
    modifyColumns(
        tableName,
        column.getNameAsString(),
        "modify",
        ModifyTableBuilder.newBuilder(tableName).modify(column));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
    String name = Bytes.toString(columnName);
    modifyColumns(tableName, name, "delete", ModifyTableBuilder.newBuilder(tableName).delete(name));
  }

  /** {@inheritDoc} */
  @Override
  public void modifyTable(TableName tableName, HTableDescriptor newDescriptor) throws IOException {
    if (isTableAvailable(tableName)) {
      try {
        ModifyColumnFamiliesRequest request =
            buildModifications(newDescriptor, getTableDescriptor(tableName)).build();
        tableAdminClientWrapper.modifyFamilies(request);
      } catch (Throwable throwable) {
        throw new IOException(
            String.format("Failed to modify table '%s'", tableName.getNameAsString()), throwable);
      }
    } else {
      throw new TableNotFoundException(tableName);
    }
  }

  /**
   * modifyColumns.
   *
   * @param tableName a {@link TableName} object for error messages.
   * @param columnName a {@link String} object for error messages
   * @param modificationType a {@link String} object for error messages
   * @param builder a {@link ModifyTableBuilder} object to send.
   * @throws java.io.IOException if any.
   */
  protected Void modifyColumns(
      TableName tableName, String columnName, String modificationType, ModifyTableBuilder builder)
      throws IOException {
    try {
      tableAdminClientWrapper.modifyFamilies(builder.build());
      return null;
    } catch (Throwable throwable) {
      throw new IOException(
          String.format(
              "Failed to %s column '%s' in table '%s'",
              modificationType, columnName, tableName.getNameAsString()),
          throwable);
    }
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public void addColumn(String tableName, HColumnDescriptor column) throws IOException {
    addColumn(TableName.valueOf(tableName), column);
  }

  /**
   * Modify an existing column family on a table. NOTE: this is needed for backwards compatibility
   * for the hbase shell.
   *
   * @param tableName a {@link TableName} object.
   * @param descriptor a {@link HColumnDescriptor} object.
   * @throws java.io.IOException if any.
   */
  public void modifyColumns(final String tableName, HColumnDescriptor descriptor)
      throws IOException {
    modifyColumn(TableName.valueOf(tableName), descriptor);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * deleteColumn.
   *
   * @param tableName a {@link java.lang.String} object.
   * @param columnName an array of byte.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public void deleteColumn(String tableName, byte[] columnName) throws IOException {
    deleteColumn(TableName.valueOf(tableName), columnName);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  /**
   * deleteColumn.
   *
   * @param tableName a {@link java.lang.String} object.
   * @param columnName a {@link java.lang.String} object.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public void deleteColumn(final String tableName, final String columnName) throws IOException {
    deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(columnName));
  }

  /** {@inheritDoc} */
  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return new ClusterStatus() {
      @Override
      public Collection<ServerName> getServers() {
        // TODO(sduskis): Point the server name to options.getServerName()
        return Collections.emptyList();
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /** {@inheritDoc} */
  @Override
  public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
    return connection.getAllRegionInfos(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // no-op
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    if (tableNames == null || tableNames.isEmpty()) {
      return listTables();
    }

    TableName[] tableNameArray = tableNames.toArray(new TableName[tableNames.size()]);
    return getTableDescriptors(tableNameArray);
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
    Preconditions.checkNotNull(names);
    if (names.isEmpty()) {
      return listTables();
    }

    TableName[] tableNameArray = new TableName[names.size()];
    for (int i = 0; i < names.size(); i++) {
      tableNameArray[i] = TableName.valueOf(names.get(i));
    }
    return getTableDescriptors(tableNameArray);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("project", options.getProjectId())
        .add("instance", options.getInstanceId())
        .add("adminHost", options.getAdminHost())
        .toString();
  }

  /* Unsupported operations */

  /** {@inheritDoc} */
  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException("getOperationTimeout"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void abort(String why, Throwable e) {
    throw new UnsupportedOperationException("abort"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean isAborted() {
    throw new UnsupportedOperationException("isAborted"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
    if (!preserveSplits) {
      LOG.info("truncate will preserveSplits. The passed in variable is ignored.");
    }
    try {
      tableAdminClientWrapper.dropAllRows(tableName.getNameAsString());
    } catch (Throwable throwable) {
      throw new IOException(
          String.format("Failed to truncate table '%s'", tableName.getNameAsString()), throwable);
    }
    disabledTables.remove(tableName);
  }

  /**
   * deleteRowRangeByPrefix.
   *
   * @param tableName a {@link org.apache.hadoop.hbase.TableName} object.
   * @param prefix an array of byte.
   * @throws java.io.IOException if any.
   */
  public void deleteRowRangeByPrefix(TableName tableName, byte[] prefix) throws IOException {
    try {
      tableAdminClientWrapper.dropRowRange(
          tableName.getNameAsString(), ByteString.copyFrom(prefix));
    } catch (Throwable throwable) {
      throw new IOException(
          String.format("Failed to truncate table '%s'", tableName.getNameAsString()), throwable);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return tableExists(tableName);
  }

  /**
   * {@inheritDoc}
   *
   * <p>HBase column operations are not synchronous, since they're not as fast as Bigtable. Bigtable
   * does not have async operations, so always return (0, 0). This is needed for some shell
   * operations.
   */
  @Override
  public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
    return new Pair<>(0, 0);
  }

  /** {@inheritDoc} */
  @Override
  public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
    return getAlterStatus(TableName.valueOf(tableName));
  }

  /**
   * getAlterStatus.
   *
   * @param tableName a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.hbase.util.Pair} object.
   * @throws java.io.IOException if any.
   */
  public Pair<Integer, Integer> getAlterStatus(String tableName) throws IOException {
    return getAlterStatus(TableName.valueOf(tableName));
  }

  // ------------ SNAPSHOT methods begin

  /**
   * Creates a snapshot from an existing table. NOTE: Cloud Bigtable has a cleanup policy
   *
   * @param snapshotName a {@link String} object.
   * @param tableName a {@link TableName} object.
   * @throws IOException if any.
   */
  @Override
  public void snapshot(String snapshotName, TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {

    snapshotTable(snapshotName, tableName);
  }

  protected Backup snapshotTable(String snapshotName, TableName tableName) throws IOException {
    CreateBackupRequest request =
        CreateBackupRequest.of(getBackupClusterName().toString(), snapshotName)
            .setSourceTableId(tableName.getNameAsString());

    int ttlSecs =
        configuration.getInt(BigtableOptionsFactory.BIGTABLE_BACKUP_DEFAULT_TTL_SECS_KEY, -1);
    Instant expireTime = Instant.now().plus(ttlSecs, ChronoUnit.SECONDS);
    if (ttlSecs > 0) {
      request.setExpireTime(expireTime);
    }

    return Futures.getChecked(
        tableAdminClientWrapper.createBackupAsync(request), IOException.class);
  }

  /**
   * This is needed for the hbase shell.
   *
   * @param snapshotName a byte array object.
   * @param tableName a byte array object.
   * @throws IOException if any.
   */
  public void snapshot(byte[] snapshotName, byte[] tableName)
      throws IOException, IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public void snapshot(byte[] snapshotName, TableName tableName)
      throws IOException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), tableName);
  }

  /**
   * This is needed for the hbase shell.
   *
   * @param snapshotName a byte array object.
   * @param tableName a byte array object.
   * @throws IOException if any.
   */
  public void cloneSnapshot(byte[] snapshotName, byte[] tableName) throws IOException {
    cloneSnapshot(snapshotName, TableName.valueOf(tableName));
  }

  /**
   * @param snapshotName a {@link String} object.
   * @param tableName a {@link TableName} object.
   * @throws IOException if any.
   */
  @Override
  public void cloneSnapshot(byte[] snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    cloneSnapshot(Bytes.toString(snapshotName), tableName);
  }

  /**
   * @param snapshotName a {@link String} object.
   * @param tableName a {@link TableName} object.
   * @throws IOException if any.
   */
  @Override
  public void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    RestoreTableRequest request =
        RestoreTableRequest.of(getClusterName().toString(), snapshotName)
            .setTableId(tableName.getNameAsString());
    Futures.getChecked(tableAdminClientWrapper.restoreTableAsync(request), IOException.class);
  }

  protected BigtableClusterName getClusterName() throws IOException {
    return connection.getSession().getClusterName();
  }

  protected BigtableClusterName getBackupClusterName() throws IOException {
    if (bigtableSnapshotClusterName == null) {
      try {
        bigtableSnapshotClusterName = getClusterName();
      } catch (IllegalStateException e) {
        throw new IllegalStateException(
            "Failed to determine which cluster to use for snapshots, please configure it using "
                + BigtableOptionsFactory.BIGTABLE_BACKUP_CLUSTER_ID_KEY);
      }
    }
    return bigtableSnapshotClusterName;
  }

  /** {@inheritDoc} */
  @Override
  public void deleteSnapshot(byte[] snapshotName) throws IOException {
    deleteSnapshot(Bytes.toString(snapshotName));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {
    Futures.getUnchecked(
        tableAdminClientWrapper.deleteBackupAsync(getClusterName().getClusterId(), snapshotName));
  }

  /**
   * {@inheritDoc}
   *
   * <p>The snapshots will be deleted serially and the first failure will prevent the deletion of
   * the remaining snapshots.
   */
  @Override
  public void deleteSnapshots(String regex) throws IOException {
    deleteSnapshots(Pattern.compile(regex));
  }

  /**
   * {@inheritDoc}
   *
   * <p>The snapshots will be deleted serially and the first failure will prevent the deletion of
   * the remaining snapshots.
   */
  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {
    if (pattern != null && !pattern.matcher("").matches()) {
      for (SnapshotDescription description : listSnapshots()) {
        if (pattern.matcher(description.getName()).matches()) {
          tableAdminClientWrapper.deleteBackupAsync(
              getClusterName().getClusterId(), description.getName());
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteTableSnapshots(String tableNameRegex, String snapshotNameRegex)
      throws IOException {
    deleteTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
  }

  /**
   * {@inheritDoc}
   *
   * <p>The snapshots will be deleted serially and the first failure will prevent the deletion of
   * the remaining snapshots.
   */
  @Override
  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
      throws IOException {
    for (SnapshotDescription snapshotDescription :
        listTableSnapshots(tableNamePattern, snapshotNamePattern)) {
      deleteSnapshot(snapshotDescription.getName());
    }
  }

  // ------------- Unsupported snapshot methods.
  /** {@inheritDoc} */
  @Override
  public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot"); // TODO
  }

  // ------------- Snapshot method end

  /** {@inheritDoc} */
  @Override
  public void closeRegion(String regionname, String serverName) throws IOException {
    throw new UnsupportedOperationException("closeRegion"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void closeRegion(byte[] regionname, String serverName) throws IOException {
    throw new UnsupportedOperationException("closeRegion"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName)
      throws IOException {
    throw new UnsupportedOperationException("closeRegionWithEncodedRegionName"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
    throw new UnsupportedOperationException("closeRegion"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
    throw new UnsupportedOperationException("getOnlineRegions"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void flush(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("flush"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void flushRegion(byte[] bytes) throws IOException {
    LOG.info("flushRegion is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void compact(TableName tableName) throws IOException {
    LOG.info("compact is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void compactRegion(byte[] bytes) throws IOException {
    LOG.info("compactRegion is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void compact(TableName tableName, byte[] bytes) throws IOException {
    LOG.info("compact is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void compactRegion(byte[] bytes, byte[] bytes2) throws IOException {
    LOG.info("compactRegion is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void majorCompact(TableName tableName) throws IOException {
    LOG.info("majorCompact is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void majorCompactRegion(byte[] bytes) throws IOException {
    LOG.info("majorCompactRegion is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void majorCompact(TableName tableName, byte[] bytes) throws IOException {
    LOG.info("majorCompact is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void majorCompactRegion(byte[] bytes, byte[] bytes2) throws IOException {
    LOG.info("majorCompactRegion is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void compactRegionServer(ServerName serverName, boolean b) throws IOException {
    LOG.info("compactRegionServer is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void move(byte[] encodedRegionName, byte[] destServerName)
      throws HBaseIOException, MasterNotRunningException, ZooKeeperConnectionException {
    LOG.info("move is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void assign(byte[] regionName)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    LOG.info("assign is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void unassign(byte[] regionName, boolean force)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    LOG.info("unassign is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void offline(byte[] regionName) throws IOException {
    throw new UnsupportedOperationException("offline"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean setBalancerRunning(boolean on, boolean synchronous)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException("setBalancerRunning"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean balancer() throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException("balancer"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean enableCatalogJanitor(boolean enable) throws MasterNotRunningException {
    throw new UnsupportedOperationException("enableCatalogJanitor"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public int runCatalogScan() throws MasterNotRunningException {
    throw new UnsupportedOperationException("runCatalogScan"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean isCatalogJanitorEnabled() throws MasterNotRunningException {
    throw new UnsupportedOperationException("isCatalogJanitorEnabled"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void mergeRegions(
      byte[] encodedNameOfRegionA, byte[] encodedNameOfRegionB, boolean forcible)
      throws IOException {
    LOG.info("mergeRegions is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void split(TableName tableName) throws IOException {
    LOG.info("split is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void splitRegion(byte[] bytes) throws IOException {
    LOG.info("splitRegion is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void split(TableName tableName, byte[] bytes) throws IOException {
    LOG.info("split is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void splitRegion(byte[] bytes, byte[] bytes2) throws IOException {
    LOG.info("split is a no-op");
  }

  /** {@inheritDoc} */
  @Override
  public void shutdown() throws IOException {
    throw new UnsupportedOperationException("shutdown"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void stopMaster() throws IOException {
    throw new UnsupportedOperationException("stopMaster"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void stopRegionServer(String hostnamePort) throws IOException {
    throw new UnsupportedOperationException("stopRegionServer"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("createNamespace is a no-op");
    } else {
      throw new UnsupportedOperationException("createNamespace"); // TODO
    }
  }

  /** {@inheritDoc} */
  @Override
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("modifyNamespace is a no-op");
    } else {
      throw new UnsupportedOperationException("modifyNamespace"); // TODO
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteNamespace(String name) throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("deleteNamespace is a no-op");
    } else {
      throw new UnsupportedOperationException("deleteNamespace"); // TODO
    }
  }

  /** {@inheritDoc} */
  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("getNamespaceDescriptor is a no-op");
      return null;
    } else {
      throw new UnsupportedOperationException("getNamespaceDescriptor"); // TODO
    }
  }

  /** {@inheritDoc} */
  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("listNamespaceDescriptors is a no-op");
      return new NamespaceDescriptor[0];
    } else {
      throw new UnsupportedOperationException("listNamespaceDescriptors"); // TODO
    }
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("listTableDescriptorsByNamespace is a no-op");
      return new HTableDescriptor[0];
    } else {
      throw new UnsupportedOperationException("listTableDescriptorsByNamespace"); // TODO
    }
  }

  /** {@inheritDoc} */
  @Override
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    if (provideWarningsForNamespaces()) {
      LOG.warn("listTableNamesByNamespace is a no-op");
      return new TableName[0];
    } else {
      throw new UnsupportedOperationException("listTableNamesByNamespace"); // TODO
    }
  }

  private boolean provideWarningsForNamespaces() {
    return configuration.getBoolean(BigtableOptionsFactory.BIGTABLE_NAMESPACE_WARNING_KEY, false);
  }

  /** {@inheritDoc} */
  @Override
  public String[] getMasterCoprocessors() {
    throw new UnsupportedOperationException("getMasterCoprocessors"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException("execProcedure"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException("execProcedureWithRet"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException("isProcedureFinished"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public CoprocessorRpcChannel coprocessorService() {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    throw new UnsupportedOperationException("coprocessorService"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void updateConfiguration(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException("updateConfiguration"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void updateConfiguration() throws IOException {
    throw new UnsupportedOperationException("updateConfiguration"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public int getMasterInfoPort() throws IOException {
    throw new UnsupportedOperationException("getMasterInfoPort"); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
    throw new UnsupportedOperationException("rollWALWriter"); // TODO
  }
}
