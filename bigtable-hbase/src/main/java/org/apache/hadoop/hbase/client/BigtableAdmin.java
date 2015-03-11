package org.apache.hadoop.hbase.client;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
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
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest.Builder;
import com.google.bigtable.admin.table.v1.ListTablesRequest;
import com.google.bigtable.admin.table.v1.ListTablesResponse;
import com.google.cloud.bigtable.hbase.BigtableOptions;
import com.google.cloud.bigtable.hbase.Logger;
import com.google.cloud.bigtable.hbase.adapters.ClusterMetadataSetter;
import com.google.cloud.bigtable.hbase.adapters.ColumnDescriptorAdapter;
import com.google.cloud.bigtable.hbase.adapters.ColumnFamilyFormatter;
import com.google.cloud.bigtable.hbase.adapters.TableAdapter;
import com.google.cloud.bigtable.hbase.adapters.TableMetadataSetter;
import com.google.cloud.hadoop.hbase.BigtableAdminClient;

public class BigtableAdmin implements Admin {

  private static final Logger LOG = new Logger(BigtableAdmin.class);

  /**
   * Bigtable doesn't require disabling tables before deletes or schema changes. Some clients do
   * call disable first, and then check for disable before deletes or schema changes. We're keeping
   * track of that state in memory on so that those clients can proceed with the delete/schema
   * change
   */
  private final Set<TableName> disabledTables;

  private final Configuration configuration;
  private final BigtableOptions options;
  private final BigtableConnection connection;
  private final BigtableAdminClient bigtableAdminClient;

  private ClusterMetadataSetter clusterMetadataSetter;
  private final ColumnDescriptorAdapter columnDescriptorAdapter = new ColumnDescriptorAdapter();
  private final TableAdapter tableAdapter;

  public BigtableAdmin(
      BigtableOptions options,
      Configuration configuration,
      BigtableConnection connection,
      BigtableAdminClient bigtableAdminClient,
      Set<TableName> disabledTables) {
    LOG.debug("Creating BigtableAdmin");
    this.configuration = configuration;
    this.options = options;
    this.connection = connection;
    this.bigtableAdminClient = bigtableAdminClient;
    this.disabledTables = disabledTables;
    this.clusterMetadataSetter = ClusterMetadataSetter.from(options);
    this.tableAdapter = new TableAdapter(options, columnDescriptorAdapter);
  }

  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void abort(String why, Throwable e) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isAborted() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    for(TableName existingTableName : listTableNames(tableName.getNameAsString())) {
      if (existingTableName.equals(tableName)) {
        return true;
      }
    }
    return false;
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public boolean tableExists(String tableName) throws IOException {
    return tableExists(TableName.valueOf(tableName));
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return listTables(".*");
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    // NOTE: We don't have systables.
    ListTablesRequest.Builder builder = ListTablesRequest.newBuilder();
    clusterMetadataSetter.setMetadata(builder);
    ListTablesResponse response;
    try {
      response = bigtableAdminClient.listTables(builder.build());
    } catch (Throwable throwable) {
      throw new IOException("Failed to listTables", throwable);
    }
    List<HTableDescriptor> result = new ArrayList<>();
    for (com.google.bigtable.admin.table.v1.Table table : response.getTablesList()) {
      // Convert the fully qualified Bigtable table name into an HBase table name before
      // comparing against the pattern.
      HTableDescriptor tableDescriptor = tableAdapter.adapt(table);
      if (pattern.matcher(tableDescriptor.getTableName().getNameAsString()).matches()) {
        result.add(tableDescriptor);
      }
    }

    return result.toArray(new HTableDescriptor[result.size()]);
  }

  @Override
  public HTableDescriptor[] listTables(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return listTables(pattern);
  }


  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public TableName[] listTableNames(String patternStr) throws IOException {
    return listTableNames(Pattern.compile(patternStr));
  }

  @Override
  public TableName[] listTableNames(Pattern pattern) throws IOException {
    return getTableNames(listTables(pattern));
  }

  private TableName[] getTableNames(HTableDescriptor[] tableList) {
    TableName[] tableNames = new TableName[tableList.length];
    for (int i=0; i<tableList.length; i++) {
      tableNames[i] = tableList[i].getTableName();
    }
    return tableNames;
  }

  @Override
  public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
    return listTableNames(pattern);
  }

  @Override
  public TableName[] listTableNames(String regex, boolean includeSysTables) throws IOException {
    return listTableNames(regex);
  }
  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] listTables(String regex, boolean includeSysTables) throws IOException {
    return listTables(regex);
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return getTableNames(listTables());
  }


  @Override
  public HTableDescriptor getTableDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    if (tableName == null) {
      return null;
    }

    HTableDescriptor[] response = listTables(tableName.getNameAsString());
    if (response.length > 0) {
      return response[0];
    } else {
      throw new TableNotFoundException(tableName.getNameAsString());
    }
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public String[] getTableNames(String regex) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(regex);
    String[] tableNames = new String[tableDescriptors.length];
    for (int i = 0; i < tableDescriptors.length; i++) {
      tableNames[i] = tableDescriptors[i].getNameAsString();
    }
    return tableNames;
  }

  @Override
  public void createTable(HTableDescriptor desc) throws IOException {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    clusterMetadataSetter.setMetadata(builder);
    builder.setTableId(desc.getTableName().getQualifierAsString());
    builder.setTable(tableAdapter.adapt(desc));
    try {
      bigtableAdminClient.createTable(builder.build());
    } catch (Throwable throwable) {
      throw new IOException(
          String.format(
              "Failed to create table '%s'",
              desc.getTableName().getNameAsString()),
          throwable);
    }
  }

  @Override
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    throw new UnsupportedOperationException("createTable");  // TODO
  }

  @Override
  public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException("createTable");  // TODO
  }

  @Override
  public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException("createTableAsync");  // TODO
  }

  @Override
  public void deleteTable(TableName tableName) throws IOException {
    Builder deleteBuilder = DeleteTableRequest.newBuilder();
    TableMetadataSetter.from(tableName, options).setMetadata(deleteBuilder);
    try {
      bigtableAdminClient.deleteTable(deleteBuilder.build());
    } catch (Throwable throwable) {
      throw new IOException(
          String.format(
              "Failed to delete table '%s'",
              tableName.getNameAsString()),
          throwable);
    }
  }

  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    throw new UnsupportedOperationException("deleteTables");  // TODO
  }

  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException("deleteTables");  // TODO
  }

  @Override
  public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
    throw new UnsupportedOperationException("truncateTable");  // TODO
  }

  @Override
  public void enableTable(TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    if (!this.tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
    disabledTables.remove(tableName);
    LOG.warn("Table " + tableName + " was enabled in memory only.");
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public void enableTable(String tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  @Override
  public void enableTableAsync(TableName tableName) throws IOException {
    enableTable(tableName);
  }

  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(regex);
    for (HTableDescriptor descriptor : tableDescriptors) {
      enableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(pattern);
    for (HTableDescriptor descriptor : tableDescriptors) {
      enableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  @Override
  public void disableTableAsync(TableName tableName) throws IOException {
    disableTable(tableName);
  }

  @Override
  public void disableTable(TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    if (!this.tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
    if (this.isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName);
    }
    disabledTables.add(tableName);
    LOG.warn("Table " + tableName + " was disabled in memory only.");
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public void disableTable(String tableName) throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  @Override
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(regex);
    for (HTableDescriptor descriptor : tableDescriptors) {
      disableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    HTableDescriptor[] tableDescriptors = listTables(pattern);
    for (HTableDescriptor descriptor : tableDescriptors) {
      disableTable(descriptor.getTableName());
    }
    return tableDescriptors;
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return !isTableDisabled(tableName);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return disabledTables.contains(tableName);
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public boolean isTableDisabled(String tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("isTableAvailable");  // TODO
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException("isTableAvailable");  // TODO
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("getAlterStatus");  // TODO
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
    throw new UnsupportedOperationException("getAlterStatus");  // TODO
  }

  @Override
  public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {
    TableMetadataSetter tableMetadataSetter = TableMetadataSetter.from(tableName, options);

    ColumnFamily.Builder columnFamily =
        columnDescriptorAdapter.adapt(column);

    CreateColumnFamilyRequest.Builder createColumnFamilyBuilder =
        CreateColumnFamilyRequest.newBuilder()
            .setColumnFamilyId(column.getNameAsString())
            .setColumnFamily(columnFamily);

    tableMetadataSetter.setMetadata(createColumnFamilyBuilder);

    try {
      bigtableAdminClient.createColumnFamily(createColumnFamilyBuilder.build());
    } catch (Throwable throwable) {
      throw new IOException(
          String.format(
              "Failed to add column '%s' to table '%s'",
              column.getNameAsString(),
              tableName.getNameAsString()),
          throwable);
    }
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public void addColumn(String tableName, HColumnDescriptor column) throws IOException {
    addColumn(TableName.valueOf(tableName), column);
  }

  @Override
  public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
    ColumnFamilyFormatter formatter = ColumnFamilyFormatter.from(tableName, options);
    String bigtableColumnName = formatter.formatForBigtable(Bytes.toString(columnName));
    try {
      bigtableAdminClient.deleteColumnFamily(
          DeleteColumnFamilyRequest.newBuilder().setName(bigtableColumnName).build());
    } catch (Throwable throwable) {
      throw new IOException(
          String.format(
              "Failed to delete column '%s' from table '%s'",
              Bytes.toString(columnName),
              tableName.getNameAsString()),
          throwable);
    }
  }

  // Used by the Hbase shell but not defined by Admin. Will be removed once the
  // shell is switch to use the methods defined in the interface.
  @Deprecated
  public void deleteColumn(String tableName, byte[] columnName) throws IOException {
    deleteColumn(TableName.valueOf(tableName), columnName);
  }

  @Override
  public void modifyColumn(TableName tableName, HColumnDescriptor descriptor) throws IOException {
    throw new UnsupportedOperationException("modifyColumn");  // TODO
  }

  @Override
  public void closeRegion(String regionname, String serverName) throws IOException {
    throw new UnsupportedOperationException("closeRegion");  // TODO
  }

  @Override
  public void closeRegion(byte[] regionname, String serverName) throws IOException {
    throw new UnsupportedOperationException("closeRegion");  // TODO
  }

  @Override
  public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName)
      throws IOException {
    throw new UnsupportedOperationException("closeRegionWithEncodedRegionName");  // TODO
  }

  @Override
  public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
    throw new UnsupportedOperationException("closeRegion");  // TODO
  }

  @Override
  public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
    throw new UnsupportedOperationException("getOnlineRegions");  // TODO
  }

  @Override
  public void flush(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("flush");  // TODO
  }

  @Override
  public void flushRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("flushRegion");  // TODO
  }

  @Override
  public void compact(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("compact");  // TODO
  }

  @Override
  public void compactRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("compactRegion");  // TODO
  }

  @Override
  public void compact(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("compact");  // TODO
  }

  @Override
  public void compactRegion(byte[] bytes, byte[] bytes2) throws IOException {
    throw new UnsupportedOperationException("compactRegion");  // TODO
  }

  @Override
  public void majorCompact(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("majorCompact");  // TODO
  }

  @Override
  public void majorCompactRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("majorCompactRegion");  // TODO
  }

  @Override
  public void majorCompact(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("majorCompact");  // TODO
  }

  @Override
  public void majorCompactRegion(byte[] bytes, byte[] bytes2) throws IOException {
    throw new UnsupportedOperationException("majorCompactRegion");  // TODO
  }

  @Override
  public void compactRegionServer(ServerName serverName, boolean b) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void move(byte[] encodedRegionName, byte[] destServerName)
      throws HBaseIOException, MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException("move");  // TODO
  }

  @Override
  public void assign(byte[] regionName)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    throw new UnsupportedOperationException("assign");  // TODO
  }

  @Override
  public void unassign(byte[] regionName, boolean force)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    throw new UnsupportedOperationException("unassign");  // TODO
  }

  @Override
  public void offline(byte[] regionName) throws IOException {
    throw new UnsupportedOperationException("offline");  // TODO
  }

  @Override
  public boolean setBalancerRunning(boolean on, boolean synchronous)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException("setBalancerRunning");  // TODO
  }

  @Override
  public boolean balancer()
      throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException("balancer");  // TODO
  }

  @Override
  public boolean enableCatalogJanitor(boolean enable)
      throws MasterNotRunningException {
    throw new UnsupportedOperationException("enableCatalogJanitor");  // TODO
  }

  @Override
  public int runCatalogScan() throws MasterNotRunningException {
    throw new UnsupportedOperationException("runCatalogScan");  // TODO
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws MasterNotRunningException {
    throw new UnsupportedOperationException("isCatalogJanitorEnabled");  // TODO
  }

  @Override
  public void mergeRegions(byte[] encodedNameOfRegionA, byte[] encodedNameOfRegionB,
      boolean forcible) throws IOException {
    throw new UnsupportedOperationException("mergeRegions");  // TODO
  }

  @Override
  public void split(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("split");  // TODO
  }

  @Override
  public void splitRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("splitRegion");  // TODO
  }

  @Override
  public void split(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException("split");  // TODO
  }

  @Override
  public void splitRegion(byte[] bytes, byte[] bytes2) throws IOException {
    throw new UnsupportedOperationException("splitRegion");  // TODO
  }

  @Override
  public void modifyTable(TableName tableName, HTableDescriptor htd) throws IOException {
    throw new UnsupportedOperationException("modifyTable");  // TODO
  }

  @Override
  public void shutdown() throws IOException {
    throw new UnsupportedOperationException("shutdown");  // TODO
  }

  @Override
  public void stopMaster() throws IOException {
    throw new UnsupportedOperationException("stopMaster");  // TODO
  }

  @Override
  public void stopRegionServer(String hostnamePort) throws IOException {
    throw new UnsupportedOperationException("stopRegionServer");  // TODO
  }

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

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    throw new UnsupportedOperationException("createNamespace");  // TODO
  }

  @Override
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    throw new UnsupportedOperationException("modifyNamespace");  // TODO
  }

  @Override
  public void deleteNamespace(String name) throws IOException {
    throw new UnsupportedOperationException("deleteNamespace");  // TODO
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    throw new UnsupportedOperationException("getNamespaceDescriptor");  // TODO
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    throw new UnsupportedOperationException("listNamespaceDescriptors");  // TODO
  }

  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
    throw new UnsupportedOperationException("listDescriptorsByNamespace");  // TODO
  }

  @Override
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    throw new UnsupportedOperationException("listTableNamesByNamespace");  // TODO
  }

  @Override
  public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
    List<HRegionInfo> regionInfos = new ArrayList<>();
    for (HRegionLocation location : connection.getRegionLocator(tableName).getAllRegionLocations()) {
      regionInfos.add(location.getRegionInfo());
    }
    return regionInfos;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    throw new UnsupportedOperationException("getTableDescriptorsByTableName");  // TODO
  }

  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
    throw new UnsupportedOperationException("getTableDescriptors");  // TODO
  }

  @Override
  public String[] getMasterCoprocessors() {
    throw new UnsupportedOperationException("getMasterCoprocessors");  // TODO
  }

  @Override
  public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(TableName tableName)
      throws IOException {
    throw new UnsupportedOperationException("getCompactionState");
  }

  @Override
  public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(byte[] bytes)
      throws IOException {
    throw new UnsupportedOperationException("getCompactionStateForRegion");
  }

  @Override
  public void snapshot(String snapshotName, TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException("snapshot");  // TODO
  }

  @Override
  public void snapshot(byte[] snapshotName, TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException("snapshot");  // TODO
  }

  @Override
  public void snapshot(String snapshotName, TableName tableName,
      HBaseProtos.SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException("snapshot");  // TODO
  }

  @Override
  public void snapshot(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException("snapshot");  // TODO
  }

  @Override
  public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    throw new UnsupportedOperationException("takeSnapshotAsync");  // TODO
  }

  @Override
  public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    throw new UnsupportedOperationException("isSnapshotFinished");  // TODO
  }

  @Override
  public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot");  // TODO
  }

  @Override
  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot");  // TODO
  }

  @Override
  public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot");  // TODO
  }

  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException("restoreSnapshot");  // TODO
  }

  @Override
  public void cloneSnapshot(byte[] snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    throw new UnsupportedOperationException("cloneSnapshot");  // TODO
  }

  @Override
  public void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    throw new UnsupportedOperationException("cloneSnapshot");  // TODO
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException("execProcedure");  // TODO
  }

  @Override
  public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException("execProcedureWithRet");  // TODO
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException("isProcedureFinished");  // TODO
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
    throw new UnsupportedOperationException("listSnapshots");  // TODO
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException {
    throw new UnsupportedOperationException("listSnapshots");  // TODO
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException("listSnapshots");  // TODO
  }

  @Override
  public void deleteSnapshot(byte[] snapshotName) throws IOException {
    throw new UnsupportedOperationException("deleteSnapshot");  // TODO
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {
    throw new UnsupportedOperationException("deleteSnapshot");  // TODO
  }

  @Override
  public void deleteSnapshots(String regex) throws IOException {
    throw new UnsupportedOperationException("deleteSnapshots");  // TODO
  }

  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException("deleteSnapshots");  // TODO
  }

  @Override
  public CoprocessorRpcChannel coprocessorService() {
    throw new UnsupportedOperationException("coprocessorService");  // TODO
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    throw new UnsupportedOperationException("coprocessorService");  // TODO
  }

  @Override
  public void updateConfiguration(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException("updateConfiguration");  // TODO
  }

  @Override
  public void updateConfiguration() throws IOException {
    throw new UnsupportedOperationException("updateConfiguration");  // TODO
  }

  @Override
  public int getMasterInfoPort() throws IOException {
    throw new UnsupportedOperationException("getMasterInfoPort");  // TODO
  }

  @Override
  public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
    throw new UnsupportedOperationException("rollWALWriter");  // TODO
  }
}
