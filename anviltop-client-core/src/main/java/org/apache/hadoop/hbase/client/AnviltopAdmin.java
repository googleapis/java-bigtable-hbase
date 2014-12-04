package org.apache.hadoop.hbase.client;


import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.CreateFamilyRequest;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.CreateFamilyResponse;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.CreateTableRequest;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.CreateTableResponse;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.DeleteFamilyRequest;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.DeleteFamilyResponse;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.DeleteTableRequest;
import com.google.bigtable.anviltop.AnviltopAdminServiceMessages.DeleteTableResponse;
import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.anviltop.hbase.AnviltopOptions;
import com.google.cloud.anviltop.hbase.adapters.ColumnDescriptorAdapter;
import com.google.cloud.hadoop.hbase.AnviltopAdminClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

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
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AnviltopAdmin implements Admin {

  private final Configuration configuration;
  private final AnviltopOptions options;
  private final AnvilTopConnection connection;
  private final AnviltopAdminClient anviltopAdminClient;
  private final ColumnDescriptorAdapter columnDescriptorAdapter = new ColumnDescriptorAdapter();

  public AnviltopAdmin(
      AnviltopOptions options,
      Configuration configuration,
      AnvilTopConnection connection,
      AnviltopAdminClient anviltopAdminClient) {
    this.configuration = configuration;
    this.options = options;
    this.connection = connection;
    this.anviltopAdminClient = anviltopAdminClient;
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
  public HConnection getConnection() {
    return connection;
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    throw new UnsupportedOperationException("tableExists");  // TODO
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    throw new UnsupportedOperationException("listTables");  // TODO
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException("listTables");  // TODO
  }

  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    throw new UnsupportedOperationException("listTables");  // TODO
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    throw new UnsupportedOperationException("listTableNames");  // TODO
  }

  @Override
  public HTableDescriptor getTableDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    throw new UnsupportedOperationException("getTableDescriptor");  // TODO
  }

  @Override
  public void createTable(HTableDescriptor desc) throws IOException {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setProjectId(options.getProjectId())
        .setTableNameBytes(ByteString.copyFrom(desc.getName()));

    for (HColumnDescriptor column : desc.getColumnFamilies()) {
      builder.addColumnFamilies(AnviltopData.ColumnFamily.newBuilder().setName(column.getNameAsString()));
    }

    anviltopAdminClient.createTable(builder.build());
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
    anviltopAdminClient.deleteTable(DeleteTableRequest.newBuilder()
        .setProjectId(options.getProjectId())
        .setTableNameBytes(ByteString.copyFrom(tableName.getQualifier()))
        .build());
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
    // enableTable is a NOP for Anviltop, tables are always enabled.
  }

  @Override
  public void enableTableAsync(TableName tableName) throws IOException {
    // enableTableAsync is a NOP for Anviltop, tables are always enabled.
  }

  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    // enableTables is a NOP for Anviltop, tables are always enabled.
    return new HTableDescriptor[]{};
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    // enableTables is a NOP for Anviltop, tables are always enabled.
    return new HTableDescriptor[]{};
  }

  @Override
  public void disableTableAsync(TableName tableName) throws IOException {
    // disableTableAsync is a NOP for Anviltop, tables can't be actually disabled.
  }

  @Override
  public void disableTable(TableName tableName) throws IOException {
    // disableTable is a NOP for Anviltop, tables can't be actually disabled.
  }

  @Override
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    // disableTables is a NOP for Anviltop, tables can't be actually disabled.
    return new HTableDescriptor[]{};
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    // disableTables is a NOP for Anviltop, tables can't be actually disabled.
    return new HTableDescriptor[]{};
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    // Anviltop tables can't be disabled.
    return true;
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    // Anviltop tables can't be disabled.
    return false;
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
    anviltopAdminClient.createFamily(
        CreateFamilyRequest.newBuilder()
            .setProjectId(options.getProjectId())
            .setTableName(tableName.getQualifierAsString())
            .setFamily(columnDescriptorAdapter.adapt(column))
            .build());
  }

  @Override
  public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
    anviltopAdminClient.deleteFamily(
        DeleteFamilyRequest.newBuilder()
            .setProjectId(options.getProjectId())
            .setTableName(tableName.getQualifierAsString())
            .setFamilyNameBytes(ByteString.copyFrom(columnName)).build());
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
