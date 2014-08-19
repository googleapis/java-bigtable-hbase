package org.apache.hadoop.hbase.client;


import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.anviltop.hbase.AnviltopOptions;
import com.google.cloud.hadoop.hbase.AnviltopAdminClient;
import com.google.protobuf.ServiceException;

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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AnviltopAdmin implements Admin {

  private final AnviltopOptions options;
  private final AnvilTopConnection connection;
  private final AnviltopAdminClient anviltopAdminClient;

  public AnviltopAdmin(AnviltopOptions options,
      AnvilTopConnection connection,
      AnviltopAdminClient anviltopAdminClient) {
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
  public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor getTableDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void createTable(HTableDescriptor desc) throws IOException {
    anviltopAdminClient.createTable(options.getProjectId(), Bytes.toString(desc.getName()));
  }

  @Override
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void deleteTable(TableName tableName) throws IOException {
    anviltopAdminClient.deleteTable(options.getProjectId(), tableName.getQualifierAsString());
  }

  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void enableTable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void enableTableAsync(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void disableTableAsync(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void disableTable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {
    anviltopAdminClient.createFamily(
        options.getProjectId(),
        tableName.getQualifierAsString(),
        AnviltopData.ColumnFamily.newBuilder()
            .setName(column.getNameAsString())
            .build());
  }

  @Override
  public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
    anviltopAdminClient.deleteFamily(
        options.getProjectId(),
        tableName.getQualifierAsString(),
        Bytes.toString(columnName));
  }

  @Override
  public void modifyColumn(TableName tableName, HColumnDescriptor descriptor) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void closeRegion(String regionname, String serverName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void closeRegion(byte[] regionname, String serverName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void flush(String tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void flush(byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void compact(String tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void compact(byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void compact(String tableOrRegionName, String columnFamily)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void compact(byte[] tableNameOrRegionName, byte[] columnFamily)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void majorCompact(String tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void majorCompact(byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void majorCompact(String tableNameOrRegionName, String columnFamily)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void majorCompact(byte[] tableNameOrRegionName, byte[] columnFamily)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void move(byte[] encodedRegionName, byte[] destServerName)
      throws HBaseIOException, MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void assign(byte[] regionName)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void unassign(byte[] regionName, boolean force)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void offline(byte[] regionName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean setBalancerRunning(boolean on, boolean synchronous)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean balancer()
      throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean enableCatalogJanitor(boolean enable)
      throws ServiceException, MasterNotRunningException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public int runCatalogScan() throws ServiceException, MasterNotRunningException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws ServiceException, MasterNotRunningException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void mergeRegions(byte[] encodedNameOfRegionA, byte[] encodedNameOfRegionB,
      boolean forcible) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void split(String tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void split(byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void split(String tableNameOrRegionName, String splitPoint)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void split(byte[] tableNameOrRegionName, byte[] splitPoint)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void modifyTable(TableName tableName, HTableDescriptor htd) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void shutdown() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void stopMaster() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void stopRegionServer(String hostnamePort) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void deleteNamespace(String name) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public byte[][] rollHLogWriter(String serverName) throws IOException, FailedLogCloseException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public String[] getMasterCoprocessors() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(
      String tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(
      byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void snapshot(String snapshotName, TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void snapshot(byte[] snapshotName, TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void snapshot(String snapshotName, TableName tableName,
      HBaseProtos.SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void snapshot(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isSnapshotFinished(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void restoreSnapshot(byte[] snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void cloneSnapshot(byte[] snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void deleteSnapshot(byte[] snapshotName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void deleteSnapshots(String regex) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public CoprocessorRpcChannel coprocessorService() {
    throw new UnsupportedOperationException();  // TODO
  }
}
