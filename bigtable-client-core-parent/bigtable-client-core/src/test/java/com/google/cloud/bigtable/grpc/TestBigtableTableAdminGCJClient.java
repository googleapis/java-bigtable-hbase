/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.admin.v2.BackupName;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteBackupRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.Backup;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoredTableResult;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.admin.v2.models.UpdateBackupRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableVeneerSettingsFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.longrunning.Operation;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Instant;

@RunWith(JUnit4.class)
public class TestBigtableTableAdminGCJClient {

  private static final String PROJECT_ID = "fake-project-id";
  private static final String INSTANCE_ID = "fake-instance-id";
  private static final String TABLE_ID = "fake-table-id";
  private static final String CLUSTER_ID = "fake-cluster-id";
  private static final String BACKUP_ID = "fake-backup-id";
  private static final String TEST_TABLE_ID_1 = "test-table-1";
  private static final String TEST_TABLE_ID_2 = "test-table-2";
  private static final String TEST_TABLE_ID_3 = "test-table-3";
  private static final String tableName =
      NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID);

  private Server server;
  private BigtableTableAdminClient adminClientV2;
  private BaseBigtableTableAdminClient baseClient;
  private BigtableTableAdminGCJClient adminGCJClient;
  private FakeBigtableAdminServiceImpl serviceImpl;

  @Before
  public void setUp() throws Exception {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();
    BigtableOptions options =
        BigtableOptions.builder()
            .setAdminHost("localhost")
            .setPort(availablePort)
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID)
            .setAppProfileId("AppProfileId")
            .setUseGCJClient(true)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUsePlaintextNegotiation(true)
            .build();

    serviceImpl = new FakeBigtableAdminServiceImpl();
    server = ServerBuilder.forPort(availablePort).addService(serviceImpl).build();
    server.start();
    BigtableTableAdminSettings settings =
        BigtableVeneerSettingsFactory.createTableAdminSettings(options);
    adminClientV2 = BigtableTableAdminClient.create(settings);
    BaseBigtableTableAdminSettings baseSettings =
        BaseBigtableTableAdminSettings.create(settings.getStubSettings());
    baseClient = BaseBigtableTableAdminClient.create(baseSettings);
    adminGCJClient = new BigtableTableAdminGCJClient(adminClientV2, baseClient);
  }

  @Test
  public void testCreateTable() {
    CreateTableRequest req = CreateTableRequest.of(TABLE_ID);
    Table response = adminGCJClient.createTable(req);
    assertEquals(TABLE_ID, response.getId());
  }

  @Test
  public void testCreateTableAsync() throws Exception {
    CreateTableRequest req = CreateTableRequest.of(TABLE_ID);
    Future<Table> response = adminGCJClient.createTableAsync(req);
    assertEquals(TABLE_ID, response.get().getId());
  }

  @Test
  public void testGetTable() {
    Table response = adminGCJClient.getTable(TABLE_ID);
    assertEquals(1, response.getColumnFamilies().size());
  }

  @Test
  public void testGetTableAsync() throws Exception {
    Future<Table> response = adminGCJClient.getTableAsync("test-async-table");
    assertEquals(2, response.get().getColumnFamilies().size());
  }

  @Test
  public void testListTables() {
    List<String> tableIds = adminGCJClient.listTables();
    assertEquals(Arrays.asList(TEST_TABLE_ID_1, TEST_TABLE_ID_2, TEST_TABLE_ID_3), tableIds);
  }

  @Test
  public void testListTablesAsync() throws Exception {
    Future<List<String>> tableIds = adminGCJClient.listTablesAsync();
    assertEquals(Arrays.asList(TEST_TABLE_ID_1, TEST_TABLE_ID_2, TEST_TABLE_ID_3), tableIds.get());
  }

  @Test
  public void testDeleteTable() {
    adminGCJClient.deleteTable("deleteTableID");
    DeleteTableRequest deleteTableReq = (DeleteTableRequest) serviceImpl.getRequests().get(0);
    assertEquals("deleteTableID", NameUtil.extractTableIdFromTableName(deleteTableReq.getName()));
  }

  @Test
  public void testDeleteTableAsync() throws Exception {
    Future<Void> deleteResponse = adminGCJClient.deleteTableAsync("deleteAsyncTableID");
    deleteResponse.get();
    DeleteTableRequest deleteTableReq = (DeleteTableRequest) serviceImpl.getRequests().get(0);
    assertEquals(
        "deleteAsyncTableID", NameUtil.extractTableIdFromTableName(deleteTableReq.getName()));
  }

  @Test
  public void testModifyFamily() {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID).addFamily("first-family");
    Table response = adminGCJClient.modifyFamilies(request);
    List<ColumnFamily> columnFamilies = response.getColumnFamilies();
    assertEquals("first-family", columnFamilies.get(0).getId());
  }

  @Test
  public void testModifyFamilyAsync() throws Exception {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID)
            .addFamily("first-family")
            .addFamily("another-family");
    Future<Table> response = adminGCJClient.modifyFamiliesAsync(request);
    List<ColumnFamily> columnFamilies = response.get().getColumnFamilies();
    assertEquals("first-family", columnFamilies.get(0).getId());
    assertEquals("another-family", columnFamilies.get(1).getId());
  }

  @Test
  public void dropRowRange() {
    String rowKey = "cf-dropRange";
    adminGCJClient.dropRowRange(TABLE_ID, rowKey);
    DropRowRangeRequest rangeRequest = (DropRowRangeRequest) serviceImpl.getRequests().get(0);
    assertEquals(TABLE_ID, NameUtil.extractTableIdFromTableName(rangeRequest.getName()));
    assertEquals(rowKey, rangeRequest.getRowKeyPrefix().toStringUtf8());
  }

  @Test
  public void dropRowRangeAsync() throws Exception {
    String rowKey = "cf-dropRange-async";
    adminGCJClient.dropRowRangeAsync(TABLE_ID, rowKey).get();
    DropRowRangeRequest rangeRequest = (DropRowRangeRequest) serviceImpl.getRequests().get(0);
    assertEquals(TABLE_ID, NameUtil.extractTableIdFromTableName(rangeRequest.getName()));
    assertEquals(rowKey, rangeRequest.getRowKeyPrefix().toStringUtf8());
  }

  @Test
  public void dropAllRows() {
    String tableId = "tableWithNoDataId";
    adminGCJClient.dropAllRows(tableId);
    DropRowRangeRequest rangeRequest = (DropRowRangeRequest) serviceImpl.getRequests().get(0);
    assertEquals(tableId, NameUtil.extractTableIdFromTableName(rangeRequest.getName()));
  }

  @Test
  public void dropAllRowsAsync() throws Exception {
    String tableId = "tableWithNoDataId";
    adminGCJClient.dropAllRowsAsync(tableId).get();
    DropRowRangeRequest rangeRequest = (DropRowRangeRequest) serviceImpl.getRequests().get(0);
    assertEquals(tableId, NameUtil.extractTableIdFromTableName(rangeRequest.getName()));
  }

  @Test
  public void testSnapshotTableAsync() throws Exception {
    SnapshotTableRequest request =
        SnapshotTableRequest.newBuilder().setSnapshotId("snapshotId").setName(tableName).build();
    Future<Operation> actualResponse = adminGCJClient.snapshotTableAsync(request);
    assertTrue(actualResponse.get().getDone());
  }

  @Test
  public void testGetSnapshotAsync() throws Exception {
    GetSnapshotRequest request = GetSnapshotRequest.newBuilder().setName(tableName).build();
    Future<Snapshot> actualResponse = adminGCJClient.getSnapshotAsync(request);
    assertEquals("testSnapshotName", actualResponse.get().getName());
  }

  @Test
  public void testListSnapshotsAsync() throws Exception {
    ListSnapshotsRequest request = ListSnapshotsRequest.newBuilder().setParent(tableName).build();
    Future<ListSnapshotsResponse> actualResponse = adminGCJClient.listSnapshotsAsync(request);
    assertEquals("firstSnapshotName", actualResponse.get().getSnapshots(0).getName());
    assertEquals("secondSnapshotName", actualResponse.get().getSnapshots(1).getName());
  }

  @Test
  public void testDeleteSnapshotAsync() throws Exception {
    DeleteSnapshotRequest request =
        DeleteSnapshotRequest.newBuilder().setName("testSnapshotName").build();
    adminGCJClient.deleteSnapshotAsync(request).get();
    DeleteSnapshotRequest receivedReq = (DeleteSnapshotRequest) serviceImpl.getRequests().get(0);
    assertEquals("testSnapshotName", receivedReq.getName());
  }

  @Test
  public void testCreateTableFromSnapshotAsync() throws Exception {
    CreateTableFromSnapshotRequest request =
        CreateTableFromSnapshotRequest.newBuilder()
            .setTableId(TABLE_ID)
            .setSourceSnapshot("test-snapshot")
            .build();
    Future<Operation> actualResponse = adminGCJClient.createTableFromSnapshotAsync(request);
    assertTrue(actualResponse.get().getDone());
  }

  @Test
  public void testCreateAndUpdateBackupAsync() throws Exception {
    CreateBackupRequest request =
        CreateBackupRequest.of(CLUSTER_ID, BACKUP_ID).setSourceTableId(tableName);
    Future<Backup> actualResponse = adminGCJClient.createBackupAsync(request);
    assertEquals(BACKUP_ID, actualResponse.get().getId());
    assertEquals(Instant.EPOCH, actualResponse.get().getExpireTime());

    Instant expireTime = Instant.ofEpochMilli(12345L);
    Future<Backup> updateResponse =
        adminGCJClient.updateBackupAsync(
            UpdateBackupRequest.of(CLUSTER_ID, BACKUP_ID).setExpireTime(expireTime));
    assertEquals(BACKUP_ID, updateResponse.get().getId());
    assertEquals(expireTime, updateResponse.get().getExpireTime());
  }

  @Test
  public void testGetBackupAsync() throws Exception {
    Future<Backup> actualResponse = adminGCJClient.getBackupAsync(CLUSTER_ID, BACKUP_ID);
    assertEquals(BACKUP_ID, actualResponse.get().getId());
  }

  @Test
  public void testListBackupsAsync() throws Exception {
    Future<List<String>> actualResponse = adminGCJClient.listBackupsAsync(CLUSTER_ID);
    assertEquals("fake-backup-1", actualResponse.get().get(0));
    assertEquals("fake-backup-2", actualResponse.get().get(1));
  }

  @Test
  public void testDeleteBackupAsync() throws Exception {
    String backupName = BackupName.format(PROJECT_ID, INSTANCE_ID, CLUSTER_ID, BACKUP_ID);
    adminGCJClient.deleteBackupAsync(CLUSTER_ID, BACKUP_ID).get();
    DeleteBackupRequest receivedReq = (DeleteBackupRequest) serviceImpl.getRequests().get(0);
    assertEquals(backupName, receivedReq.getName());
  }

  @Test
  public void testRestoreTableAsync() throws Exception {
    RestoreTableRequest request =
        RestoreTableRequest.of(CLUSTER_ID, BACKUP_ID).setTableId(TABLE_ID);
    Future<RestoredTableResult> actualResponse = adminGCJClient.restoreTableAsync(request);
    assertEquals(TABLE_ID, actualResponse.get().getTable().getId());
  }

  @After
  public void tearDown() throws Exception {
    if (adminClientV2 != null) {
      adminClientV2.close();
      adminClientV2 = null;
    }
    if (baseClient != null) {
      baseClient.close();
      baseClient = null;
    }
    if (server != null) {
      server.shutdown();
      server.awaitTermination();
      server = null;
    }
  }
}
