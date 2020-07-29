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

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateBackupMetadata;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteBackupRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetBackupRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListBackupsRequest;
import com.google.bigtable.admin.v2.ListBackupsResponse;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.RestoreTableMetadata;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.Backup;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoredTableResult;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.admin.v2.models.UpdateBackupRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.threeten.bp.Instant;

@RunWith(JUnit4.class)
public class TestBigtableTableAdminClientWrapper {
  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String CLUSTER_ID = "clusterId";
  private static final String TABLE_ID = "tableId";
  private static final String BACKUP_ID = "backupId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final String COLUMN_FAMILY = "myColumnFamily";
  private static final String ROW_KEY_PREFIX = "row-key-val";
  private static final String UPDATE_FAMILY = "update-family";
  private static final String TEST_TABLE_ID_1 = "test-table-1";
  private static final String TEST_TABLE_ID_2 = "test-table-2";
  private static final String TEST_TABLE_ID_3 = "test-table-3";

  private BigtableOptions options =
      BigtableOptions.builder()
          .setProjectId(PROJECT_ID)
          .setInstanceId(INSTANCE_ID)
          .setAppProfileId(APP_PROFILE_ID)
          .build();
  private static final BigtableInstanceName INSTANCE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID);
  private static final String TABLE_NAME = INSTANCE_NAME.toTableNameStr(TABLE_ID);

  private static final BigtableClusterName CLUSTER_NAME = INSTANCE_NAME.toClusterName(CLUSTER_ID);
  private static final String BACKUP_NAME = CLUSTER_NAME.toBackupName(BACKUP_ID);

  private BigtableTableAdminClient mockAdminClient;

  private BigtableTableAdminClientWrapper clientWrapper;

  @Before
  public void setUp() {
    mockAdminClient = Mockito.mock(BigtableTableAdminClient.class);
    clientWrapper = new BigtableTableAdminClientWrapper(mockAdminClient, options);
  }

  @Test
  public void testCreateTable() {
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);

    when(mockAdminClient.createTable(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(createTableData());
    Table response = clientWrapper.createTable(request);

    assertEquals(Table.fromProto(createTableData()), response);
    verify(mockAdminClient).createTable(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void testCreateTableAsync() throws Exception {
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);

    when(mockAdminClient.createTableAsync(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(immediateFuture(createTableData()));
    Future<Table> response = clientWrapper.createTableAsync(request);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(mockAdminClient).createTableAsync(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void testGetTable() {
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();

    when(mockAdminClient.getTable(request)).thenReturn(createTableData());
    Table response = clientWrapper.getTable(TABLE_ID);

    assertEquals(Table.fromProto(createTableData()), response);
    verify(mockAdminClient).getTable(request);
  }

  @Test
  public void testGetTableAsync() throws Exception {
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();

    when(mockAdminClient.getTableAsync(request)).thenReturn(immediateFuture(createTableData()));
    Future<Table> response = clientWrapper.getTableAsync(TABLE_ID);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(mockAdminClient).getTableAsync(request);
  }

  @Test
  public void testListTables() {
    ImmutableList<String> tableIdList =
        ImmutableList.of(TEST_TABLE_ID_1, TEST_TABLE_ID_2, TEST_TABLE_ID_3);
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(INSTANCE_NAME.toString()).build();
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_1)).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_2)).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_3)).build();

    when(mockAdminClient.listTables(request)).thenReturn(builder.build());
    List<String> actualResponse = clientWrapper.listTables();

    assertEquals(tableIdList, actualResponse);
    verify(mockAdminClient).listTables(request);
  }

  @Test
  public void testListTablesAsync() throws Exception {
    ImmutableList<String> tableIdList =
        ImmutableList.of(TEST_TABLE_ID_1, TEST_TABLE_ID_2, TEST_TABLE_ID_3);
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(INSTANCE_NAME.toString()).build();
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_1)).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_2)).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_3)).build();

    when(mockAdminClient.listTablesAsync(request)).thenReturn(immediateFuture(builder.build()));
    Future<List<String>> actualResponse = clientWrapper.listTablesAsync();

    assertEquals(tableIdList, actualResponse.get());
    verify(mockAdminClient).listTablesAsync(request);
  }

  @Test
  public void testDeleteTable() {
    DeleteTableRequest request = DeleteTableRequest.newBuilder().setName(TABLE_NAME).build();

    doNothing().when(mockAdminClient).deleteTable(request);
    clientWrapper.deleteTable(TABLE_ID);

    verify(mockAdminClient).deleteTable(request);
  }

  @Test
  public void testDeleteTableAsync() {
    DeleteTableRequest request = DeleteTableRequest.newBuilder().setName(TABLE_NAME).build();

    when(mockAdminClient.deleteTableAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    clientWrapper.deleteTableAsync(TABLE_ID);

    verify(mockAdminClient).deleteTableAsync(request);
  }

  @Test
  public void testModifyFamilies() {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID)
            .addFamily(COLUMN_FAMILY, GCRULES.maxVersions(1))
            .updateFamily(UPDATE_FAMILY, GCRULES.defaultRule());

    when(mockAdminClient.modifyColumnFamily(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(createTableData());
    Table response = clientWrapper.modifyFamilies(request);

    assertEquals(Table.fromProto(createTableData()), response);
    verify(mockAdminClient).modifyColumnFamily(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void testModifyFamiliesAsync() throws Exception {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID)
            .addFamily(COLUMN_FAMILY, GCRULES.maxVersions(1))
            .updateFamily(UPDATE_FAMILY, GCRULES.defaultRule());

    when(mockAdminClient.modifyColumnFamilyAsync(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(immediateFuture(createTableData()));
    Future<Table> response = clientWrapper.modifyFamiliesAsync(request);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(mockAdminClient).modifyColumnFamilyAsync(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void dropRowRangeForDeleteByPrefix() {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(false)
            .setRowKeyPrefix(ByteString.copyFromUtf8(ROW_KEY_PREFIX))
            .build();

    doNothing().when(mockAdminClient).dropRowRange(request);
    clientWrapper.dropRowRange(TABLE_ID, ROW_KEY_PREFIX);

    verify(mockAdminClient).dropRowRange(request);
  }

  @Test
  public void dropRowRangeForTruncate() {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(true)
            .build();

    doNothing().when(mockAdminClient).dropRowRange(request);
    clientWrapper.dropAllRows(TABLE_ID);

    verify(mockAdminClient).dropRowRange(request);
  }

  @Test
  public void dropRowRangeAsyncForDeleteByPrefix() {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(false)
            .setRowKeyPrefix(ByteString.copyFromUtf8(ROW_KEY_PREFIX))
            .build();

    when(mockAdminClient.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    clientWrapper.dropRowRangeAsync(TABLE_ID, ROW_KEY_PREFIX);

    verify(mockAdminClient).dropRowRangeAsync(request);
  }

  @Test
  public void dropRowRangeAsyncForTruncate() {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(true)
            .build();

    when(mockAdminClient.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    clientWrapper.dropAllRowsAsync(TABLE_ID);

    verify(mockAdminClient).dropRowRangeAsync(request);
  }

  @Test
  public void testSnapshotTableAsync() throws Exception {
    SnapshotTableRequest request =
        SnapshotTableRequest.newBuilder().setSnapshotId("snaphsotId").setName(TABLE_NAME).build();
    Operation response = Operation.getDefaultInstance();
    when(mockAdminClient.snapshotTableAsync(request)).thenReturn(Futures.immediateFuture(response));
    Future<Operation> actualResponse = clientWrapper.snapshotTableAsync(request);
    assertEquals(response, actualResponse.get());
    verify(mockAdminClient).snapshotTableAsync(request);
  }

  @Test
  public void testGetSnapshotAsync() throws Exception {
    GetSnapshotRequest request = GetSnapshotRequest.newBuilder().setName(TABLE_NAME).build();
    Snapshot response = Snapshot.getDefaultInstance();
    when(mockAdminClient.getSnapshotAsync(request)).thenReturn(Futures.immediateFuture(response));
    Future<Snapshot> actualResponse = clientWrapper.getSnapshotAsync(request);
    assertEquals(response, actualResponse.get());
    verify(mockAdminClient).getSnapshotAsync(request);
  }

  @Test
  public void testListSnapshotsAsync() throws Exception {
    ListSnapshotsRequest request = ListSnapshotsRequest.newBuilder().setParent(TABLE_NAME).build();
    ListSnapshotsResponse response = ListSnapshotsResponse.getDefaultInstance();
    when(mockAdminClient.listSnapshotsAsync(request)).thenReturn(Futures.immediateFuture(response));
    Future<ListSnapshotsResponse> actualResponse = clientWrapper.listSnapshotsAsync(request);
    assertEquals(response, actualResponse.get());
    verify(mockAdminClient).listSnapshotsAsync(request);
  }

  @Test
  public void testDeleteSnapshotAsync() {
    DeleteSnapshotRequest request = DeleteSnapshotRequest.newBuilder().setName(TABLE_NAME).build();
    when(mockAdminClient.deleteSnapshotAsync(request))
        .thenReturn(Futures.immediateFuture(Empty.getDefaultInstance()));
    clientWrapper.deleteSnapshotAsync(request);
    verify(mockAdminClient).deleteSnapshotAsync(request);
  }

  @Test
  public void testCreateTableFromSnapshotAsync() throws Exception {
    CreateTableFromSnapshotRequest request =
        CreateTableFromSnapshotRequest.newBuilder()
            .setTableId(TABLE_ID)
            .setSourceSnapshot("test-snapshot")
            .build();
    Operation response = Operation.getDefaultInstance();
    when(mockAdminClient.createTableFromSnapshotAsync(request))
        .thenReturn(Futures.immediateFuture(response));
    Future<Operation> actualResponse = clientWrapper.createTableFromSnapshotAsync(request);
    assertEquals(response, actualResponse.get());
    verify(mockAdminClient).createTableFromSnapshotAsync(request);
  }

  @Test
  public void testCreateBackupAsync() throws Exception {
    CreateBackupRequest request =
        CreateBackupRequest.of(CLUSTER_ID, BACKUP_ID).setSourceTableId(TABLE_NAME);
    com.google.bigtable.admin.v2.CreateBackupRequest requestProto =
        request.toProto(PROJECT_ID, INSTANCE_ID);
    Operation op =
        Operation.newBuilder()
            .setMetadata(Any.pack(CreateBackupMetadata.getDefaultInstance()))
            .setResponse(
                Any.pack(
                    com.google.bigtable.admin.v2.Backup.newBuilder()
                        .setName(BACKUP_NAME)
                        .setSourceTable(TABLE_NAME)
                        .build()))
            .build();
    when(mockAdminClient.createBackupAsync(requestProto)).thenReturn(Futures.immediateFuture(op));
    Future<Backup> actualResponse = clientWrapper.createBackupAsync(request);
    assertEquals(BACKUP_ID, actualResponse.get().getId());
    verify(mockAdminClient).createBackupAsync(requestProto);
  }

  @Test
  public void testGetBackupAsync() throws Exception {
    GetBackupRequest request = GetBackupRequest.newBuilder().setName(BACKUP_NAME).build();
    com.google.bigtable.admin.v2.Backup response =
        com.google.bigtable.admin.v2.Backup.newBuilder()
            .setName(BACKUP_NAME)
            .setSourceTable(TABLE_NAME)
            .build();
    when(mockAdminClient.getBackupAsync(request)).thenReturn(Futures.immediateFuture(response));
    Future<Backup> actualResponse = clientWrapper.getBackupAsync(CLUSTER_ID, BACKUP_ID);
    assertEquals(BACKUP_ID, actualResponse.get().getId());
    assertEquals(TABLE_ID, actualResponse.get().getSourceTableId());
    verify(mockAdminClient).getBackupAsync(request);
  }

  @Test
  public void testUpdateBackupAsync() throws ExecutionException, InterruptedException {
    // Setup
    Timestamp expireTime = Timestamp.newBuilder().setSeconds(123456789).build();
    long sizeBytes = 12345L;
    UpdateBackupRequest req = UpdateBackupRequest.of(CLUSTER_ID, BACKUP_ID);
    com.google.bigtable.admin.v2.Backup response =
        com.google.bigtable.admin.v2.Backup.newBuilder()
            .setName(NameUtil.formatBackupName(PROJECT_ID, INSTANCE_ID, CLUSTER_ID, BACKUP_ID))
            .setSourceTable(NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID))
            .setExpireTime(expireTime)
            .setSizeBytes(sizeBytes)
            .build();
    Mockito.when(mockAdminClient.updateBackupAsync(req.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(Futures.immediateFuture(response));

    Backup actualResult = clientWrapper.updateBackupAsync(req).get();

    assertEquals(actualResult.getId(), BACKUP_ID);
    assertEquals(actualResult.getSourceTableId(), TABLE_ID);
    assertEquals(
        actualResult.getExpireTime(), Instant.ofEpochMilli(Timestamps.toMillis(expireTime)));
    assertEquals(actualResult.getSizeBytes(), sizeBytes);
  }

  @Test
  public void testListBackupsAsync() throws Exception {
    ListBackupsRequest request =
        ListBackupsRequest.newBuilder().setParent(CLUSTER_NAME.toString()).build();
    ListBackupsResponse response =
        ListBackupsResponse.newBuilder()
            .addBackups(
                com.google.bigtable.admin.v2.Backup.newBuilder()
                    .setName(BACKUP_NAME)
                    .setSourceTable(TABLE_NAME)
                    .build())
            .build();
    when(mockAdminClient.listBackupsAsync(request)).thenReturn(Futures.immediateFuture(response));
    Future<List<String>> actualResponse = clientWrapper.listBackupsAsync(CLUSTER_ID);
    assertEquals(response.getBackupsCount(), actualResponse.get().size());
    verify(mockAdminClient).listBackupsAsync(request);
  }

  @Test
  public void testDeleteBackupAsync() {
    DeleteBackupRequest request = DeleteBackupRequest.newBuilder().setName(BACKUP_NAME).build();
    when(mockAdminClient.deleteBackupAsync(request))
        .thenReturn(Futures.immediateFuture(Empty.getDefaultInstance()));
    clientWrapper.deleteBackupAsync(CLUSTER_ID, BACKUP_ID);
    verify(mockAdminClient).deleteBackupAsync(request);
  }

  @Test
  public void testRestoreTableAsync() throws Exception {
    RestoreTableRequest request =
        RestoreTableRequest.of(CLUSTER_ID, BACKUP_ID).setTableId(TABLE_ID);
    com.google.bigtable.admin.v2.RestoreTableRequest requestProto =
        request.toProto(PROJECT_ID, INSTANCE_ID);
    Operation op =
        Operation.newBuilder()
            .setMetadata(Any.pack(RestoreTableMetadata.newBuilder().setName(BACKUP_NAME).build()))
            .setResponse(
                Any.pack(
                    com.google.bigtable.admin.v2.Table.newBuilder().setName(TABLE_NAME).build()))
            .build();
    when(mockAdminClient.restoreTableAsync(requestProto)).thenReturn(Futures.immediateFuture(op));
    Future<RestoredTableResult> actualResponse = clientWrapper.restoreTableAsync(request);
    assertEquals(TABLE_ID, actualResponse.get().getTable().getId());
    verify(mockAdminClient).restoreTableAsync(requestProto);
  }

  private static com.google.bigtable.admin.v2.Table createTableData() {
    GCRules.GCRule gcRule = GCRULES.maxVersions(1);
    ColumnFamily columnFamily = ColumnFamily.newBuilder().setGcRule(gcRule.toProto()).build();

    return com.google.bigtable.admin.v2.Table.newBuilder()
        .setName(TABLE_NAME)
        .putColumnFamilies(COLUMN_FAMILY, columnFamily)
        .build();
  }
}
