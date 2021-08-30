/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.*;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.ListClustersResponse.Builder;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.*;
import com.google.cloud.bigtable.admin.v2.models.Backup;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestAdminClientClassicApi {

  private static final String PROJECT_ID = "fake-project-id";
  private static final String INSTANCE_ID = "fake-instance-id";
  private static final BigtableInstanceName INSTANCE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID);

  private static final String TABLE_ID = "fake-Table-id";
  private static final String CLUSTER_ID = "fake-cluster-id";
  private static final String TABLE_NAME =
      NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID);

  private static final String COLUMN_FAMILY = "myColumnFamily";
  private static final String ROW_KEY_PREFIX = "row-key-val";
  private static final String UPDATE_FAMILY = "update-family";
  private static final String BACKUP_ID = "fake-backup-id";
  private static final BigtableClusterName CLUSTER_NAME = INSTANCE_NAME.toClusterName(CLUSTER_ID);
  private static final String BACKUP_NAME = CLUSTER_NAME.toBackupName(BACKUP_ID);

  private BigtableTableAdminClient tableDelegate;
  private BigtableInstanceClient instanceDelegate;

  private AdminClientWrapper adminClientWrapper;

  @Before
  public void setUp() {
    tableDelegate = Mockito.mock(BigtableTableAdminClient.class);
    instanceDelegate = Mockito.mock(BigtableInstanceClient.class);
    adminClientWrapper = new AdminClientClassicApi(tableDelegate, INSTANCE_NAME, instanceDelegate);
  }

  @Test
  public void testCreateTableAsync() throws Exception {
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);

    when(tableDelegate.createTableAsync(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(immediateFuture(createTableData()));

    assertEquals(
        Table.fromProto(createTableData()), adminClientWrapper.createTableAsync(request).get());
    verify(tableDelegate).createTableAsync(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void testGetTableAsync() throws Exception {
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();
    when(tableDelegate.getTableAsync(request)).thenReturn(immediateFuture(createTableData()));

    assertEquals(
        Table.fromProto(createTableData()), adminClientWrapper.getTableAsync(TABLE_ID).get());
    verify(tableDelegate).getTableAsync(request);
  }

  @Test
  public void testListTablesAsync() throws Exception {
    ImmutableList<String> tableIdList =
        ImmutableList.of("test-table-1", "test-table-2", "test-table-3");
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(INSTANCE_NAME.toString()).build();
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-1")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-2")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-3")).build();
    when(tableDelegate.listTablesAsync(request)).thenReturn(immediateFuture(builder.build()));

    assertEquals(tableIdList, adminClientWrapper.listTablesAsync().get());
    verify(tableDelegate).listTablesAsync(request);
  }

  @Test
  public void testDeleteTableAsync() throws ExecutionException, InterruptedException {
    DeleteTableRequest request = DeleteTableRequest.newBuilder().setName(TABLE_NAME).build();
    when(tableDelegate.deleteTableAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    adminClientWrapper.deleteTableAsync(TABLE_ID).get();

    verify(tableDelegate).deleteTableAsync(request);
  }

  @Test
  public void testModifyFamiliesAsync() throws Exception {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID)
            .addFamily(COLUMN_FAMILY, GCRULES.maxVersions(1))
            .updateFamily(UPDATE_FAMILY, GCRULES.maxAge(Duration.ofHours(100)));

    when(tableDelegate.modifyColumnFamilyAsync(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(immediateFuture(createTableData()));
    Future<Table> response = adminClientWrapper.modifyFamiliesAsync(request);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(tableDelegate).modifyColumnFamilyAsync(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void dropRowRangeAsyncForDeleteByPrefix() throws ExecutionException, InterruptedException {
    ByteString rowKey = ByteString.copyFrom(new byte[] {0, -32, 122, 13});
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(false)
            .setRowKeyPrefix(rowKey)
            .build();

    when(tableDelegate.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    adminClientWrapper.dropRowRangeAsync(TABLE_ID, rowKey).get();

    verify(tableDelegate).dropRowRangeAsync(request);
  }

  @Test
  public void dropRowRangeAsyncForTruncate() throws ExecutionException, InterruptedException {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(true)
            .build();

    when(tableDelegate.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    adminClientWrapper.dropAllRowsAsync(TABLE_ID).get();

    verify(tableDelegate).dropRowRangeAsync(request);
  }

  @Test
  public void listClusters() {
    ListClustersRequest request =
        ListClustersRequest.newBuilder().setParent(INSTANCE_NAME.getInstanceName()).build();
    Builder builder = ListClustersResponse.newBuilder();
    Cluster cluster = Cluster.newBuilder().setName(CLUSTER_ID).build();
    builder.addClusters(cluster);
    ListClustersResponse listClustersResponse = builder.build();
    when(instanceDelegate.listCluster(request)).thenReturn(listClustersResponse);

    List<com.google.cloud.bigtable.admin.v2.models.Cluster> expected = new ArrayList<>();
    expected.add(com.google.cloud.bigtable.admin.v2.models.Cluster.fromProto(cluster));

    List<com.google.cloud.bigtable.admin.v2.models.Cluster> actual =
        adminClientWrapper.listClusters(INSTANCE_ID);
    assertEquals(expected, actual);
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
    when(tableDelegate.waitForOperation(op)).thenReturn(op);
    when(tableDelegate.createBackupAsync(requestProto)).thenReturn(Futures.immediateFuture(op));
    Future<Backup> actualResponse = adminClientWrapper.createBackupAsync(request);
    assertEquals(BACKUP_ID, actualResponse.get().getId());
    verify(tableDelegate).createBackupAsync(requestProto);
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
    when(tableDelegate.listBackupsAsync(request)).thenReturn(Futures.immediateFuture(response));
    Future<List<String>> actualResponse = adminClientWrapper.listBackupsAsync(CLUSTER_ID);
    assertEquals(response.getBackupsCount(), actualResponse.get().size());
    verify(tableDelegate).listBackupsAsync(request);
  }

  @Test
  public void testDeleteBackupAsync() {
    DeleteBackupRequest request = DeleteBackupRequest.newBuilder().setName(BACKUP_NAME).build();
    when(tableDelegate.deleteBackupAsync(request))
        .thenReturn(Futures.immediateFuture(Empty.getDefaultInstance()));
    adminClientWrapper.deleteBackupAsync(CLUSTER_ID, BACKUP_ID);
    verify(tableDelegate).deleteBackupAsync(request);
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
    when(tableDelegate.waitForOperation(op)).thenReturn(op);
    when(tableDelegate.restoreTableAsync(requestProto)).thenReturn(Futures.immediateFuture(op));
    Future<RestoredTableResult> actualResponse = adminClientWrapper.restoreTableAsync(request);
    assertEquals(TABLE_ID, actualResponse.get().getTable().getId());
    verify(tableDelegate).restoreTableAsync(requestProto);
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
