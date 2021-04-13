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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static org.junit.Assert.assertEquals;

import com.google.bigtable.admin.v2.*;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.common.collect.Queues;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAdminClientVeneerApi {

  private static final String PROJECT_ID = "fake-project-id";
  private static final String INSTANCE_ID = "fake-instance-id";
  private static final String TABLE_ID_1 = "fake-Table-id-1";
  private static final String TABLE_ID_2 = "fake-Table-id-2";
  private static final String TABLE_NAME =
      NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID_1);
  private static final BigtableInstanceName INSTANCE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID);
  private static final String CLUSTER_ID = "fake-cluster-id";
  private static final String BACKUP_ID = "fake-backup-id";
  private static final BigtableClusterName CLUSTER_NAME = INSTANCE_NAME.toClusterName(CLUSTER_ID);
  private static final String BACKUP_NAME = CLUSTER_NAME.toBackupName(BACKUP_ID);

  private FakeBigtableAdmin fakeAdminService = new FakeBigtableAdmin();
  private Server server;
  private BigtableTableAdminClient adminClientV2;
  private AdminClientVeneerApi adminClientWrapper;

  @Before
  public void setUp() throws Exception {
    final int port;
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }

    server = ServerBuilder.forPort(port).addService(fakeAdminService).build();
    server.start();
    adminClientV2 =
        BigtableTableAdminClient.create(
            BigtableTableAdminSettings.newBuilderForEmulator(port)
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build());

    adminClientWrapper = new AdminClientVeneerApi(adminClientV2, null);
  }

  @After
  public void tearDown() throws Exception {
    adminClientWrapper.close();
    adminClientV2.close();

    if (server != null) {
      server.shutdown();
      server.awaitTermination();
    }
  }

  @Test
  public void testCreateTableAsync() throws Exception {
    CreateTableRequest req = CreateTableRequest.of(TABLE_ID_1);
    Future<Table> response = adminClientWrapper.createTableAsync(req);
    assertEquals(TABLE_ID_1, response.get().getId());
  }

  @Test
  public void testGetTableAsync() throws Exception {
    Future<Table> response = adminClientWrapper.getTableAsync("test-async-table");
    assertEquals(TABLE_ID_1, response.get().getId());
  }

  @Test
  public void testListTablesAsync() throws Exception {
    Future<List<String>> tableIds = adminClientWrapper.listTablesAsync();
    assertEquals(Arrays.asList(TABLE_ID_1, TABLE_ID_2), tableIds.get());
  }

  @Test
  public void testDeleteTableAsync() throws Exception {
    Future<Void> deleteResponse = adminClientWrapper.deleteTableAsync("deleteAsyncTableID");
    deleteResponse.get();
    DeleteTableRequest deleteTableReq = fakeAdminService.popLastRequest();
    assertEquals(
        "deleteAsyncTableID", NameUtil.extractTableIdFromTableName(deleteTableReq.getName()));
  }

  @Test
  public void testModifyFamilyAsync() throws Exception {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID_1)
            .addFamily("first-family")
            .addFamily("another-family");
    Future<Table> response = adminClientWrapper.modifyFamiliesAsync(request);
    assertEquals(TABLE_ID_1, response.get().getId());
  }

  @Test
  public void dropRowRangeAsync() throws Exception {
    ByteString rowKey = ByteString.copyFromUtf8("cf-dropRange-async");
    adminClientWrapper.dropRowRangeAsync(TABLE_ID_1, rowKey).get();
    DropRowRangeRequest rangeRequest = fakeAdminService.popLastRequest();
    assertEquals(TABLE_ID_1, NameUtil.extractTableIdFromTableName(rangeRequest.getName()));
    assertEquals(rowKey, rangeRequest.getRowKeyPrefix());
  }

  @Test
  public void dropAllRowsAsync() throws Exception {
    String tableId = "tableWithNoDataId";
    adminClientWrapper.dropAllRowsAsync(tableId).get();
    DropRowRangeRequest rangeRequest = fakeAdminService.popLastRequest();
    assertEquals(tableId, NameUtil.extractTableIdFromTableName(rangeRequest.getName()));
  }

  @Test
  public void listBackupsAsync() throws ExecutionException, InterruptedException {
    List<String> listApiFuture = adminClientWrapper.listBackupsAsync(CLUSTER_ID).get();
    assertEquals(1, listApiFuture.size());
    assertEquals("fake-backup-id", listApiFuture.get(0));
  }

  @Test
  public void testDeleteBackupAsync() throws Exception {
    Future<Void> deleteResponse = adminClientWrapper.deleteBackupAsync(CLUSTER_ID, BACKUP_ID);
    deleteResponse.get();
    DeleteBackupRequest deleteBackupRequest = fakeAdminService.popLastRequest();
    assertEquals(
        "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-id",
        deleteBackupRequest.getName());
  }

  private static class FakeBigtableAdmin extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {

    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T) requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void createTable(
        com.google.bigtable.admin.v2.CreateTableRequest request,
        StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      requests.add(request);
      responseObserver.onNext(
          com.google.bigtable.admin.v2.Table.newBuilder()
              .setName(NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID_1))
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void getTable(
        GetTableRequest request,
        StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      requests.add(request);
      responseObserver.onNext(
          com.google.bigtable.admin.v2.Table.newBuilder()
              .setName(NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID_1))
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void listTables(
        ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
      requests.add(request);
      ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
      builder
          .addTablesBuilder()
          .setName(NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID_1))
          .build();
      builder
          .addTablesBuilder()
          .setName(NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID_2))
          .build();
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteTable(DeleteTableRequest request, StreamObserver<Empty> responseObserver) {
      requests.add(request);
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void modifyColumnFamilies(
        com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest request,
        StreamObserver<com.google.bigtable.admin.v2.Table> responseObserver) {
      requests.add(request);
      responseObserver.onNext(
          com.google.bigtable.admin.v2.Table.newBuilder()
              .setName(NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID_1))
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void dropRowRange(DropRowRangeRequest request, StreamObserver<Empty> responseObserver) {
      requests.add(request);
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    }

    @Override
    public void listBackups(
        ListBackupsRequest request, StreamObserver<ListBackupsResponse> responseObserver) {
      requests.add(request);
      responseObserver.onNext(
          ListBackupsResponse.newBuilder()
              .addBackups(com.google.bigtable.admin.v2.Backup.newBuilder().setName(BACKUP_NAME))
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteBackup(DeleteBackupRequest request, StreamObserver<Empty> responseObserver) {
      requests.add(request);
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    }
  }
}
