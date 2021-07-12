/*
 * Copyright 2019 Google LLC
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

import com.google.bigtable.admin.v2.Backup;
import com.google.bigtable.admin.v2.BigtableTableAdminGrpc.BigtableTableAdminImplBase;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateBackupMetadata;
import com.google.bigtable.admin.v2.CreateBackupRequest;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
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
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification;
import com.google.bigtable.admin.v2.RestoreTableMetadata;
import com.google.bigtable.admin.v2.RestoreTableRequest;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.UpdateBackupRequest;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class FakeBigtableAdminServiceImpl extends BigtableTableAdminImplBase {

  private static final BigtableInstanceName INSTANCE_NAME =
      new BigtableInstanceName("fake-project-id", "fake-instance-id");
  private static final String TEST_TABLE_ID_1 = "test-table-1";
  private static final String TEST_TABLE_ID_2 = "test-table-2";
  private static final String TEST_TABLE_ID_3 = "test-table-3";

  private final LinkedBlockingQueue<GeneratedMessageV3> requests = new LinkedBlockingQueue<>();

  @Override
  public void createTable(CreateTableRequest request, StreamObserver<Table> responseObserver) {
    responseObserver.onNext(
        Table.newBuilder().setName(INSTANCE_NAME.toTableNameStr(request.getTableId())).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTable(GetTableRequest request, StreamObserver<Table> responseObserver) {
    String tableId = NameUtil.extractTableIdFromTableName(request.getName());
    Table.Builder builder =
        Table.newBuilder()
            .setName(INSTANCE_NAME.toTableNameStr(tableId))
            .putColumnFamilies("first", ColumnFamily.getDefaultInstance());

    if ("test-async-table".equals(tableId)) {
      builder.putColumnFamilies("second", ColumnFamily.getDefaultInstance());
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void listTables(
      ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_1)).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_2)).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr(TEST_TABLE_ID_3)).build();
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteTable(DeleteTableRequest request, StreamObserver<Empty> responseObserver) {
    requests.add(request);
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void dropRowRange(DropRowRangeRequest request, StreamObserver<Empty> responseObserver) {
    requests.add(request);
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void createTableFromSnapshot(
      CreateTableFromSnapshotRequest request, StreamObserver<Operation> responseObserver) {
    responseObserver.onNext(Operation.newBuilder().setDone(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void modifyColumnFamilies(
      ModifyColumnFamiliesRequest request, StreamObserver<Table> responseObserver) {
    Table.Builder tableBuilder = Table.newBuilder().setName(request.getName());
    for (Modification mod : request.getModificationsList()) {
      tableBuilder.putColumnFamilies(mod.getId(), mod.getCreate());
    }
    responseObserver.onNext(tableBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void snapshotTable(
      SnapshotTableRequest request, StreamObserver<Operation> responseObserver) {
    responseObserver.onNext(Operation.newBuilder().setDone(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getSnapshot(GetSnapshotRequest request, StreamObserver<Snapshot> responseObserver) {
    responseObserver.onNext(Snapshot.newBuilder().setName("testSnapshotName").build());
    responseObserver.onCompleted();
  }

  @Override
  public void listSnapshots(
      ListSnapshotsRequest request, StreamObserver<ListSnapshotsResponse> responseObserver) {
    ListSnapshotsResponse listSnapshot =
        ListSnapshotsResponse.newBuilder()
            .addSnapshots(Snapshot.newBuilder().setName("firstSnapshotName").build())
            .addSnapshots(Snapshot.newBuilder().setName("secondSnapshotName").build())
            .build();
    responseObserver.onNext(listSnapshot);
    responseObserver.onCompleted();
  }

  @Override
  public void deleteSnapshot(
      DeleteSnapshotRequest request, StreamObserver<Empty> responseObserver) {
    requests.add(request);
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void createBackup(
      CreateBackupRequest request, StreamObserver<Operation> responseObserver) {
    responseObserver.onNext(
        Operation.newBuilder()
            .setMetadata(
                Any.pack(CreateBackupMetadata.newBuilder().setName(request.getBackupId()).build()))
            .setResponse(
                Any.pack(
                    com.google.bigtable.admin.v2.Backup.newBuilder()
                        .setName(
                            "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-id")
                        .setSourceTable(
                            "projects/fake-project-id/instances/fake-instance-id/tables/fake-table-id")
                        .build()))
            .setDone(true)
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getBackup(GetBackupRequest request, StreamObserver<Backup> responseObserver) {
    responseObserver.onNext(
        Backup.newBuilder()
            .setName(
                "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-id")
            .setSourceTable(
                "projects/fake-project-id/instances/fake-instance-id/tables/fake-table-id")
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void updateBackup(UpdateBackupRequest request, StreamObserver<Backup> responseObserver) {
    responseObserver.onNext(
        Backup.newBuilder()
            .setName(
                "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-id")
            .setSourceTable(
                "projects/fake-project-id/instances/fake-instance-id/tables/fake-table-id")
            .setExpireTime(request.getBackup().getExpireTime())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void listBackups(
      ListBackupsRequest request, StreamObserver<ListBackupsResponse> responseObserver) {
    ListBackupsResponse listBackups =
        ListBackupsResponse.newBuilder()
            .addBackups(
                Backup.newBuilder()
                    .setName(
                        "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-1")
                    .build())
            .addBackups(
                Backup.newBuilder()
                    .setName(
                        "projects/fake-project-id/instances/fake-instance-id/clusters/fake-cluster-id/backups/fake-backup-2")
                    .build())
            .build();
    responseObserver.onNext(listBackups);
    responseObserver.onCompleted();
  }

  @Override
  public void deleteBackup(DeleteBackupRequest request, StreamObserver<Empty> responseObserver) {
    requests.add(request);
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void restoreTable(
      RestoreTableRequest request, StreamObserver<Operation> responseObserver) {
    responseObserver.onNext(
        Operation.newBuilder()
            .setMetadata(
                Any.pack(RestoreTableMetadata.newBuilder().setName(request.getTableId()).build()))
            .setResponse(
                Any.pack(
                    com.google.bigtable.admin.v2.Table.newBuilder()
                        .setName(
                            "projects/fake-project-id/instances/fake-instance-id/tables/fake-table-id")
                        .build()))
            .setDone(true)
            .build());
    responseObserver.onCompleted();
  }

  public List<GeneratedMessageV3> getRequests() {
    return new ArrayList<>(requests);
  }
}
