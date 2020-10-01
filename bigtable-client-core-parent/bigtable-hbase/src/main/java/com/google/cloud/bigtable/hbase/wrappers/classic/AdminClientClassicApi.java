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

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.bigtable.admin.v2.DeleteBackupRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetBackupRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListBackupsRequest;
import com.google.bigtable.admin.v2.ListBackupsResponse;
import com.google.bigtable.admin.v2.ListClustersRequest;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.Backup;
import com.google.cloud.bigtable.admin.v2.models.Cluster;
import com.google.cloud.bigtable.admin.v2.models.CreateBackupRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoreTableRequest;
import com.google.cloud.bigtable.admin.v2.models.RestoredTableResult;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.admin.v2.models.UpdateBackupRequest;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class AdminClientClassicApi implements AdminClientWrapper {

  private final BigtableTableAdminClient tableDelegate;
  private final BigtableInstanceClient instanceDelegate;
  private final BigtableInstanceName instanceName;
  private final ExecutorService batchThreadPool;

  AdminClientClassicApi(
      @Nonnull BigtableTableAdminClient tableDelegate,
      @Nonnull BigtableInstanceName instanceName,
      BigtableInstanceClient instanceDelegate) {
    this.tableDelegate = tableDelegate;
    this.instanceName = instanceName;
    this.batchThreadPool = BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool();
    this.instanceDelegate = instanceDelegate;
  }

  @Override
  public ApiFuture<Table> createTableAsync(CreateTableRequest request) {
    com.google.bigtable.admin.v2.CreateTableRequest requestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.createTableAsync(requestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        });
  }

  @Override
  public ApiFuture<Table> getTableAsync(String tableId) {
    GetTableRequest requestProto =
        GetTableRequest.newBuilder().setName(instanceName.toTableNameStr(tableId)).build();

    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.getTableAsync(requestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        });
  }

  @Override
  public ApiFuture<List<String>> listTablesAsync() {
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(instanceName.toString()).build();
    ListenableFuture<ListTablesResponse> response = tableDelegate.listTablesAsync(request);

    return ApiFutureUtil.transformAndAdapt(
        response,
        new Function<ListTablesResponse, List<String>>() {
          @Override
          public List<String> apply(ListTablesResponse input) {
            ImmutableList.Builder<String> tableIdsBuilder = ImmutableList.builder();
            for (com.google.bigtable.admin.v2.Table tableProto : input.getTablesList()) {
              tableIdsBuilder.add(instanceName.toTableId(tableProto.getName()));
            }

            return tableIdsBuilder.build();
          }
        });
  }

  @Override
  public ApiFuture<Void> deleteTableAsync(String tableId) {
    DeleteTableRequest request =
        DeleteTableRequest.newBuilder().setName(instanceName.toTableNameStr(tableId)).build();

    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.deleteTableAsync(request),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request) {
    com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest modifyColumnRequestProto =
        request.toProto(instanceName.getProjectId(), instanceName.getInstanceId());

    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.modifyColumnFamilyAsync(modifyColumnRequestProto),
        new Function<com.google.bigtable.admin.v2.Table, Table>() {
          @Override
          public Table apply(com.google.bigtable.admin.v2.Table tableProto) {
            return Table.fromProto(tableProto);
          }
        });
  }

  @Override
  public ApiFuture<Void> dropRowRangeAsync(String tableId, ByteString rowKeyPrefix) {
    Preconditions.checkNotNull(rowKeyPrefix);
    DropRowRangeRequest protoRequest =
        DropRowRangeRequest.newBuilder()
            .setName(instanceName.toTableNameStr(tableId))
            .setDeleteAllDataFromTable(false)
            .setRowKeyPrefix(rowKeyPrefix)
            .build();
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.dropRowRangeAsync(protoRequest),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<Void> dropAllRowsAsync(String tableId) {
    DropRowRangeRequest protoRequest =
        DropRowRangeRequest.newBuilder()
            .setName(instanceName.toTableNameStr(tableId))
            .setDeleteAllDataFromTable(true)
            .build();
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.dropRowRangeAsync(protoRequest),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<Backup> createBackupAsync(CreateBackupRequest request) {
    ListenableFuture<Operation> backupAsync =
        tableDelegate.createBackupAsync(
            request.toProto(instanceName.getProjectId(), instanceName.getInstanceId()));
    Function<Operation, Backup> function =
        new Function<Operation, Backup>() {
          @Override
          public Backup apply(final Operation operation) {
            try {
              Future<Operation> operationFuture =
                  batchThreadPool.submit(
                      new Callable<Operation>() {
                        @Override
                        public Operation call() throws Exception {
                          return tableDelegate.waitForOperation(operation);
                        }
                      });

              return Backup.fromProto(
                  operationFuture
                      .get()
                      .getResponse()
                      .unpack(com.google.bigtable.admin.v2.Backup.class));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("Interrupted while waiting for operation to finish");
            } catch (ExecutionException | InvalidProtocolBufferException e) {
              throw new IllegalStateException(e);
            }
          }
        };
    return ApiFutureUtil.transformAndAdapt(backupAsync, function);
  }

  @Override
  public ApiFuture<Backup> getBackupAsync(String clusterId, String backupId) {
    BigtableClusterName clusterName = instanceName.toClusterName(clusterId);
    GetBackupRequest request =
        GetBackupRequest.newBuilder().setName(clusterName.toBackupName(backupId)).build();
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.getBackupAsync(request),
        new Function<com.google.bigtable.admin.v2.Backup, Backup>() {
          @Override
          public Backup apply(com.google.bigtable.admin.v2.Backup backup) {
            return Backup.fromProto(backup);
          }
        });
  }

  @Override
  public ApiFuture<Backup> updateBackupAsync(UpdateBackupRequest request) {
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.updateBackupAsync(
            request.toProto(instanceName.getProjectId(), instanceName.getInstanceId())),
        new Function<com.google.bigtable.admin.v2.Backup, Backup>() {
          @Override
          public Backup apply(com.google.bigtable.admin.v2.Backup backup) {
            return Backup.fromProto(backup);
          }
        });
  }

  @Override
  public ApiFuture<List<String>> listBackupsAsync(String clusterId) {
    BigtableClusterName clusterName = instanceName.toClusterName(clusterId);
    ListBackupsRequest request =
        ListBackupsRequest.newBuilder().setParent(clusterName.getClusterName()).build();

    // pagination is not required as page_size in the request is not set - everything will be
    // returned
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.listBackupsAsync(request),
        new Function<ListBackupsResponse, List<String>>() {
          @Override
          public List<String> apply(ListBackupsResponse response) {
            List<String> backups = new ArrayList<>();
            for (com.google.bigtable.admin.v2.Backup backup : response.getBackupsList()) {
              backups.add(NameUtil.extractBackupIdFromBackupName(backup.getName()));
            }
            return backups;
          }
        });
  }

  @Override
  public ApiFuture<Void> deleteBackupAsync(String clusterId, String backupId) {
    BigtableClusterName clusterName = instanceName.toClusterName(clusterId);
    DeleteBackupRequest request =
        DeleteBackupRequest.newBuilder().setName(clusterName.toBackupName(backupId)).build();
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.deleteBackupAsync(request),
        new Function<Empty, Void>() {
          @Override
          public Void apply(Empty empty) {
            return null;
          }
        });
  }

  @Override
  public ApiFuture<RestoredTableResult> restoreTableAsync(RestoreTableRequest request) {
    return ApiFutureUtil.transformAndAdapt(
        tableDelegate.restoreTableAsync(
            request.toProto(instanceName.getProjectId(), instanceName.getInstanceId())),
        new Function<Operation, RestoredTableResult>() {
          @Override
          public RestoredTableResult apply(final Operation operation) {
            try {
              Future<Operation> operationFuture =
                  batchThreadPool.submit(
                      new Callable<Operation>() {
                        @Override
                        public Operation call() throws Exception {
                          return tableDelegate.waitForOperation(operation);
                        }
                      });

              return new RestoredTableResult(
                  Table.fromProto(
                      operationFuture
                          .get()
                          .getResponse()
                          .unpack(com.google.bigtable.admin.v2.Table.class)),
                  operation
                      .getMetadata()
                      .unpack(com.google.bigtable.admin.v2.RestoreTableMetadata.class)
                      .getOptimizeTableOperationName());
            } catch (IOException | InterruptedException | ExecutionException e) {
              throw new IllegalStateException(e);
            }
          }
        });
  }

  @Override
  public List<Cluster> listClusters(String instanceId) {
    ListClustersRequest request =
        ListClustersRequest.newBuilder().setParent(instanceName.getInstanceName()).build();
    ListClustersResponse listClustersResponse = instanceDelegate.listCluster(request);
    List<com.google.bigtable.admin.v2.Cluster> protoList = listClustersResponse.getClustersList();

    List<Cluster> clusters = new ArrayList<>(protoList.size());
    for (com.google.bigtable.admin.v2.Cluster value : protoList) {
      clusters.add(Cluster.fromProto(value));
    }
    return clusters;
  }

  @Override
  public void close() {}
}
