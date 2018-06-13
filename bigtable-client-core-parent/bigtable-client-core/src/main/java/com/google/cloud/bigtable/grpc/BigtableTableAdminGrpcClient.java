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
package com.google.cloud.bigtable.grpc;

import static com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor.GRPC_RESOURCE_PREFIX_KEY;

import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.common.base.Preconditions;
import com.google.longrunning.Operation;
import com.google.common.primitives.Ints;
import java.io.IOException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.bigtable.admin.v2.CheckConsistencyRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.admin.v2.CheckConsistencyResponse;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenResponse;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncUtilities;
import com.google.cloud.bigtable.grpc.async.RetryingUnaryOperation;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;

/**
 * A gRPC client for accessing the Bigtable Table Admin API.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableTableAdminGrpcClient implements BigtableTableAdminClient {

  private final BigtableAsyncRpc<ListTablesRequest, ListTablesResponse> listTablesRpc;
  private final RetryOptions retryOptions;
  private final ScheduledExecutorService retryExecutorService;
  private final BigtableAsyncRpc<GetTableRequest, Table> getTableRpc;
  private final BigtableAsyncRpc<CreateTableRequest, Table> createTableRpc;
  private final BigtableAsyncRpc<ModifyColumnFamiliesRequest, Table> modifyColumnFamilyRpc;
  private final BigtableAsyncRpc<DeleteTableRequest, Empty> deleteTableRpc;
  private final BigtableAsyncRpc<DropRowRangeRequest, Empty> dropRowRangeRpc;
  private final BigtableAsyncRpc<GenerateConsistencyTokenRequest, GenerateConsistencyTokenResponse> generateConsistencyTokenRpc;
  private final BigtableAsyncRpc<CheckConsistencyRequest, CheckConsistencyResponse> checkConsistencyRpc;

  private final BigtableAsyncRpc<SnapshotTableRequest, Operation> snapshotTableRpc;
  private final BigtableAsyncRpc<GetSnapshotRequest, Snapshot> getSnapshotRpc;
  private final BigtableAsyncRpc<ListSnapshotsRequest, ListSnapshotsResponse> listSnapshotsRpc;
  private final BigtableAsyncRpc<DeleteSnapshotRequest, Empty> deleteSnapshotRpc;
  private final BigtableAsyncRpc<CreateTableFromSnapshotRequest, Operation> createTableFromSnapshotRpc;

  /**
   * <p>Constructor for BigtableTableAdminGrpcClient.</p>
   *
   * @param channel a {@link io.grpc.Channel} object.
   */
  public BigtableTableAdminGrpcClient(Channel channel,
      ScheduledExecutorService retryExecutorService, BigtableOptions bigtableOptions) {
    BigtableAsyncUtilities asyncUtilities = new BigtableAsyncUtilities.Default(channel);

    // grpc static variable method descriptors are deprecated.  They are going to be replaced
    // with static methods.  Once the new static methods exist, then we should use those.
    // For now, use a hacky approach to get the method descrptors so that all environments work.
    //
    // See https://github.com/grpc/grpc-java/issues/1901 for more details.
    ServerServiceDefinition definition = new BigtableTableAdminGrpc.BigtableTableAdminImplBase() {}
      .bindService();

    // Read only methods.  These are always retried.
    this.listTablesRpc = asyncUtilities.createAsyncRpc(
        this.<ListTablesRequest, ListTablesResponse>
            getMethod(definition, "ListTables"),
        Predicates.<ListTablesRequest> alwaysTrue());
    this.getTableRpc = asyncUtilities.createAsyncRpc(
        this.<GetTableRequest, Table>
            getMethod(definition, "GetTable"),
       Predicates.<GetTableRequest> alwaysTrue());

    // Write methods. These are only retried for UNAVAILABLE or UNAUTHORIZED
    this.createTableRpc = asyncUtilities.createAsyncRpc(
        this.<CreateTableRequest, Table>
            getMethod(definition, "CreateTable"),
        Predicates.<CreateTableRequest> alwaysFalse());
    this.modifyColumnFamilyRpc =
        asyncUtilities.createAsyncRpc(
            this.<ModifyColumnFamiliesRequest, Table>
                getMethod(definition, "ModifyColumnFamilies"),
            Predicates.<ModifyColumnFamiliesRequest> alwaysFalse());
    this.deleteTableRpc = asyncUtilities.createAsyncRpc(
        this.<DeleteTableRequest, Empty>
            getMethod(definition, "DeleteTable"),
        Predicates.<DeleteTableRequest> alwaysFalse());
    this.dropRowRangeRpc = asyncUtilities.createAsyncRpc(
        this.<DropRowRangeRequest, Empty>
            getMethod(definition, "DropRowRange"),
        Predicates.<DropRowRangeRequest> alwaysFalse());
    this.generateConsistencyTokenRpc = asyncUtilities.createAsyncRpc(
        this.<GenerateConsistencyTokenRequest, GenerateConsistencyTokenResponse>
            getMethod(definition, "GenerateConsistencyToken"),
        Predicates.<GenerateConsistencyTokenRequest> alwaysFalse());
    this.checkConsistencyRpc = asyncUtilities.createAsyncRpc(
        this.<CheckConsistencyRequest, CheckConsistencyResponse>
            getMethod(definition, "CheckConsistency"),
        Predicates.<CheckConsistencyRequest> alwaysFalse());

    this.snapshotTableRpc = asyncUtilities.createAsyncRpc(
        this.<SnapshotTableRequest, Operation>
            getMethod(definition, "SnapshotTable"),
        Predicates.<SnapshotTableRequest>alwaysFalse());

    this.getSnapshotRpc = asyncUtilities.createAsyncRpc(
        this.<GetSnapshotRequest, Snapshot>
            getMethod(definition, "GetSnapshot"),
        Predicates.<GetSnapshotRequest>alwaysTrue());

    this.listSnapshotsRpc = asyncUtilities.createAsyncRpc(
        this.<ListSnapshotsRequest, ListSnapshotsResponse>
            getMethod(definition, "ListSnapshots"),
        Predicates.<ListSnapshotsRequest>alwaysTrue());

    this.deleteSnapshotRpc = asyncUtilities.createAsyncRpc(
        this.<DeleteSnapshotRequest, Empty>
            getMethod(definition, "DeleteSnapshot"),
        Predicates.<DeleteSnapshotRequest>alwaysFalse());

    this.createTableFromSnapshotRpc = asyncUtilities.createAsyncRpc(
        this.<CreateTableFromSnapshotRequest, Operation>
            getMethod(definition, "CreateTableFromSnapshot"),
        Predicates.<CreateTableFromSnapshotRequest>alwaysFalse());

    this.retryOptions = bigtableOptions.getRetryOptions();
    this.retryExecutorService = retryExecutorService;
  }

  private static <Req, Resp> MethodDescriptor<Req, Resp> getMethod(ServerServiceDefinition def, String methodName) {
    ServerMethodDefinition methodDescriptor =
        def.getMethod("google.bigtable.admin.v2.BigtableTableAdmin/" + methodName);
    return (MethodDescriptor<Req, Resp>)
        Preconditions.checkNotNull(methodDescriptor.getMethodDescriptor());
  }

  /** {@inheritDoc} */
  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    return createUnaryListener(request, listTablesRpc, request.getParent()).getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<ListTablesResponse> listTablesAsync(ListTablesRequest request) {
    return createUnaryListener(request, listTablesRpc, request.getParent()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(GetTableRequest request) {
    return createUnaryListener(request, getTableRpc, request.getName()).getBlockingResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Table> getTableAsync(GetTableRequest request) {
    return createUnaryListener(request, getTableRpc, request.getName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public void createTable(CreateTableRequest request) {
    createUnaryListener(request, createTableRpc, request.getParent()).getBlockingResult();
  }

  /** {@inheritDoc}
   * @return */
  @Override
  public ListenableFuture<Table> createTableAsync(CreateTableRequest request) {
    return createUnaryListener(request, createTableRpc, request.getParent()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public Table modifyColumnFamily(ModifyColumnFamiliesRequest request) {
    return createUnaryListener(request, modifyColumnFamilyRpc, request.getName())
        .getBlockingResult();
  }

  /** {@inheritDoc}
   * @return */
  @Override
  public ListenableFuture<Table> modifyColumnFamilyAsync(ModifyColumnFamiliesRequest request) {
    return createUnaryListener(request, modifyColumnFamilyRpc, request.getName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public void deleteTable(DeleteTableRequest request) {
    createUnaryListener(request, deleteTableRpc, request.getName()).getBlockingResult();
  }

  /** {@inheritDoc}
   * @return */
  @Override
  public ListenableFuture<Empty> deleteTableAsync(DeleteTableRequest request) {
    return createUnaryListener(request, deleteTableRpc, request.getName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public void dropRowRange(DropRowRangeRequest request) {
    createUnaryListener(request, dropRowRangeRpc, request.getName()).getBlockingResult();
  }

  /** {@inheritDoc}
   * @return */
  @Override
  public ListenableFuture<Empty> dropRowRangeAsync(DropRowRangeRequest request) {
    return createUnaryListener(request, dropRowRangeRpc, request.getName()).getAsyncResult();
  }


  /** {@inheritDoc} */
  @Override
  public void waitForReplication(BigtableTableName tableName, long timeout) throws InterruptedException, TimeoutException {
    // A backoff that randomizes with an interval of 10s.
    ExponentialBackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(10 * 1000)
        .setMaxIntervalMillis(10 * 1000)
        .setMaxElapsedTimeMillis(Ints.checkedCast(timeout * 1000))
        .build();

    waitForReplication(tableName, backOff);
  }

  @VisibleForTesting
  void waitForReplication(BigtableTableName tableName, BackOff backOff) throws InterruptedException, TimeoutException {
    String token = generateConsistencyToken(tableName);

    while (!checkConsistency(tableName, token)) {
      long backOffMillis;
      try {
        backOffMillis = backOff.nextBackOffMillis();
      } catch (IOException e) {
        // Should never happen, we only use ExponentialBackOff which doesn't throw.
        throw new RuntimeException("Problem getting backoff: " + e);
      }
      if (backOffMillis == BackOff.STOP) {
        throw new TimeoutException(
            "Table " + tableName.toString() + " is not consistent after timeout.");

      } else {
        // sleep for backOffMillis milliseconds and retry operation.
        Thread.sleep(backOffMillis);
      }
    }
  }

  private String generateConsistencyToken(BigtableTableName tableName) {
    GenerateConsistencyTokenRequest request =
        GenerateConsistencyTokenRequest.newBuilder().setName(tableName.toString()).build();

    return createUnaryListener(request, generateConsistencyTokenRpc, request.getName())
        .getBlockingResult()
        .getConsistencyToken();
  }

  private boolean checkConsistency(BigtableTableName tableName, String token) {
    CheckConsistencyRequest request = CheckConsistencyRequest.newBuilder()
        .setName(tableName.toString())
        .setConsistencyToken(token)
        .build();

    return createUnaryListener(request, checkConsistencyRpc, request.getName())
        .getBlockingResult()
        .getConsistent();
  }

  private <ReqT, RespT> RetryingUnaryOperation<ReqT, RespT> createUnaryListener(
      ReqT request, BigtableAsyncRpc<ReqT, RespT> rpc, String resource) {
    CallOptions callOptions = CallOptions.DEFAULT;
    Metadata metadata = createMetadata(resource);
    return new RetryingUnaryOperation<>(
        retryOptions, request, rpc, callOptions, retryExecutorService, metadata);
  }

  /**
   * Creates a {@link Metadata} that contains pertinent headers.
   */
  private Metadata createMetadata(String resource) {
    Metadata metadata = new Metadata();
    if (resource != null) {
      metadata.put(GRPC_RESOURCE_PREFIX_KEY, resource);
    }
    return metadata;
  }


  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Operation> snapshotTableAsync(SnapshotTableRequest request) {
    return createUnaryListener(request, snapshotTableRpc, request.getName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Snapshot> getSnapshotAsync(GetSnapshotRequest request) {
    return createUnaryListener(request, getSnapshotRpc, request.getName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<ListSnapshotsResponse> listSnapshotsAsync(ListSnapshotsRequest request) {
    return createUnaryListener(request, listSnapshotsRpc, request.getParent()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Empty> deleteSnapshotAsync(DeleteSnapshotRequest request) {
    return createUnaryListener(request, deleteSnapshotRpc, request.getName()).getAsyncResult();
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Operation> createTableFromSnapshotAsync(CreateTableFromSnapshotRequest request) {
    return createUnaryListener(request, createTableFromSnapshotRpc, request.getParent()).getAsyncResult();
  }
}
