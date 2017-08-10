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

import com.google.common.primitives.Ints;
import java.io.IOException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.bigtable.admin.v2.CheckConsistencyRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest;
import com.google.common.annotations.VisibleForTesting;
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

  /**
   * <p>Constructor for BigtableTableAdminGrpcClient.</p>
   *
   * @param channel a {@link io.grpc.Channel} object.
   */
  public BigtableTableAdminGrpcClient(Channel channel,
      ScheduledExecutorService retryExecutorService, BigtableOptions bigtableOptions) {
    BigtableAsyncUtilities asyncUtilities = new BigtableAsyncUtilities.Default(channel);

    // Read only methods.  These are always retried.
    this.listTablesRpc = asyncUtilities.createAsyncRpc(BigtableTableAdminGrpc.METHOD_LIST_TABLES,
      Predicates.<ListTablesRequest> alwaysTrue());
    this.getTableRpc = asyncUtilities.createAsyncRpc(BigtableTableAdminGrpc.METHOD_GET_TABLE,
      Predicates.<GetTableRequest> alwaysTrue());

    // Write methods. These are only retried for UNAVAILABLE or UNAUTHORIZED
    this.createTableRpc = asyncUtilities.createAsyncRpc(BigtableTableAdminGrpc.METHOD_CREATE_TABLE,
      Predicates.<CreateTableRequest> alwaysFalse());
    this.modifyColumnFamilyRpc =
        asyncUtilities.createAsyncRpc(BigtableTableAdminGrpc.METHOD_MODIFY_COLUMN_FAMILIES,
          Predicates.<ModifyColumnFamiliesRequest> alwaysFalse());
    this.deleteTableRpc = asyncUtilities.createAsyncRpc(BigtableTableAdminGrpc.METHOD_DELETE_TABLE,
      Predicates.<DeleteTableRequest> alwaysFalse());
    this.dropRowRangeRpc = asyncUtilities.createAsyncRpc(BigtableTableAdminGrpc.METHOD_DROP_ROW_RANGE,
      Predicates.<DropRowRangeRequest> alwaysFalse());
    this.generateConsistencyTokenRpc = asyncUtilities.createAsyncRpc(
        BigtableTableAdminGrpc.METHOD_GENERATE_CONSISTENCY_TOKEN,
        Predicates.<GenerateConsistencyTokenRequest> alwaysFalse());
    this.checkConsistencyRpc = asyncUtilities.createAsyncRpc(
        BigtableTableAdminGrpc.METHOD_CHECK_CONSISTENCY,
        Predicates.<CheckConsistencyRequest> alwaysFalse());

    this.retryOptions = bigtableOptions.getRetryOptions();
    this.retryExecutorService = retryExecutorService;
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
}
