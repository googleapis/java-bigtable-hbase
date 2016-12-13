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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.BigtableDataGrpcClient;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncUtilities;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBigtableDataGrpcClient {

  private static final String TABLE_NAME =
      new BigtableInstanceName("projectId", "instanceId").toTableNameStr("tableId");

  private static final GoogleCloudResourcePrefixInterceptor interceptor =
      new GoogleCloudResourcePrefixInterceptor("Value we don't want");


  private static final BigtableAsyncRpc.RpcMetrics metrics =
      BigtableAsyncRpc.RpcMetrics.createRpcMetrics(BigtableGrpc.METHOD_READ_ROWS);

  @Mock
  ChannelPool mockChannelPool;

  @Mock
  ClientCall mockClientCall;

  @Mock
  BigtableAsyncUtilities mockAsyncUtilities;

  @Mock
  ListenableFuture mockFuture;

  @Mock
  ScheduledExecutorService executorService;

  @Mock
  NanoClock nanoClock;

  @Mock
  BigtableAsyncRpc mockBigtableRpc;

  Map<String, Predicate> predicates;

  private Metadata tableMetadata;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockChannelPool.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(mockClientCall);

    when(mockBigtableRpc.getRpcMetrics()).thenReturn(metrics);
    predicates = new HashMap<>();
    Answer<BigtableAsyncRpc> answer =
        new Answer<BigtableAsyncRpc>() {
          @Override
          public BigtableAsyncRpc answer(InvocationOnMock invocation) throws Throwable {
            MethodDescriptor descriptor = invocation.getArgumentAt(0, MethodDescriptor.class);
            String fullMethodName = descriptor.getFullMethodName();
            if (invocation.getArguments().length > 1) {
              predicates.put(fullMethodName, invocation.getArgumentAt(1, Predicate.class));
            }
            return mockBigtableRpc;
          }
        };
    when(mockAsyncUtilities.createAsyncRpc(any(MethodDescriptor.class), any(Predicate.class)))
        .thenAnswer(answer);

    when(mockBigtableRpc.newCall(any(CallOptions.class))).thenReturn(mockClientCall);
    tableMetadata = new Metadata();
    tableMetadata.put(GoogleCloudResourcePrefixInterceptor.GRPC_RESOURCE_PREFIX_KEY, TABLE_NAME);
  }

  protected BigtableDataGrpcClient createClient(boolean allowRetriesWithoutTimestamp) {
    RetryOptions retryOptions =
        RetryOptionsUtil.createTestRetryOptions(nanoClock, allowRetriesWithoutTimestamp);
    BigtableOptions options = new BigtableOptions.Builder().setRetryOptions(retryOptions).build();
    return new BigtableDataGrpcClient(executorService, options, mockAsyncUtilities);
  }

  @Test
  public void testRetyableMutateRow() throws Exception {
    MutateRowRequest request = MutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    setResponse(MutateRowResponse.getDefaultInstance());
    createClient(false).mutateRow(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testRetyableMutateRowAsync() throws InterruptedException, ExecutionException {
    MutateRowRequest request = MutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    when(mockFuture.get()).thenReturn(MutateRowsResponse.getDefaultInstance());
    createClient(false).mutateRowAsync(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testRetyableCheckAndMutateRow() throws Exception {
    CheckAndMutateRowRequest request =
        CheckAndMutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    setResponse(CheckAndMutateRowResponse.getDefaultInstance());
    createClient(false).checkAndMutateRow(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testRetyableCheckAndMutateRowAsync() {
    CheckAndMutateRowRequest request =
        CheckAndMutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    createClient(false).checkAndMutateRowAsync(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testMutateRowPredicate() {
    Predicate<MutateRowRequest> defaultPredicate = BigtableDataGrpcClient.IS_RETRYABLE_MUTATION;
    createClient(true);
    Predicate<MutateRowRequest> allowNoTimestampsPredicate =
        predicates.get(BigtableGrpc.METHOD_MUTATE_ROW.getFullMethodName());

    assertFalse(defaultPredicate.apply(null));
    assertTrue(allowNoTimestampsPredicate.apply(null));

    MutateRowRequest noDataRequest = MutateRowRequest.getDefaultInstance();
    assertTrue(defaultPredicate.apply(noDataRequest));
    assertTrue(allowNoTimestampsPredicate.apply(noDataRequest));

    MutateRowRequest requestWithCells = MutateRowRequest.newBuilder()
        .addMutations(Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)))
        .build();
    assertFalse(defaultPredicate.apply(requestWithCells));
    assertTrue(allowNoTimestampsPredicate.apply(requestWithCells));
  }

  @Test
  public void testMutateRowsPredicate() {
    Predicate<MutateRowsRequest> predicate = BigtableDataGrpcClient.ARE_RETRYABLE_MUTATIONS;
    assertFalse(predicate.apply(null));

    MutateRowsRequest.Builder request = MutateRowsRequest.newBuilder();
    assertTrue(predicate.apply(request.build()));

    request.addEntries(Entry.newBuilder().addMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1))));
    assertFalse(predicate.apply(request.build()));
  }

  @Test
  public void testCheckAndMutateRowPredicate() {
    Predicate<CheckAndMutateRowRequest> predicate =
        BigtableDataGrpcClient.IS_RETRYABLE_CHECK_AND_MUTATE;
    assertFalse(predicate.apply(null));

    CheckAndMutateRowRequest.Builder request = CheckAndMutateRowRequest.newBuilder();
    assertTrue(predicate.apply(request.build()));

    request.addTrueMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(predicate.apply(request.build()));

    request.clearTrueMutations();
    request.addFalseMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(predicate.apply(request.build()));
  }

  @Test
  public void testSingleRowRead() {
    ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder().setTableName(TABLE_NAME);
    requestBuilder.getRowsBuilder().addRowKeys(ByteString.copyFrom(new byte[0]));
    createClient(false).readRows(requestBuilder.build());
    verifyRequestCalled(requestBuilder.build());
  }

  @Test
  public void testMultiRowRead() {
    ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder().setTableName(TABLE_NAME);
    requestBuilder.getRowsBuilder().addRowRanges(RowRange.getDefaultInstance());
    createClient(false).readRows(requestBuilder.build());
    verifyRequestCalled(requestBuilder.build());
  }

  private void setResponse(final Object response) {
    Answer<Void> answer =
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            invocation.getArgumentAt(2, ClientCall.Listener.class).onMessage(response);
            Metadata metadata = invocation.getArgumentAt(3, Metadata.class);
            interceptor.updateHeaders(metadata);
            String headerValue =
                metadata.get(GoogleCloudResourcePrefixInterceptor.GRPC_RESOURCE_PREFIX_KEY);
            Assert.assertEquals(TABLE_NAME, headerValue);
            return null;
          }
        };
    doAnswer(answer)
        .when(mockBigtableRpc)
        .start(same(mockClientCall),
            any(),
            any(ClientCall.Listener.class), 
            any(Metadata.class));
  }

  private void verifyRequestCalled(Object request) {
    verify(mockBigtableRpc, times(1))
        .newCall(any(CallOptions.class));
    verify(mockBigtableRpc, times(1))
        .start(
            same(mockClientCall),
            eq(request),
            any(ClientCall.Listener.class),
            any(Metadata.class));
  }
}
