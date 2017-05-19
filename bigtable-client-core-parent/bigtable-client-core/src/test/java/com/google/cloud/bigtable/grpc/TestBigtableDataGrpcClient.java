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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.BigtableDataGrpcClient;
import com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor;
import com.google.protobuf.ByteString;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBigtableDataGrpcClient {

  private static final String TABLE_NAME =
      new BigtableInstanceName("projectId", "instanceId").toTableNameStr("tableId");

  @Mock
  Channel mockChannel;

  @Mock
  ClientCall mockClientCall;

  @Mock
  NanoClock nanoClock;

  BigtableDataGrpcClient defaultClient;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(mockClientCall);
    defaultClient = createClient(false);
  }

  protected BigtableDataGrpcClient createClient(boolean allowRetriesWithoutTimestamp) {
    RetryOptions retryOptions =
        RetryOptionsUtil.createTestRetryOptions(nanoClock, allowRetriesWithoutTimestamp);
    BigtableOptions options = new BigtableOptions.Builder().setRetryOptions(retryOptions).build();
    doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        checkHeader(invocation.getArgumentAt(1, Metadata.class));
        return null;
      }
    }).when(mockClientCall).start(any(ClientCall.Listener.class), any(Metadata.class));
    return new BigtableDataGrpcClient(mockChannel, null, options);
  }

  @Test
  public void testRetyableMutateRow() throws Exception {
    MutateRowRequest request = MutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    setResponse(MutateRowResponse.getDefaultInstance());
    defaultClient.mutateRow(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testRetyableMutateRowAsync() {
    MutateRowRequest request = MutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    defaultClient.mutateRowAsync(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testRetyableCheckAndMutateRow() throws Exception {
    CheckAndMutateRowRequest request =
        CheckAndMutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    setResponse(CheckAndMutateRowResponse.getDefaultInstance());
    defaultClient.checkAndMutateRow(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testRetyableCheckAndMutateRowAsync() {
    CheckAndMutateRowRequest request =
        CheckAndMutateRowRequest.newBuilder().setTableName(TABLE_NAME).build();
    defaultClient.checkAndMutateRowAsync(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testMutateRowPredicate() {
    assertFalse(defaultClient.mutateRowRpc.isRetryable(null));

    MutateRowRequest noDataRequest = MutateRowRequest.getDefaultInstance();
    assertTrue(defaultClient.mutateRowRpc.isRetryable(noDataRequest));

    MutateRowRequest requestWithCells =
        MutateRowRequest.newBuilder()
            .addMutations(
                Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)))
            .build();
    assertFalse(defaultClient.mutateRowRpc.isRetryable(requestWithCells));
    assertTrue(createClient(true).mutateRowRpc.isRetryable(requestWithCells));
  }

  @Test
  public void testMutateRowsPredicate() {
    assertFalse(defaultClient.mutateRowsRpc.isRetryable(null));

    MutateRowsRequest.Builder request = MutateRowsRequest.newBuilder();
    assertTrue(defaultClient.mutateRowsRpc.isRetryable(request.build()));

    request.addEntries(Entry.newBuilder().addMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1))));
    assertFalse(defaultClient.mutateRowsRpc.isRetryable(request.build()));
  }

  @Test
  public void testCheckAndMutateRowPredicate() {
    assertFalse(defaultClient.checkAndMutateRpc.isRetryable(null));

    CheckAndMutateRowRequest.Builder request = CheckAndMutateRowRequest.newBuilder();
    assertTrue(defaultClient.checkAndMutateRpc.isRetryable(request.build()));

    request.addTrueMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(defaultClient.checkAndMutateRpc.isRetryable(request.build()));

    request.clearTrueMutations();
    request.addFalseMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(defaultClient.checkAndMutateRpc.isRetryable(request.build()));
  }

  @Test
  public void testSingleRowRead() {
    ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder().setTableName(TABLE_NAME);
    requestBuilder.getRowsBuilder().addRowKeys(ByteString.EMPTY);
    defaultClient.readRows(requestBuilder.build());
    verifyRequestCalled(requestBuilder.build());
  }

  @Test
  public void testMultiRowRead() {
    ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder().setTableName(TABLE_NAME);
    requestBuilder.getRowsBuilder().addRowRanges(RowRange.getDefaultInstance());
    defaultClient.readRows(requestBuilder.build());
    verifyRequestCalled(requestBuilder.build());
  }

  private void setResponse(final Object response) {
    Answer<Void> answer = new Answer<Void>(){

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        checkHeader(invocation.getArgumentAt(1, Metadata.class));
        ClientCall.Listener listener = invocation.getArgumentAt(0, ClientCall.Listener.class);
        listener.onMessage(response);
        listener.onClose(Status.OK, null);
        return null;
      }
    };
    doAnswer(answer)
        .when(mockClientCall)
        .start(any(ClientCall.Listener.class), any(Metadata.class));
  }

  private void checkHeader(Metadata metadata) {
    Assert.assertEquals(
        TABLE_NAME, metadata.get(GoogleCloudResourcePrefixInterceptor.GRPC_RESOURCE_PREFIX_KEY));
  }

  private void verifyRequestCalled(Object request) {
    verify(mockClientCall, times(1)).start(any(ClientCall.Listener.class), any(Metadata.class));
    verify(mockClientCall, times(1)).sendMessage(eq(request));
    verify(mockClientCall, times(1)).halfClose();
  }
}
