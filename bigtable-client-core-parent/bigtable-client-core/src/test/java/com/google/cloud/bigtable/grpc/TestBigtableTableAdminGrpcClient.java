/*
 * Copyright 2018 Google LLC All Rights Reserved.
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

import com.google.api.client.util.NanoClock;
import com.google.bigtable.admin.v2.CheckConsistencyRequest;
import com.google.bigtable.admin.v2.CheckConsistencyResponse;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenResponse;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBigtableTableAdminGrpcClient {

  private static final BigtableInstanceName INSTANCE_NAME = new BigtableInstanceName("projectId", "instanceId");

  private static final String TABLE_NAME = INSTANCE_NAME.toTableNameStr("tableId");

  @Mock
  Channel mockChannel;

  @Mock
  ClientCall mockClientCall;

  @Mock
  NanoClock nanoClock;

  BigtableTableAdminGrpcClient defaultClient;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(mockClientCall);
    defaultClient = createClient(false);
  }

  protected BigtableTableAdminGrpcClient createClient(boolean allowRetriesWithoutTimestamp) {
    BigtableOptions options = BigtableOptions.getDefaultOptions();
    return new BigtableTableAdminGrpcClient(mockChannel, null, options);
  }

  @Test
  public void testGetTable() {
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();
    setResponse(Table.getDefaultInstance());
    defaultClient.getTable(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testModifyFamiles() {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.newBuilder().setName(TABLE_NAME).build();
    setResponse(Table.getDefaultInstance());
    defaultClient.modifyColumnFamily(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testCreateTable() {
    CreateTableRequest request = CreateTableRequest.newBuilder()
        .setParent(INSTANCE_NAME.getInstanceName()).build();
    complete();
    defaultClient.createTable(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testDropRowRanges() {
    DropRowRangeRequest request = DropRowRangeRequest.newBuilder()
        .setName(TABLE_NAME).build();
    complete();
    defaultClient.dropRowRange(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testGenerateConsistencyToken() throws TimeoutException, InterruptedException {
    GenerateConsistencyTokenRequest request = GenerateConsistencyTokenRequest.newBuilder()
        .setName(TABLE_NAME).build();
    final AtomicInteger counter = new AtomicInteger(0);
    Answer<Void> answer = new Answer<Void>(){
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        Listener listener = invocation.getArgumentAt(0, Listener.class);
        if (counter.incrementAndGet() == 1) {
          listener.onMessage(GenerateConsistencyTokenResponse.getDefaultInstance());
        } else {
          listener.onMessage(CheckConsistencyResponse.newBuilder().setConsistent(true).build());
        }
        listener.onClose(Status.OK, null);
        return null;
      }
    };
    doAnswer(answer)
        .when(mockClientCall)
        .start(any(Listener.class), any(Metadata.class));
    defaultClient.waitForReplication(INSTANCE_NAME.toTableName("TABLE_NAME"), 6000);
    verify(mockClientCall, times(2)).start(any(Listener.class), any(Metadata.class));
    verify(mockClientCall, times(1)).sendMessage(Matchers.isA(GenerateConsistencyTokenRequest.class));
    verify(mockClientCall, times(1)).sendMessage(Matchers.isA(CheckConsistencyRequest.class));
    verify(mockClientCall, times(2)).halfClose();
  }


  @Test
  public void testSnapshotTable() throws Exception {
    SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
        .setName(TABLE_NAME).build();
    complete();
    defaultClient.snapshotTableAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }


  @Test
  public void testGetSnapshot() throws Exception {
    GetSnapshotRequest request = GetSnapshotRequest.newBuilder()
        .setName(TABLE_NAME).build();
    complete();
    defaultClient.getSnapshotAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testListSnapshots() throws Exception {
    ListSnapshotsRequest request = ListSnapshotsRequest.newBuilder()
        .setParent(TABLE_NAME).build();
    complete();
    defaultClient.listSnapshotsAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    DeleteSnapshotRequest request = DeleteSnapshotRequest.newBuilder()
        .setName(TABLE_NAME).build();
    complete();
    defaultClient.deleteSnapshotAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testCreateTableFromSnapshot() throws Exception {
    CreateTableFromSnapshotRequest request = CreateTableFromSnapshotRequest.newBuilder()
        .setParent(INSTANCE_NAME.getInstanceName()).build();
    complete();
    defaultClient.createTableFromSnapshotAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  private void  complete() {
    Answer<Void> answer = new Answer<Void>(){
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        Listener listener = invocation.getArgumentAt(0, Listener.class);
        listener.onMessage("");
        listener.onClose(Status.OK, null);
        return null;
      }
    };
    doAnswer(answer)
        .when(mockClientCall)
        .start(any(Listener.class), any(Metadata.class));
  }
  
  private void setResponse(final Object response) {
    Answer<Void> answer = new Answer<Void>(){
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        Listener listener = invocation.getArgumentAt(0, Listener.class);
        listener.onMessage(response);
        listener.onClose(Status.OK, null);
        return null;
      }
    };
    doAnswer(answer)
        .when(mockClientCall)
        .start(any(Listener.class), any(Metadata.class));
  }

  private void verifyRequestCalled(Object request) {
    verify(mockClientCall, times(1)).start(any(Listener.class), any(Metadata.class));
    verify(mockClientCall, times(1)).sendMessage(eq(request));
    verify(mockClientCall, times(1)).halfClose();
  }
}
