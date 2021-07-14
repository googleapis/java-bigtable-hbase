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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.Backup;
import com.google.bigtable.admin.v2.CheckConsistencyRequest;
import com.google.bigtable.admin.v2.CheckConsistencyResponse;
import com.google.bigtable.admin.v2.CreateBackupRequest;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteBackupRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenResponse;
import com.google.bigtable.admin.v2.GetBackupRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListBackupsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.RestoreTableRequest;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.UpdateBackupRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBigtableTableAdminGrpcClient {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final BigtableInstanceName INSTANCE_NAME =
      new BigtableInstanceName("projectId", "instanceId");

  private static final String TABLE_NAME = INSTANCE_NAME.toTableNameStr("tableId");

  @Mock Channel mockChannel;

  @Mock ClientCall mockClientCall;

  BigtableTableAdminGrpcClient defaultClient;

  @Before
  public void setup() {
    when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(mockClientCall);
    defaultClient = createClient();
  }

  protected BigtableTableAdminGrpcClient createClient() {
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
    CreateTableRequest request =
        CreateTableRequest.newBuilder().setParent(INSTANCE_NAME.getInstanceName()).build();
    setResponse(Table.getDefaultInstance());
    defaultClient.createTable(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testDropRowRanges() {
    DropRowRangeRequest request = DropRowRangeRequest.newBuilder().setName(TABLE_NAME).build();
    complete();
    defaultClient.dropRowRange(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testGenerateConsistencyToken() throws TimeoutException, InterruptedException {
    GenerateConsistencyTokenRequest request =
        GenerateConsistencyTokenRequest.newBuilder().setName(TABLE_NAME).build();
    final AtomicInteger counter = new AtomicInteger(0);
    Answer<Void> answer =
        new Answer<Void>() {
          @Override
          public Void answer(final InvocationOnMock invocation) throws Throwable {
            Listener listener = invocation.getArgument(0, Listener.class);
            if (counter.incrementAndGet() == 1) {
              listener.onMessage(GenerateConsistencyTokenResponse.getDefaultInstance());
            } else {
              listener.onMessage(CheckConsistencyResponse.newBuilder().setConsistent(true).build());
            }
            listener.onClose(Status.OK, null);
            return null;
          }
        };
    doAnswer(answer).when(mockClientCall).start(any(Listener.class), any(Metadata.class));
    defaultClient.waitForReplication(INSTANCE_NAME.toTableName("TABLE_NAME"), 6000);
    verify(mockClientCall, times(2)).start(any(Listener.class), any(Metadata.class));
    verify(mockClientCall, times(1))
        .sendMessage(ArgumentMatchers.isA(GenerateConsistencyTokenRequest.class));
    verify(mockClientCall, times(1))
        .sendMessage(ArgumentMatchers.isA(CheckConsistencyRequest.class));
    verify(mockClientCall, times(2)).halfClose();
  }

  @Test
  public void testGetIamPolicy() throws Exception {
    GetIamPolicyRequest request = GetIamPolicyRequest.newBuilder().setResource(TABLE_NAME).build();
    setResponse(Policy.getDefaultInstance());
    defaultClient.getIamPolicy(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testSetIamPolicy() throws Exception {
    SetIamPolicyRequest request = SetIamPolicyRequest.newBuilder().setResource(TABLE_NAME).build();
    setResponse(Policy.getDefaultInstance());
    defaultClient.setIamPolicy(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testTestIamPermissions() throws Exception {
    TestIamPermissionsRequest request =
        TestIamPermissionsRequest.newBuilder().setResource(TABLE_NAME).build();
    setResponse(TestIamPermissionsResponse.getDefaultInstance());
    defaultClient.testIamPermissions(request);
    verifyRequestCalled(request);
  }

  @Test
  public void testSnapshotTable() throws Exception {
    SnapshotTableRequest request = SnapshotTableRequest.newBuilder().setName(TABLE_NAME).build();
    complete();
    defaultClient.snapshotTableAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testGetSnapshot() throws Exception {
    GetSnapshotRequest request = GetSnapshotRequest.newBuilder().setName(TABLE_NAME).build();
    complete();
    defaultClient.getSnapshotAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testListSnapshots() throws Exception {
    ListSnapshotsRequest request = ListSnapshotsRequest.newBuilder().setParent(TABLE_NAME).build();
    complete();
    defaultClient.listSnapshotsAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    DeleteSnapshotRequest request = DeleteSnapshotRequest.newBuilder().setName(TABLE_NAME).build();
    complete();
    defaultClient.deleteSnapshotAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testCreateTableFromSnapshot() throws Exception {
    CreateTableFromSnapshotRequest request =
        CreateTableFromSnapshotRequest.newBuilder()
            .setParent(INSTANCE_NAME.getInstanceName())
            .build();
    complete();
    defaultClient.createTableFromSnapshotAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testBackupTable() throws Exception {
    CreateBackupRequest request =
        CreateBackupRequest.newBuilder()
            .setBackupId("backupId")
            .setBackup(
                com.google.bigtable.admin.v2.Backup.newBuilder()
                    .setName("backupId")
                    .setSourceTable(TABLE_NAME))
            .build();
    complete();
    defaultClient.createBackupAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testGetBackup() throws Exception {
    GetBackupRequest request = GetBackupRequest.newBuilder().setName(TABLE_NAME).build();
    complete();
    defaultClient.getBackupAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testUpdateBackup() throws InterruptedException, ExecutionException, TimeoutException {
    UpdateBackupRequest request =
        UpdateBackupRequest.newBuilder()
            .setBackup(Backup.newBuilder().setName("test").build())
            .build();
    complete();
    defaultClient.updateBackupAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testListBackups() throws Exception {
    ListBackupsRequest request = ListBackupsRequest.newBuilder().setParent(TABLE_NAME).build();
    complete();
    defaultClient.listBackupsAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testDeleteBackup() throws Exception {
    DeleteBackupRequest request = DeleteBackupRequest.newBuilder().setName(TABLE_NAME).build();
    complete();
    defaultClient.deleteBackupAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  @Test
  public void testCreateTableFromBackup() throws Exception {
    RestoreTableRequest request =
        RestoreTableRequest.newBuilder().setParent(INSTANCE_NAME.getInstanceName()).build();
    complete();
    defaultClient.restoreTableAsync(request).get(1, TimeUnit.SECONDS);
    verifyRequestCalled(request);
  }

  private void complete() {
    setResponse("");
  }

  private void setResponse(final Object response) {
    Answer<Void> answer =
        new Answer<Void>() {
          @Override
          public Void answer(final InvocationOnMock invocation) throws Throwable {
            Listener listener = invocation.getArgument(0, Listener.class);
            listener.onMessage(response);
            listener.onClose(Status.OK, null);
            return null;
          }
        };
    doAnswer(answer).when(mockClientCall).start(any(Listener.class), any(Metadata.class));
  }

  private void verifyRequestCalled(Object request) {
    verify(mockClientCall, times(1)).start(any(Listener.class), any(Metadata.class));
    verify(mockClientCall, times(1)).sendMessage(eq(request));
    verify(mockClientCall, times(1)).halfClose();
  }
}
