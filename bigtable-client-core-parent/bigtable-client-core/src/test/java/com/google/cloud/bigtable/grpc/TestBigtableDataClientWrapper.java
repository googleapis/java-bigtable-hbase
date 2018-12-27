/*
 * Copyright 2018 Google LLC.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;

import static com.google.cloud.bigtable.util.DataWrapperUtil.convert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.util.DataWrapperUtil;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBigtableDataClientWrapper {

  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(InstanceName.of(PROJECT_ID, INSTANCE_ID), APP_PROFILE_ID);

  private BigtableOptions options =
      BigtableOptions.builder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID)
        .setAppProfileId(APP_PROFILE_ID).build();
  @Mock
  private BigtableDataClient client;

  private BigtableDataClientWrapper clientWrapper;

  @Before
  public void setUp() {
    clientWrapper = new BigtableDataClientWrapper(client, options);
  }

  @Test
  public void testMutateRow() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest mutateRowRequest = rowMutation.toProto(REQUEST_CONTEXT);
    when(client.mutateRow(mutateRowRequest)).thenReturn(MutateRowResponse.getDefaultInstance());
    clientWrapper.mutateRow(rowMutation);
    verify(client).mutateRow(mutateRowRequest);
  }

  @Test
  public void testMutateRowAsync() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest request = rowMutation.toProto(REQUEST_CONTEXT);
    ListenableFuture<MutateRowResponse> response =
        Futures.immediateFuture(MutateRowResponse.getDefaultInstance());
    when(client.mutateRowAsync(request)).thenReturn(response);
    clientWrapper.mutateRowAsync(rowMutation);
    verify(client).mutateRowAsync(request);
  }

  @Test
  public void testTrueMutationRow(){
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(true).build();
    when(client.checkAndMutateRow(request)).thenReturn(response);
    Boolean actual = clientWrapper.checkAndMutateRow(conditonalMutation);
    verify(client).checkAndMutateRow(request);
    assertTrue(actual);
  }

  @Test
  public void testFalseMutationRow(){
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").otherwise(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(false).build();
    when(client.checkAndMutateRow(request)).thenReturn(response);
    Boolean actual = clientWrapper.checkAndMutateRow(conditonalMutation);
    verify(client).checkAndMutateRow(request);
    assertTrue(actual);
  }

  @Test
  public void testTrueMutationRowAsync() throws Exception{
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(true).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);

    when(client.checkAndMutateRowAsync(request)).thenReturn(future);
    ListenableFuture<Boolean> actual = clientWrapper.checkAndMutateRowAsync(conditonalMutation);
    verify(client).checkAndMutateRowAsync(request);
    assertTrue(actual.get());
  }

  @Test
  public void testFalseMutationRowAsync() throws Exception{
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").otherwise(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(false).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);
    when(client.checkAndMutateRowAsync(request)).thenReturn(future);
    ListenableFuture<Boolean> actual = clientWrapper.checkAndMutateRowAsync(conditonalMutation);
    verify(client).checkAndMutateRowAsync(request);
    assertTrue(actual.get());
  }

  @Test
  public void testReadModifyWrite(){
    ReadModifyWriteRow readModify = ReadModifyWriteRow.create(TABLE_ID, "test-key");
    ReadModifyWriteRowRequest request = readModify.toProto(REQUEST_CONTEXT);
    Row expectedRow = buildRow();
    ReadModifyWriteRowResponse response =
        ReadModifyWriteRowResponse.newBuilder().setRow(expectedRow).build();

    when(client.readModifyWriteRow(request)).thenReturn(response);
    com.google.cloud.bigtable.data.v2.models.Row actualRow =
        clientWrapper.readModifyWriteRow(readModify);
    assertEquals(convert(expectedRow), actualRow);
    verify(client).readModifyWriteRow(request);
  }

  @Test
  public void testReadModifyWriteAsync() throws Exception{
    ReadModifyWriteRow readModify = ReadModifyWriteRow.create(TABLE_ID, "test-key");
    ReadModifyWriteRowRequest request = readModify.toProto(REQUEST_CONTEXT);
    Row expectedRow = buildRow();
    ReadModifyWriteRowResponse response =
        ReadModifyWriteRowResponse.newBuilder().setRow(expectedRow).build();
    ListenableFuture<ReadModifyWriteRowResponse> listenableResponse =
        Futures.immediateFuture(response);

    when(client.readModifyWriteRowAsync(request)).thenReturn(listenableResponse);
    ListenableFuture<com.google.cloud.bigtable.data.v2.models.Row> output =
        clientWrapper.readModifyWriteRowAsync(readModify);
    verify(client).readModifyWriteRowAsync(request);
    Assert.assertEquals(convert(expectedRow), output.get());
  }

  private static Row buildRow() {
    Cell cell =
        Cell.newBuilder()
            .setValue(ByteString.copyFromUtf8("test-value"))
            .setTimestampMicros(12345)
            .addLabels("lable-1")
            .build();
    return Row.newBuilder()
        .setKey(ByteString.copyFromUtf8("test-key"))
        .addFamilies(Family.newBuilder()
            .setName("firstFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(ByteString.copyFromUtf8("qualifier1"))
                .addCells(cell)
                .build())
            .build())
        .build();
  }
}
