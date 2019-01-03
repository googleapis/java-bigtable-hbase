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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBigtableDataClientWrapper {

  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final ByteString QUALIFIER_1 = ByteString.copyFromUtf8("qualifier1");
  private static final ByteString QUALIFIER_2 = ByteString.copyFromUtf8("qualifier2");
  private static final int TIMESTAMP = 12345;
  private static final String LABEL = "label";
  private static final List<String> LABEL_LIST = Collections.singletonList(LABEL);
  private static final ByteString VALUE = ByteString.copyFromUtf8("test-value");
  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("test-key");

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
  public void testCheckMutateRow(){
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
  public void testCheckMutateRowWhenNoPredicateMatch(){
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(false).build();
    when(client.checkAndMutateRow(request)).thenReturn(response);
    Boolean actual = clientWrapper.checkAndMutateRow(conditonalMutation);
    verify(client).checkAndMutateRow(request);
    assertFalse(actual);
  }

  @Test
  public void testCheckMutateRowAsync() throws Exception{
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
  public void testCheckMutateRowAsyncWhenNoPredicateMatch() throws Exception{
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(false).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);

    when(client.checkAndMutateRowAsync(request)).thenReturn(future);
    ListenableFuture<Boolean> actual = clientWrapper.checkAndMutateRowAsync(conditonalMutation);
    verify(client).checkAndMutateRowAsync(request);
    assertFalse(actual.get());
  }

  @Test
  public void testReadModifyWriteWithOneCell(){
    ReadModifyWriteRow readModify = ReadModifyWriteRow.create(TABLE_ID, "test-key");
    ReadModifyWriteRowRequest request = readModify.toProto(REQUEST_CONTEXT);
    Row expectedRow = buildRow();
    ReadModifyWriteRowResponse response =
        ReadModifyWriteRowResponse.newBuilder().setRow(expectedRow).build();
    RowCell rowCell = RowCell.create("firstFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE);
    com.google.cloud.bigtable.data.v2.models.Row modelRow =
        com.google.cloud.bigtable.data.v2.models.Row.create(ROW_KEY, Collections.singletonList(rowCell));

    when(client.readModifyWriteRow(request)).thenReturn(response);
    com.google.cloud.bigtable.data.v2.models.Row actualRow =
        clientWrapper.readModifyWriteRow(readModify);
    assertEquals(modelRow, actualRow);
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
    RowCell rowCell = RowCell.create("firstFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE);
    com.google.cloud.bigtable.data.v2.models.Row modelRow =
        com.google.cloud.bigtable.data.v2.models.Row.create(ROW_KEY, Collections.singletonList(rowCell));

    when(client.readModifyWriteRowAsync(request)).thenReturn(listenableResponse);
    ListenableFuture<com.google.cloud.bigtable.data.v2.models.Row> output =
        clientWrapper.readModifyWriteRowAsync(readModify);
    assertEquals(modelRow, output.get());
    verify(client).readModifyWriteRowAsync(request);
  }

  private static Row buildRow() {
    Cell cell = Cell.newBuilder()
        .setValue(VALUE)
        .setTimestampMicros(TIMESTAMP)
        .addLabels(LABEL)
        .build();
    return Row.newBuilder()
        .setKey(ROW_KEY)
        .addFamilies(Family.newBuilder()
            .setName("firstFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_1)
                .addCells(cell)
                .build())
            .build())
        .build();
  }

  @Test
  public void testReadModifyWriteWithMultipleCell(){
    Row row = Row.newBuilder()
        .setKey(ROW_KEY)
        .addFamilies(Family.newBuilder()
            .setName("firstFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_1)
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(TIMESTAMP)
                    .addLabels(LABEL)
                    .build())
                .build())
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_2)
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(TIMESTAMP)
                    .addLabels(LABEL)
                    .build())
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(54321)
                    .addLabels(LABEL)
                    .build())
                .build())
            .build())
        .addFamilies(Family.newBuilder()
            .setName("secondFamily")
            .addColumns(Column.newBuilder()
                .setQualifier(QUALIFIER_1)
                .addCells(Cell.newBuilder()
                    .setValue(VALUE)
                    .setTimestampMicros(TIMESTAMP)
                    .addLabels(LABEL)
                    .build())
                .build()))
        .build();
    ReadModifyWriteRowResponse response =
        ReadModifyWriteRowResponse.newBuilder().setRow(row).build();
    ReadModifyWriteRow readModify = ReadModifyWriteRow.create(TABLE_ID, "test-key");
    ReadModifyWriteRowRequest request = readModify.toProto(REQUEST_CONTEXT);
    ImmutableList.Builder<RowCell> rowCells = ImmutableList.builder();
    rowCells.add(RowCell.create("firstFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE));
    rowCells.add(RowCell.create("firstFamily", QUALIFIER_2, TIMESTAMP, LABEL_LIST, VALUE));
    rowCells.add(RowCell.create("firstFamily", QUALIFIER_2, 54321, LABEL_LIST, VALUE));
    rowCells.add(RowCell.create("secondFamily", QUALIFIER_1, TIMESTAMP, LABEL_LIST, VALUE));
    com.google.cloud.bigtable.data.v2.models.Row expectedModelRow =
        com.google.cloud.bigtable.data.v2.models.Row.create(ROW_KEY, rowCells.build());

    when(client.readModifyWriteRow(request)).thenReturn(response);
    com.google.cloud.bigtable.data.v2.models.Row actualRow =
        clientWrapper.readModifyWriteRow(readModify);
    assertEquals(expectedModelRow, actualRow);
    verify(client).readModifyWriteRow(request);
  }
}
