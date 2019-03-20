/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowAdapter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import static com.google.api.core.ApiFutures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class TestBigtableDataGCJClient {

  private static final String PROJECT_ID = "ignored";
  private static final String INSTANCE_ID = "ignored";
  private static final String TABLE_ID = "myTest";
  private static final String ROW_KEY = "row-key";
  private static final String COL_FAMILY = "cf1";

  private static final String TEST_TABLE_ID_1 = "test-table-1";
  private static final String TEST_TABLE_ID_2 = "test-table-2";
  private static final String TEST_TABLE_ID_3 = "test-table-3";
  private static final String tableName =
      NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID);
  private static final ByteString QUALIFIER_1 = ByteString.copyFromUtf8("qualifier1");
  private static final ByteString QUALIFIER_2 = ByteString.copyFromUtf8("qualifier2");
  private static final int TIMESTAMP = 12345;
  private static final String LABEL = "label";
  private static final List<String> LABELS = ImmutableList.of(LABEL);
  private static final ByteString VALUE_1 = ByteString.copyFromUtf8("test-value-1");
  private static final ByteString VALUE_2 = ByteString.copyFromUtf8("test-value-2");
  private static final List<Row> rows = ImmutableList.of(
      Row.create(ByteString.copyFromUtf8(ROW_KEY),
          ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1))),
      Row.create(ByteString.copyFromUtf8("row-key-2"),
          ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_2, TIMESTAMP, LABELS, VALUE_2))));

  private static final List<FlatRow> flatRows = ImmutableList.of(
      FlatRow.newBuilder()
          .withRowKey(ByteString.copyFromUtf8(ROW_KEY))
          .addCell(COL_FAMILY, QUALIFIER_1, TIMESTAMP, VALUE_1, LABELS)
          .build(),
      FlatRow.newBuilder()
          .withRowKey(ByteString.copyFromUtf8(ROW_KEY))
          .addCell(COL_FAMILY, QUALIFIER_2, TIMESTAMP, VALUE_2, LABELS)
          .build());

  private BigtableDataClient dataClientV2;
  private BigtableDataGCJClient dataGCJClient;

  @Before
  public void setUp(){
    dataClientV2 = mock(BigtableDataClient.class);
    dataGCJClient = new BigtableDataGCJClient(dataClientV2);
  }

  @Test
  public void testMutateRow() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ROW_KEY);
    doNothing().when(dataClientV2).mutateRow(rowMutation);
    dataGCJClient.mutateRow(rowMutation);
    verify(dataClientV2).mutateRow(rowMutation);
  }

  @Test
  public void testMutateRowAsync() throws Exception {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ROW_KEY);
    when(dataClientV2.mutateRowAsync(rowMutation))
        .thenReturn(ApiFutures.<Void>immediateFuture( null));
    dataGCJClient.mutateRowAsync(rowMutation).get();
    verify(dataClientV2).mutateRowAsync(rowMutation);
  }

  @Test
  public void testReadModifyWriteRow() {
    ReadModifyWriteRow mutation = ReadModifyWriteRow.create(TABLE_ID, ROW_KEY);
    Row expectedRow = Row.create(ByteString.copyFromUtf8(ROW_KEY),
        ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1)));
    when(dataClientV2.readModifyWriteRow(mutation)).thenReturn(expectedRow);
    Row actualRow = dataGCJClient.readModifyWriteRow(mutation);
    assertEquals(expectedRow, actualRow);
    verify(dataClientV2).readModifyWriteRow(mutation);
  }

  @Test
  public void testReadModifyWriteRowAsync() throws Exception {
    ReadModifyWriteRow mutation = ReadModifyWriteRow.create(TABLE_ID, ROW_KEY);
    Row expectedRow = Row.create(ByteString.copyFromUtf8(ROW_KEY),
        ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1)));
    when(dataClientV2.readModifyWriteRowAsync(mutation))
        .thenReturn(immediateFuture(expectedRow));
    Row actualRow = dataGCJClient.readModifyWriteRowAsync(mutation).get();
    assertEquals(expectedRow, actualRow);
    verify(dataClientV2).readModifyWriteRowAsync(mutation);
  }

  @Test
  public void testCheckAndMutateRow() {
    ConditionalRowMutation checkAndMutate = ConditionalRowMutation.create(TABLE_ID, ROW_KEY)
        .then(Mutation.create().setCell(COL_FAMILY, QUALIFIER_1, VALUE_1));
    when(dataClientV2.checkAndMutateRow(checkAndMutate)).thenReturn(Boolean.TRUE);
    assertTrue(dataGCJClient.checkAndMutateRow(checkAndMutate));
    verify(dataClientV2).checkAndMutateRow(checkAndMutate);
  }

  @Test
  public void testCheckAndMutateRowAsync() throws Exception {
    ConditionalRowMutation checkAndMutate = ConditionalRowMutation.create(TABLE_ID, ROW_KEY)
        .then(Mutation.create().setCell(COL_FAMILY, QUALIFIER_1, VALUE_1));
    when(dataClientV2.checkAndMutateRowAsync(checkAndMutate))
        .thenReturn(immediateFuture(Boolean.TRUE));
    assertTrue(dataGCJClient.checkAndMutateRowAsync(checkAndMutate).get());
    verify(dataClientV2).checkAndMutateRowAsync(checkAndMutate);
  }

  @Test
  public void testSampleRowKeys() {
    List<KeyOffset> expectedKeyOff =
        ImmutableList.of(KeyOffset.create(ByteString.copyFromUtf8(ROW_KEY), 10));
    when(dataClientV2.sampleRowKeys(TABLE_ID)).thenReturn(expectedKeyOff);
    List<KeyOffset> keyOffSets = dataGCJClient.sampleRowKeys(TABLE_ID);
    assertEquals(expectedKeyOff, keyOffSets);
    verify(dataClientV2).sampleRowKeys(TABLE_ID);
  }

  @Test
  public void testSampleRowKeysAsync() throws Exception {
    List<KeyOffset> expectedKeyOff =
        ImmutableList.of(KeyOffset.create(ByteString.copyFromUtf8(ROW_KEY), 10));
    when(dataClientV2.sampleRowKeysAsync(TABLE_ID)).thenReturn(immediateFuture(expectedKeyOff));
    List<KeyOffset> keyOffSets = dataGCJClient.sampleRowKeysAsync(TABLE_ID).get();
    assertEquals(expectedKeyOff, keyOffSets);
    verify(dataClientV2).sampleRowKeysAsync(TABLE_ID);
  }

  @Test
  public void testReadRowAsync() throws Exception {
    Query request = Query.create(TABLE_ID).range("a", "d");
    ServerStreamingCallable<Query, Row> serverStreaming = mock(ServerStreamingCallable.class);
    UnaryCallable<Query, List<Row>> unaryCallable = mock(UnaryCallable.class);
    List<Row> expectedRows = ImmutableList.of(
        Row.create(ByteString.copyFromUtf8(ROW_KEY),
            ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1))),
        Row.create(ByteString.copyFromUtf8("row-key-2"),
            ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_2, TIMESTAMP, LABELS, VALUE_2))));

    when(dataClientV2.readRowsCallable()).thenReturn(serverStreaming);
    when(serverStreaming.all()).thenReturn(unaryCallable);
    when(unaryCallable.futureCall(request)).thenReturn(immediateFuture(expectedRows));
    ApiFuture<List<Row>> actualRows = dataGCJClient.readRowsAsync(request);

    assertEquals(expectedRows, actualRows.get());
    verify(dataClientV2).readRowsCallable();
    verify(serverStreaming).all();
    verify(unaryCallable).futureCall(request);
  }

  @Test
  public void testReadFlatRowsList() {
    Query request = Query.create(TABLE_ID).range("a", "d");
    ServerStreamingCallable<Query, FlatRow> serverStreaming = mock(ServerStreamingCallable.class);
    UnaryCallable<Query, List<FlatRow>> unaryCallable = mock(UnaryCallable.class);

    when(dataClientV2.readRowsCallable(Mockito.any(FlatRowAdapter.class)))
        .thenReturn(serverStreaming);
    when(serverStreaming.all()).thenReturn(unaryCallable);
    when(unaryCallable.call(request)).thenReturn(flatRows);
    List<FlatRow> actualFlatRows = dataGCJClient.readFlatRowsList(request);

    assertEquals(flatRows, actualFlatRows);
    verify(dataClientV2).readRowsCallable(Mockito.any(FlatRowAdapter.class));
    verify(serverStreaming).all();
    verify(unaryCallable).call(request);
  }

  @Test
  public void testReadFlatRowsAsync() throws Exception {
    Query request = Query.create(TABLE_ID).range("a", "d");
    ServerStreamingCallable<Query, FlatRow> serverStreaming = mock(ServerStreamingCallable.class);
    UnaryCallable<Query, List<FlatRow>> unaryCallable = mock(UnaryCallable.class);

    when(dataClientV2.readRowsCallable(Mockito.any(FlatRowAdapter.class)))
        .thenReturn(serverStreaming);
    when(serverStreaming.all()).thenReturn(unaryCallable);
    when(unaryCallable.futureCall(request)).thenReturn(immediateFuture(flatRows));
    ApiFuture<List<FlatRow>> actualFlatRows = dataGCJClient.readFlatRowsAsync(request);

    assertEquals(flatRows, actualFlatRows.get());
    verify(dataClientV2).readRowsCallable(Mockito.any(FlatRowAdapter.class));
    verify(serverStreaming).all();
    verify(unaryCallable).futureCall(request);
  }

}
