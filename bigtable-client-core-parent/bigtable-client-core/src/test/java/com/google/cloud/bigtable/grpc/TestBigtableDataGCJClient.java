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

import static com.google.api.core.ApiFutures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StreamController;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowAdapter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestBigtableDataGCJClient {

  @Rule public final MockitoRule rule = MockitoJUnit.rule();

  private static final String TABLE_ID = "myTest";
  private static final String ROW_KEY = "row-key";
  private static final String COL_FAMILY = "cf1";
  private static final ByteString QUALIFIER_1 = ByteString.copyFromUtf8("qualifier1");
  private static final ByteString QUALIFIER_2 = ByteString.copyFromUtf8("qualifier2");
  private static final int TIMESTAMP = 12345;
  private static final String LABEL = "label";
  private static final List<String> LABELS = ImmutableList.of(LABEL);
  private static final ByteString VALUE_1 = ByteString.copyFromUtf8("test-value-1");
  private static final ByteString VALUE_2 = ByteString.copyFromUtf8("test-value-2");
  private static final Query request = Query.create(TABLE_ID);

  private static final List<Row> rows =
      ImmutableList.of(
          Row.create(
              ByteString.copyFromUtf8(ROW_KEY),
              ImmutableList.of(
                  RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1))),
          Row.create(
              ByteString.copyFromUtf8("row-key-2"),
              ImmutableList.of(
                  RowCell.create(COL_FAMILY, QUALIFIER_2, TIMESTAMP, LABELS, VALUE_2))));

  private static final List<FlatRow> flatRows =
      ImmutableList.of(
          FlatRow.newBuilder()
              .withRowKey(ByteString.copyFromUtf8(ROW_KEY))
              .addCell(COL_FAMILY, QUALIFIER_1, TIMESTAMP, VALUE_1, LABELS)
              .build(),
          FlatRow.newBuilder()
              .withRowKey(ByteString.copyFromUtf8(ROW_KEY))
              .addCell(COL_FAMILY, QUALIFIER_2, TIMESTAMP, VALUE_2, LABELS)
              .build());

  @Mock private BigtableDataClient dataClientV2;
  @Mock private UnaryCallable<RowMutation, Void> mockMutateRowCallable;
  @Mock private UnaryCallable<ReadModifyWriteRow, Row> mockReadModifyWriteRowCallable;
  @Mock private UnaryCallable<ConditionalRowMutation, Boolean> mockCheckAndMutateRowCallable;
  @Mock private UnaryCallable<String, List<KeyOffset>> mockSampleRowKeysCallable;
  @Mock private ServerStreamingCallable<Query, Row> mockReadRowsCallable;
  @Mock private ServerStreamingCallable<Query, FlatRow> mockReadFlatRowsCallable;

  private BigtableDataGCJClient dataGCJClient;

  @Before
  public void setUp() {
    when(dataClientV2.mutateRowCallable()).thenReturn(mockMutateRowCallable);
    when(dataClientV2.readModifyWriteRowCallable()).thenReturn(mockReadModifyWriteRowCallable);
    when(dataClientV2.checkAndMutateRowCallable()).thenReturn(mockCheckAndMutateRowCallable);
    when(dataClientV2.sampleRowKeysCallable()).thenReturn(mockSampleRowKeysCallable);
    when(dataClientV2.readRowsCallable()).thenReturn(mockReadRowsCallable);
    when(dataClientV2.readRowsCallable(Mockito.any(FlatRowAdapter.class)))
        .thenReturn(mockReadFlatRowsCallable);
    dataGCJClient = new BigtableDataGCJClient(dataClientV2);
  }

  @Test
  public void testMutateRow() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ROW_KEY);
    when(mockMutateRowCallable.futureCall(rowMutation, null))
        .thenReturn(ApiFutures.<Void>immediateFuture(null));
    dataGCJClient.mutateRow(rowMutation);
    verify(mockMutateRowCallable).futureCall(rowMutation, null);
  }

  @Test
  public void testMutateRowAsync() throws Exception {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ROW_KEY);
    when(mockMutateRowCallable.futureCall(rowMutation, null))
        .thenReturn(ApiFutures.<Void>immediateFuture(null));
    dataGCJClient.mutateRowAsync(rowMutation).get();
    verify(mockMutateRowCallable).futureCall(rowMutation, null);
  }

  @Test
  public void testReadModifyWriteRow() {
    ReadModifyWriteRow mutation = ReadModifyWriteRow.create(TABLE_ID, ROW_KEY);
    Row expectedRow =
        Row.create(
            ByteString.copyFromUtf8(ROW_KEY),
            ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1)));
    when(mockReadModifyWriteRowCallable.futureCall(mutation, null))
        .thenReturn(ApiFutures.immediateFuture(expectedRow));
    Row actualRow = dataGCJClient.readModifyWriteRow(mutation);
    assertEquals(expectedRow, actualRow);
    verify(mockReadModifyWriteRowCallable).futureCall(mutation, null);
  }

  @Test
  public void testReadModifyWriteRowAsync() throws Exception {
    ReadModifyWriteRow mutation = ReadModifyWriteRow.create(TABLE_ID, ROW_KEY);
    Row expectedRow =
        Row.create(
            ByteString.copyFromUtf8(ROW_KEY),
            ImmutableList.of(RowCell.create(COL_FAMILY, QUALIFIER_1, TIMESTAMP, LABELS, VALUE_1)));
    when(mockReadModifyWriteRowCallable.futureCall(mutation, null))
        .thenReturn(immediateFuture(expectedRow));
    Row actualRow = dataGCJClient.readModifyWriteRowAsync(mutation).get();
    assertEquals(expectedRow, actualRow);
    verify(mockReadModifyWriteRowCallable).futureCall(mutation, null);
  }

  @Test
  public void testCheckAndMutateRow() {
    ConditionalRowMutation checkAndMutate =
        ConditionalRowMutation.create(TABLE_ID, ROW_KEY)
            .then(Mutation.create().setCell(COL_FAMILY, QUALIFIER_1, VALUE_1));
    when(mockCheckAndMutateRowCallable.futureCall(checkAndMutate, null))
        .thenReturn(ApiFutures.immediateFuture(Boolean.TRUE));
    assertTrue(dataGCJClient.checkAndMutateRow(checkAndMutate));
    verify(mockCheckAndMutateRowCallable).futureCall(checkAndMutate, null);
  }

  @Test
  public void testCheckAndMutateRowAsync() throws Exception {
    ConditionalRowMutation checkAndMutate =
        ConditionalRowMutation.create(TABLE_ID, ROW_KEY)
            .then(Mutation.create().setCell(COL_FAMILY, QUALIFIER_1, VALUE_1));
    when(mockCheckAndMutateRowCallable.futureCall(checkAndMutate, null))
        .thenReturn(immediateFuture(Boolean.TRUE));
    assertTrue(dataGCJClient.checkAndMutateRowAsync(checkAndMutate).get());
    verify(mockCheckAndMutateRowCallable).futureCall(checkAndMutate, null);
  }

  @Test
  public void testSampleRowKeys() {
    List<KeyOffset> expectedKeyOff =
        ImmutableList.of(KeyOffset.create(ByteString.copyFromUtf8(ROW_KEY), 10));
    when(mockSampleRowKeysCallable.futureCall(TABLE_ID, null))
        .thenReturn(ApiFutures.immediateFuture(expectedKeyOff));
    List<KeyOffset> keyOffSets = dataGCJClient.sampleRowKeys(TABLE_ID);
    assertEquals(expectedKeyOff, keyOffSets);
    verify(mockSampleRowKeysCallable).futureCall(TABLE_ID, null);
  }

  @Test
  public void testSampleRowKeysAsync() throws Exception {
    List<KeyOffset> expectedKeyOff =
        ImmutableList.of(KeyOffset.create(ByteString.copyFromUtf8(ROW_KEY), 10));
    when(mockSampleRowKeysCallable.futureCall(TABLE_ID, null))
        .thenReturn(immediateFuture(expectedKeyOff));
    List<KeyOffset> keyOffSets = dataGCJClient.sampleRowKeysAsync(TABLE_ID).get();
    assertEquals(expectedKeyOff, keyOffSets);
    verify(mockSampleRowKeysCallable).futureCall(TABLE_ID, null);
  }

  @Test
  public void testReadRowAsync() throws Exception {
    mockReadRows(true);
    ApiFuture<List<Row>> actualRows = dataGCJClient.readRowsAsync(request);
    assertEquals(rows, actualRows.get());
    verify(mockReadRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<Row>>any());
  }

  @Test
  public void testReadFlatRowsList() {
    mockReadFlatRows(true);
    List<FlatRow> actualFlatRows = dataGCJClient.readFlatRowsList(request);
    assertEquals(flatRows, actualFlatRows);
    verify(mockReadFlatRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<FlatRow>>any());
  }

  @Test
  public void testReadFlatRowsAsync() throws Exception {
    mockReadFlatRows(true);
    ApiFuture<List<FlatRow>> actualFlatRows = dataGCJClient.readFlatRowsAsync(request);

    assertEquals(flatRows, actualFlatRows.get());
    verify(mockReadFlatRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<FlatRow>>any());
  }

  @Test
  public void testReadRows() throws Exception {
    mockReadRows(false);

    ResultScanner<Row> results = dataGCJClient.readRows(request);
    assertEquals(rows.get(0), results.next());
    verify(mockReadRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<Row>>any());
  }

  @Test
  public void testReadFlatRows() throws Exception {
    mockReadFlatRows(false);
    ResultScanner<FlatRow> results = dataGCJClient.readFlatRows(request);
    assertEquals(flatRows.get(0), results.next());
    assertEquals(flatRows.get(1), results.next());
    verify(mockReadFlatRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<FlatRow>>any());
  }

  @Test
  public void testReadFlatRowsAsyncWithStream() {
    mockReadFlatRows(true);
    final List<FlatRow> actualFlatRows = new ArrayList<>(2);
    dataGCJClient.readFlatRowsAsync(
        request,
        new StreamObserver<FlatRow>() {
          @Override
          public void onNext(FlatRow flatRow) {
            actualFlatRows.add(flatRow);
          }

          @Override
          public void onError(Throwable throwable) {
            Assert.fail("flat row streaming should not fail");
          }

          @Override
          public void onCompleted() {}
        });
    assertEquals(flatRows, actualFlatRows);
  }

  @Test
  public void testAutoClose() throws Exception {
    try (BigtableDataGCJClient gcjClient = new BigtableDataGCJClient(dataClientV2)) {
      when(mockSampleRowKeysCallable.futureCall(TABLE_ID, null))
          .thenReturn(ApiFutures.<List<KeyOffset>>immediateFuture(ImmutableList.<KeyOffset>of()));
      gcjClient.sampleRowKeys(TABLE_ID);
      verify(mockSampleRowKeysCallable).futureCall(TABLE_ID, null);
    }
    // BigtableDataGCJClient#close should be invoked
    verify(dataClientV2).close();
  }

  @Test
  public void testCreateBulkMutationBatcher() {
    Batcher<RowMutationEntry, Void> batcher = mock(Batcher.class);
    when(dataClientV2.newBulkMutationBatcher(TABLE_ID)).thenReturn(batcher);
    dataGCJClient.createBulkMutationBatcher(TABLE_ID);
    verify(dataClientV2).newBulkMutationBatcher(TABLE_ID);
  }

  private void mockReadRows(final boolean isAsync) {
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) {
                ResponseObserver<Row> observer = invocationOnMock.getArgument(1);
                observer.onStart(mock(StreamController.class));
                observer.onResponse(rows.get(0));
                observer.onResponse(rows.get(1));
                if (isAsync) {
                  observer.onComplete();
                }
                return null;
              }
            })
        .when(mockReadRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<Row>>any());
  }

  private void mockReadFlatRows(final boolean isAsync) {
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) {
                ResponseObserver<FlatRow> observer = invocationOnMock.getArgument(1);
                observer.onStart(mock(StreamController.class));
                observer.onResponse(flatRows.get(0));
                observer.onResponse(flatRows.get(1));
                if (isAsync) {
                  observer.onComplete();
                }
                return null;
              }
            })
        .when(mockReadFlatRowsCallable)
        .call(Mockito.any(Query.class), Mockito.<ResponseObserver<FlatRow>>any());
  }
}
