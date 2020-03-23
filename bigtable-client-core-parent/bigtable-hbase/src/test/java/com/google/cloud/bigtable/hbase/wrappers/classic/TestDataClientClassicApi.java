/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import static com.google.cloud.bigtable.hbase.adapters.Adapters.FLAT_ROW_ADAPTER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
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
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ScanHandler;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestDataClientClassicApi {

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";

  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("test-key");
  private static final ByteString QUALIFIER = ByteString.copyFromUtf8("qualifier1");
  private static final int TIMESTAMP = 12345;
  private static final ByteString VALUE = ByteString.copyFromUtf8("test-value");

  private static final FlatRow SAMPLE_FLAT_ROW =
      FlatRow.newBuilder()
          .withRowKey(ByteString.copyFromUtf8("key"))
          .addCell("cf", QUALIFIER, TIMESTAMP, VALUE)
          .addCell("cf2", QUALIFIER, TIMESTAMP, VALUE)
          .build();

  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(PROJECT_ID, INSTANCE_ID, APP_PROFILE_ID);

  @Mock private BigtableDataClient delegate;

  @Mock private BigtableSession bigtableSession;

  @Mock private ResultScanner<FlatRow> mockFlatRowScanner;

  private DataClientWrapper dataClientWrapper;

  @Before
  public void setUp() {
    when(bigtableSession.getDataClient()).thenReturn(delegate);
    dataClientWrapper = new DataClientClassicApi(bigtableSession, REQUEST_CONTEXT);
  }

  @Test
  public void testCreateBulkMutation() {
    BulkMutation mockBulkMutation = Mockito.mock(BulkMutation.class);
    when(bigtableSession.createBulkMutation(Mockito.<BigtableTableName>any()))
        .thenReturn(mockBulkMutation);
    assertTrue(dataClientWrapper.createBulkMutation(TABLE_ID) instanceof BulkMutationClassicApi);
    verify(bigtableSession).createBulkMutation(Mockito.<BigtableTableName>any());
  }

  @Test
  public void testCreateBulkRead() {
    BulkRead mockBulkRead = Mockito.mock(BulkRead.class);
    when(bigtableSession.createBulkRead(Mockito.<BigtableTableName>any())).thenReturn(mockBulkRead);
    assertTrue(dataClientWrapper.createBulkRead(TABLE_ID) instanceof BulkReadClassicApi);
    verify(bigtableSession).createBulkRead(Mockito.<BigtableTableName>any());
  }

  @Test
  public void testMutateRowAsync() throws Exception {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest request = rowMutation.toProto(REQUEST_CONTEXT);
    ListenableFuture<MutateRowResponse> response =
        Futures.immediateFuture(MutateRowResponse.getDefaultInstance());
    when(delegate.mutateRowAsync(request)).thenReturn(response);
    dataClientWrapper.mutateRowAsync(rowMutation).get();
    verify(delegate).mutateRowAsync(request);
  }

  @Test
  public void testCheckMutateRowAsync() throws Exception {
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditionalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditionalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(true).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);

    when(delegate.checkAndMutateRowAsync(request)).thenReturn(future);
    Future<Boolean> actual = dataClientWrapper.checkAndMutateRowAsync(conditionalMutation);
    verify(delegate).checkAndMutateRowAsync(request);
    assertTrue(actual.get());
  }

  @Test
  public void testCheckMutateRowAsyncWhenNoPredicateMatch() throws Exception {
    Mutation mutation = Mutation.create();
    mutation.setCell("family", "qualifier", "some other value");
    ConditionalRowMutation conditonalMutation =
        ConditionalRowMutation.create(TABLE_ID, "first" + "-row" + "-key").then(mutation);
    CheckAndMutateRowRequest request = conditonalMutation.toProto(REQUEST_CONTEXT);
    CheckAndMutateRowResponse response =
        CheckAndMutateRowResponse.newBuilder().setPredicateMatched(false).build();
    ListenableFuture<CheckAndMutateRowResponse> future = Futures.immediateFuture(response);

    when(delegate.checkAndMutateRowAsync(request)).thenReturn(future);
    Future<Boolean> actual = dataClientWrapper.checkAndMutateRowAsync(conditonalMutation);
    verify(delegate).checkAndMutateRowAsync(request);
    assertFalse(actual.get());
  }

  @Test
  public void testReadModifyWriteAsyncWithEmptyCell() throws Exception {
    ReadModifyWriteRow readModify = ReadModifyWriteRow.create(TABLE_ID, "test-key");
    ReadModifyWriteRowRequest request = readModify.toProto(REQUEST_CONTEXT);
    ReadModifyWriteRowResponse response =
        ReadModifyWriteRowResponse.newBuilder()
            .setRow(Row.newBuilder().setKey(ROW_KEY).build())
            .build();
    ListenableFuture<ReadModifyWriteRowResponse> listenableResponse =
        Futures.immediateFuture(response);

    when(delegate.readModifyWriteRowAsync(request)).thenReturn(listenableResponse);
    Result actualResult = dataClientWrapper.readModifyWriteRowAsync(readModify).get();
    assertNull(actualResult.getRow());
    assertEquals(0, actualResult.rawCells().length);
    verify(delegate).readModifyWriteRowAsync(request);
  }

  @Test
  public void testReadModifyWriteAsyncWithOneRow() throws ExecutionException, InterruptedException {
    String family_1 = "col-family-1";
    String family_2 = "col-family-2";
    String family_3 = "col-family-3";

    ByteString qualifier_1 = ByteString.copyFromUtf8("test-qualifier_1-1");
    ByteString qualifier_2 = ByteString.copyFromUtf8("test-qualifier_1-2");
    ByteString qualifier_3 = ByteString.copyFromUtf8("test-qualifier_1-2");

    ByteString value_1 = ByteString.copyFromUtf8("test-values-1");
    ByteString value_2 = ByteString.copyFromUtf8("test-values-2");
    ByteString value_3 = ByteString.copyFromUtf8("test-values-3");
    ByteString value_4 = ByteString.copyFromUtf8("test-values-4");
    ByteString value_5 = ByteString.copyFromUtf8("test-values-5");
    ByteString value_6 = ByteString.copyFromUtf8("test-values-6");

    Row row =
        Row.newBuilder()
            .setKey(ROW_KEY)
            .addFamilies(
                Family.newBuilder()
                    .setName(family_1)
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_1)
                            // First Cell
                            .addCells(
                                Cell.newBuilder()
                                    .setTimestampMicros(11_111L)
                                    .setValue(value_1)
                                    .addLabels("label-1"))
                            // Same Cell with another timestamp and value
                            .addCells(
                                Cell.newBuilder()
                                    .setTimestampMicros(22_222L)
                                    .setValue(value_2)
                                    .addLabels("label-2")))
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_2)
                            // With label
                            .addCells(
                                Cell.newBuilder()
                                    .setTimestampMicros(11_111L)
                                    .setValue(value_3)
                                    .addLabels("label-3")
                                    .addLabels("label-4"))
                            // Same family, same timestamp, but different column.
                            .addCells(
                                Cell.newBuilder().setTimestampMicros(22_222L).setValue(value_4)))
                    .build())
            .addFamilies(
                Family.newBuilder()
                    .setName(family_2)
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_1)
                            // Same column, same timestamp, but different family.
                            .addCells(
                                Cell.newBuilder().setTimestampMicros(11_111L).setValue(value_5))))
            .addFamilies(
                Family.newBuilder()
                    .setName(family_3)
                    .addColumns(
                        Column.newBuilder()
                            .setQualifier(qualifier_3)
                            // Same timestamp, but different family and column.
                            .addCells(Cell.newBuilder().setValue(value_6).addLabels("label-6"))))
            .build();

    ReadModifyWriteRow readModify = ReadModifyWriteRow.create(TABLE_ID, "test-key");
    ReadModifyWriteRowRequest request = readModify.toProto(REQUEST_CONTEXT);
    ReadModifyWriteRowResponse response =
        ReadModifyWriteRowResponse.newBuilder().setRow(row).build();

    ListenableFuture<ReadModifyWriteRowResponse> listenableResponse =
        Futures.immediateFuture(response);

    when(delegate.readModifyWriteRowAsync(request)).thenReturn(listenableResponse);
    Result result = dataClientWrapper.readModifyWriteRowAsync(readModify).get();

    assertEquals(6, result.rawCells().length);

    List<org.apache.hadoop.hbase.Cell> cells1 =
        result.getColumnCells(family_1.getBytes(), qualifier_1.toByteArray());

    assertEquals(2, cells1.size());
    assertEquals(11L, cells1.get(0).getTimestamp());
    assertArrayEquals(value_1.toByteArray(), CellUtil.cloneValue(cells1.get(0)));
    assertEquals(Collections.singletonList("label-1"), ((RowCell) cells1.get(0)).getLabels());

    assertEquals(22L, cells1.get(1).getTimestamp());
    assertArrayEquals(value_2.toByteArray(), CellUtil.cloneValue(cells1.get(1)));
    assertEquals(Collections.singletonList("label-2"), ((RowCell) cells1.get(1)).getLabels());

    List<org.apache.hadoop.hbase.Cell> cells2 =
        result.getColumnCells(family_1.getBytes(), qualifier_2.toByteArray());
    assertEquals(2, cells2.size());
    assertEquals(11L, cells2.get(0).getTimestamp());
    assertArrayEquals(value_3.toByteArray(), CellUtil.cloneValue(cells2.get(0)));
    assertEquals(ImmutableList.of("label-3", "label-4"), ((RowCell) cells2.get(0)).getLabels());

    assertEquals(22L, cells2.get(1).getTimestamp());
    assertArrayEquals(value_4.toByteArray(), CellUtil.cloneValue(cells2.get(1)));

    List<org.apache.hadoop.hbase.Cell> cells3 =
        result.getColumnCells(family_2.getBytes(), qualifier_1.toByteArray());
    assertEquals(1, cells3.size());
    assertArrayEquals(value_5.toByteArray(), CellUtil.cloneValue(cells3.get(0)));

    List<org.apache.hadoop.hbase.Cell> cells4 =
        result.getColumnCells(family_3.getBytes(), qualifier_3.toByteArray());
    assertEquals(1, cells4.size());
    assertArrayEquals(value_6.toByteArray(), CellUtil.cloneValue(cells4.get(0)));
    assertEquals(Collections.singletonList("label-6"), ((RowCell) cells4.get(0)).getLabels());
  }

  @Test
  public void testSampleRowKeysAsync() throws Exception {
    final ByteString ROW_KEY_1 = ByteString.copyFromUtf8("row-key-1");
    final ByteString ROW_KEY_2 = ByteString.copyFromUtf8("row-key-2");
    final ByteString ROW_KEY_3 = ByteString.copyFromUtf8("row-key-3");

    String tableName = NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID);
    SampleRowKeysRequest requestProto =
        SampleRowKeysRequest.newBuilder().setTableName(tableName).build();
    List<SampleRowKeysResponse> responseProto =
        ImmutableList.of(
            SampleRowKeysResponse.newBuilder().setRowKey(ROW_KEY_1).setOffsetBytes(11).build(),
            SampleRowKeysResponse.newBuilder().setRowKey(ROW_KEY_2).setOffsetBytes(12).build(),
            SampleRowKeysResponse.newBuilder().setRowKey(ROW_KEY_3).setOffsetBytes(13).build());

    when(delegate.sampleRowKeysAsync(requestProto))
        .thenReturn(Futures.immediateFuture(responseProto));

    List<KeyOffset> keyOffsetList = dataClientWrapper.sampleRowKeysAsync(TABLE_ID).get();
    assertEquals(keyOffsetList.get(0).getKey(), ROW_KEY_1);
    assertEquals(keyOffsetList.get(1).getKey(), ROW_KEY_2);
    assertEquals(keyOffsetList.get(2).getKey(), ROW_KEY_3);
    verify(delegate).sampleRowKeysAsync(requestProto);
  }

  @Test
  public void testReadRows() throws Exception {
    Query query = Query.create(TABLE_ID);
    when(delegate.readFlatRows(query.toProto(REQUEST_CONTEXT))).thenReturn(mockFlatRowScanner);
    when(mockFlatRowScanner.next())
        .thenReturn(SAMPLE_FLAT_ROW)
        .thenReturn(SAMPLE_FLAT_ROW)
        .thenReturn(SAMPLE_FLAT_ROW)
        .thenReturn(null);
    doNothing().when(mockFlatRowScanner).close();

    try (org.apache.hadoop.hbase.client.ResultScanner actualResult =
        dataClientWrapper.readRows(query)) {

      assertArrayEquals(
          FLAT_ROW_ADAPTER.adaptResponse(SAMPLE_FLAT_ROW).rawCells(),
          actualResult.next().rawCells());

      assertEquals(2, actualResult.next(5).length);
    }

    verify(mockFlatRowScanner, times(4)).next();
    verify(mockFlatRowScanner).close();
    verify(delegate).readFlatRows(query.toProto(REQUEST_CONTEXT));
  }

  @Test
  public void testReadRowsAsync() throws Exception {
    Query query = Query.create(TABLE_ID);
    FlatRow anotherFlatRow = FlatRow.newBuilder().withRowKey(ROW_KEY).build();
    List<FlatRow> listFlatRows = ImmutableList.of(SAMPLE_FLAT_ROW, anotherFlatRow);
    when(delegate.readFlatRowsAsync(query.toProto(REQUEST_CONTEXT)))
        .thenReturn(Futures.immediateFuture(listFlatRows));

    List<Result> actualResult = dataClientWrapper.readRowsAsync(query).get();
    assertEquals(listFlatRows.size(), actualResult.size());
    assertArrayEquals(
        FLAT_ROW_ADAPTER.adaptResponse(SAMPLE_FLAT_ROW).rawCells(), actualResult.get(0).rawCells());
    assertArrayEquals(
        FLAT_ROW_ADAPTER.adaptResponse(anotherFlatRow).rawCells(), actualResult.get(1).rawCells());
    verify(delegate).readFlatRowsAsync(query.toProto(REQUEST_CONTEXT));
  }

  @Test
  public void testReadRowsAsyncWithStreamOb() {
    Query request = Query.create(TABLE_ID).rowKey(ROW_KEY);
    StreamObserver<Result> resultStreamOb =
        new StreamObserver<Result>() {
          @Override
          public void onNext(Result result) {}

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    when(delegate.readFlatRows(
            Mockito.<ReadRowsRequest>any(), Mockito.<StreamObserver<FlatRow>>any()))
        .thenReturn(
            new ScanHandler() {
              @Override
              public void cancel() {}
            });
    dataClientWrapper.readRowsAsync(request, resultStreamOb);
    verify(delegate)
        .readFlatRows(Mockito.<ReadRowsRequest>any(), Mockito.<StreamObserver<FlatRow>>any());
  }

  @Test
  public void testClose() throws Exception {
    doNothing().when(bigtableSession).close();
    dataClientWrapper.close();
    verify(bigtableSession).close();
  }
}
