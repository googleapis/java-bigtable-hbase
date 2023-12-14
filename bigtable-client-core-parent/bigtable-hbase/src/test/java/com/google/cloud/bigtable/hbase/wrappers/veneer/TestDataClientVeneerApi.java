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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StreamController;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings.ClientOperationTimeouts;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
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
public class TestDataClientVeneerApi {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final String TABLE_ID = "fake-table";
  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("row-key");

  private AtomicBoolean cancelled = new AtomicBoolean(false);

  private static final Row MODEL_ROW =
      Row.create(
          ROW_KEY,
          ImmutableList.of(
              RowCell.create(
                  "cf",
                  ByteString.copyFromUtf8("q"),
                  10000L,
                  ImmutableList.of("label"),
                  ByteString.copyFromUtf8("value"))));

  private static final Result EXPECTED_RESULT =
      Result.create(
          ImmutableList.<Cell>of(
              new com.google.cloud.bigtable.hbase.adapters.read.RowCell(
                  Bytes.toBytes("row-key"),
                  Bytes.toBytes("cf"),
                  Bytes.toBytes("q"),
                  10L,
                  Bytes.toBytes("value"),
                  ImmutableList.of("label"))));

  @Mock private BigtableDataClient mockDataClient;

  @Mock private Batcher<RowMutationEntry, Void> mockMutationBatcher;

  @Mock private Batcher<ByteString, Row> mockReadBatcher;

  @Mock private ServerStreamingCallable<Query, Result> mockStreamingCallable;

  @Mock private ServerStream<Result> serverStream;

  @Mock private UnaryCallable<Query, List<Result>> mockUnaryCallable;

  private DataClientVeneerApi dataClientWrapper;

  @Before
  public void setUp() throws Exception {
    dataClientWrapper = new DataClientVeneerApi(mockDataClient, ClientOperationTimeouts.EMPTY);
  }

  @Test
  public void testCreateBulkMutation() throws Exception {
    RowMutationEntry entry = RowMutationEntry.create(ROW_KEY);
    when(mockDataClient.newBulkMutationBatcher(TABLE_ID)).thenReturn(mockMutationBatcher);
    when(mockMutationBatcher.add(entry)).thenReturn(ApiFutures.<Void>immediateFuture(null));
    BulkMutationWrapper mutationWrapper = dataClientWrapper.createBulkMutation(TABLE_ID);
    mutationWrapper.add(entry).get();
    verify(mockDataClient).newBulkMutationBatcher(TABLE_ID);
    verify(mockMutationBatcher).add(entry);
  }

  @Test
  public void testCreateBulkRead() throws Exception {
    when(mockDataClient.newBulkReadRowsBatcher(
            Mockito.eq(TABLE_ID), Mockito.<Filter>isNull(), Mockito.any(GrpcCallContext.class)))
        .thenReturn(mockReadBatcher);
    when(mockReadBatcher.add(ROW_KEY)).thenReturn(ApiFutures.immediateFuture(MODEL_ROW));
    BulkReadWrapper bulkReadWrapper = dataClientWrapper.createBulkRead(TABLE_ID);
    assertResult(EXPECTED_RESULT, bulkReadWrapper.add(ROW_KEY, null).get());
    verify(mockDataClient)
        .newBulkReadRowsBatcher(
            Mockito.eq(TABLE_ID), Mockito.<Filter>isNull(), Mockito.any(GrpcCallContext.class));
    verify(mockReadBatcher).add(ROW_KEY);
  }

  @Test
  public void testMutateRowAsync() throws Exception {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ROW_KEY);
    when(mockDataClient.mutateRowAsync(rowMutation))
        .thenReturn(ApiFutures.<Void>immediateFuture(null));
    dataClientWrapper.mutateRowAsync(rowMutation).get();
    verify(mockDataClient).mutateRowAsync(rowMutation);
  }

  @Test
  public void testReadModifyWriteRowAsync() throws Exception {
    ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(TABLE_ID, ROW_KEY);
    when(mockDataClient.readModifyWriteRowAsync(readModifyWriteRow))
        .thenReturn(ApiFutures.immediateFuture(MODEL_ROW));
    Result actualResult = dataClientWrapper.readModifyWriteRowAsync(readModifyWriteRow).get();
    assertResult(EXPECTED_RESULT, actualResult);
    verify(mockDataClient).readModifyWriteRowAsync(readModifyWriteRow);
  }

  @Test
  public void testCheckAndMutateRowAsync() throws Exception {
    ConditionalRowMutation conditionalRowM = ConditionalRowMutation.create(TABLE_ID, ROW_KEY);
    when(mockDataClient.checkAndMutateRowAsync(conditionalRowM))
        .thenReturn(ApiFutures.immediateFuture(Boolean.TRUE));
    assertTrue(dataClientWrapper.checkAndMutateRowAsync(conditionalRowM).get());
    verify(mockDataClient).checkAndMutateRowAsync(conditionalRowM);
  }

  @Test
  public void testSampleRowKeysAsync() throws Exception {
    List<KeyOffset> keyOffsets =
        ImmutableList.of(
            KeyOffset.create(ByteString.copyFromUtf8("a"), 1),
            KeyOffset.create(ByteString.copyFromUtf8("z"), 1));
    when(mockDataClient.sampleRowKeysAsync(TABLE_ID))
        .thenReturn(ApiFutures.immediateFuture(keyOffsets));
    assertEquals(keyOffsets, dataClientWrapper.sampleRowKeysAsync(TABLE_ID).get());
    verify(mockDataClient).sampleRowKeysAsync(TABLE_ID);
  }

  @Test
  public void testReadRowAsync() throws Exception {
    Query expectedRequest = Query.create(TABLE_ID).rowKey(ROW_KEY).limit(1);
    UnaryCallable<Query, Row> mockCallable = Mockito.mock(UnaryCallable.class);
    when(mockCallable.futureCall(Mockito.eq(expectedRequest), Mockito.any(GrpcCallContext.class)))
        .thenReturn(ApiFutures.immediateFuture(MODEL_ROW));

    when(mockDataClient.readRowCallable()).thenReturn(mockCallable);
    Result actualResult = dataClientWrapper.readRowAsync(TABLE_ID, ROW_KEY, null).get();
    assertResult(EXPECTED_RESULT, actualResult);
    verify(mockCallable)
        .futureCall(Mockito.eq(expectedRequest), Mockito.any(GrpcCallContext.class));
  }

  @Test
  public void testReadRows_Errors() throws IOException {
    Query query = Query.create(TABLE_ID).rowKey(ROW_KEY);
    when(mockDataClient.readRowsCallable(Mockito.any(RowResultAdapter.class)))
        .thenReturn(mockStreamingCallable);
    when(mockStreamingCallable.call(Mockito.any(Query.class), Mockito.any(GrpcCallContext.class)))
        .thenReturn(serverStream);
    when(serverStream.iterator())
        .thenReturn(
            new Iterator<Result>() {
              @Override
              public boolean hasNext() {
                return true;
              }

              @Override
              public Result next() {
                throw new InternalException(
                    "fake error", null, GrpcStatusCode.of(Code.INTERNAL), false);
              }
            })
        .thenReturn(ImmutableList.<Result>of().iterator());

    assertThrows(Exception.class, () -> dataClientWrapper.readRows(query).next());

    ResultScanner noRowsResultScanner = dataClientWrapper.readRows(query);
    assertNull(noRowsResultScanner.next());
    noRowsResultScanner.close();

    verify(mockDataClient, times(2)).readRowsCallable(Mockito.<RowResultAdapter>any());
    verify(serverStream, times(2)).iterator();
    verify(mockStreamingCallable, times(2))
        .call(Mockito.any(Query.class), Mockito.any(GrpcCallContext.class));
  }

  @Test
  public void testReadRows() throws IOException {
    Query query = Query.create(TABLE_ID).rowKey(ROW_KEY);
    when(mockDataClient.readRowsCallable(Mockito.<RowResultAdapter>any()))
        .thenReturn(mockStreamingCallable)
        .thenReturn(mockStreamingCallable);
    when(serverStream.iterator())
        .thenReturn(
            ImmutableList.of(Result.EMPTY_RESULT, EXPECTED_RESULT, EXPECTED_RESULT).iterator())
        .thenReturn(ImmutableList.<Result>of().iterator());
    when(mockStreamingCallable.call(Mockito.eq(query), Mockito.any(GrpcCallContext.class)))
        .thenReturn(serverStream)
        .thenReturn(serverStream);

    ResultScanner resultScanner = dataClientWrapper.readRows(query);
    assertResult(Result.EMPTY_RESULT, resultScanner.next());
    assertResult(EXPECTED_RESULT, resultScanner.next());

    doNothing().when(serverStream).cancel();
    resultScanner.close();

    ResultScanner noRowsResultScanner = dataClientWrapper.readRows(query);
    assertNull(noRowsResultScanner.next());

    verify(serverStream).cancel();
    verify(mockDataClient, times(2)).readRowsCallable(Mockito.<RowResultAdapter>any());
    verify(serverStream, times(2)).iterator();
    verify(mockStreamingCallable, times(2))
        .call(Mockito.eq(query), Mockito.any(GrpcCallContext.class));
  }

  private static Result createRow(String key) {
    return Result.create(
        ImmutableList.<Cell>of(
            new com.google.cloud.bigtable.hbase.adapters.read.RowCell(
                Bytes.toBytes(key),
                Bytes.toBytes("cf"),
                Bytes.toBytes("q"),
                10L,
                Bytes.toBytes("value"),
                ImmutableList.of("label"))));
  }

  @Test
  public void testReadPaginatedRows() throws IOException {
    Query query = Query.create(TABLE_ID).range("a", "z");
    when(mockDataClient.readRowsCallable(Mockito.<RowResultAdapter>any()))
        .thenReturn(mockStreamingCallable);

    // First Page
    doAnswer(
            (args) -> {
              ResponseObserver<Result> observer = args.getArgument(1);
              observer.onResponse(createRow("a"));
              observer.onResponse(createRow("b"));
              observer.onComplete();
              return null;
            })
        .when(mockStreamingCallable)
        .call(
            Mockito.eq(Query.create(TABLE_ID).range("a", "z").limit(2)),
            Mockito.any(),
            Mockito.any());

    // 2nd Page
    doAnswer(
            (args) -> {
              ResponseObserver<Result> observer = args.getArgument(1);
              observer.onResponse(createRow("c"));
              observer.onResponse(createRow("d"));
              observer.onComplete();
              return null;
            })
        .when(mockStreamingCallable)
        .call(
            Mockito.eq(
                Query.create(TABLE_ID)
                    .range(ByteStringRange.unbounded().startOpen("b").endOpen("z"))
                    .limit(2)),
            Mockito.any(),
            Mockito.any());

    // 3rd Page
    doAnswer(
            (args) -> {
              ResponseObserver<Result> observer = args.getArgument(1);
              observer.onResponse(createRow("e"));
              observer.onComplete();
              return null;
            })
        .when(mockStreamingCallable)
        .call(
            Mockito.eq(
                Query.create(TABLE_ID)
                    .range(ByteStringRange.unbounded().startOpen("d").endOpen("z"))
                    .limit(2)),
            Mockito.any(),
            Mockito.any());

    // 3rd Page
    doAnswer(
            (args) -> {
              ResponseObserver<Result> observer = args.getArgument(1);
              observer.onComplete();
              return null;
            })
        .when(mockStreamingCallable)
        .call(
            Mockito.eq(
                Query.create(TABLE_ID)
                    .range(ByteStringRange.unbounded().startOpen("e").endOpen("z"))
                    .limit(2)),
            Mockito.any(),
            Mockito.any());

    ResultScanner resultScanner = dataClientWrapper.readRows(query.createPaginator(2), 1000);

    assertResult(createRow("a"), resultScanner.next());
    assertResult(createRow("b"), resultScanner.next());
    assertResult(createRow("c"), resultScanner.next());
    assertResult(createRow("d"), resultScanner.next());
    assertResult(createRow("e"), resultScanner.next());
    assertNull(resultScanner.next());
  }

  @Test
  public void testReadRowsLowMemory() throws IOException {
    Query query = Query.create(TABLE_ID);
    when(mockDataClient.readRowsCallable(Mockito.any(RowResultAdapter.class)))
        .thenReturn(mockStreamingCallable);

    StreamController mockController = Mockito.mock(StreamController.class);
    doAnswer(
            invocation -> {
              cancelled.set(true);
              return null;
            })
        .when(mockController)
        .cancel();

    // Generate
    doAnswer(
            (Answer<Void>)
                invocation -> {
                  ResponseObserver<Result> observer = invocation.getArgument(1);
                  observer.onStart(mockController);

                  for (int i = 0; i < 1000 && !cancelled.get(); i++) {
                    observer.onResponse(createRow(String.format("row%010d", i)));
                    Thread.sleep(10);
                  }
                  observer.onComplete();
                  return null;
                })
        .doAnswer(
            (Answer<Void>)
                invocation -> {
                  ResponseObserver<Result> observer = invocation.getArgument(1);
                  observer.onComplete();
                  return null;
                })
        .when(mockStreamingCallable)
        .call(
            Mockito.any(Query.class),
            Mockito.any(ResponseObserver.class),
            Mockito.any(GrpcCallContext.class));

    ResultScanner resultScanner = dataClientWrapper.readRows(query.createPaginator(100), 3);
    // Consume the stream
    Lists.newArrayList(resultScanner);

    verify(mockStreamingCallable, times(2))
        .call(
            Mockito.any(Query.class),
            Mockito.any(ResponseObserver.class),
            Mockito.any(GrpcCallContext.class));
    assertTrue(cancelled.get());
  }

  @Test
  public void testReadRowsCancel() throws IOException {
    Query query = Query.create(TABLE_ID).rowKey(ROW_KEY);
    when(mockDataClient.readRowsCallable(Mockito.<RowResultAdapter>any()))
        .thenReturn(mockStreamingCallable)
        .thenReturn(mockStreamingCallable);

    when(mockStreamingCallable.call(Mockito.eq(query), Mockito.any(GrpcCallContext.class)))
        .thenReturn(serverStream);

    Iterator<Result> mockIter = Mockito.mock(Iterator.class);
    when(serverStream.iterator()).thenReturn(mockIter);
    when(mockIter.hasNext()).thenReturn(true);
    when(mockIter.next()).thenReturn(EXPECTED_RESULT);

    ResultScanner resultScanner = dataClientWrapper.readRows(query);
    assertResult(EXPECTED_RESULT, resultScanner.next());

    doNothing().when(serverStream).cancel();
    resultScanner.close();

    // make sure that the scanner doesn't interact with the iterator on close
    verify(serverStream).cancel();
    verify(mockIter, times(1)).hasNext();
    verify(mockIter, times(1)).next();
  }

  @Test
  public void testReadRowsAsync() throws Exception {
    Query query = Query.create(TABLE_ID).rowKey(ROW_KEY);
    when(mockDataClient.readRowsCallable(Mockito.<RowResultAdapter>any()))
        .thenReturn(mockStreamingCallable);
    when(mockStreamingCallable.all()).thenReturn(mockUnaryCallable);
    List<Result> expectedResult = ImmutableList.of(Result.EMPTY_RESULT, EXPECTED_RESULT);
    when(mockUnaryCallable.futureCall(Mockito.eq(query), Mockito.any(GrpcCallContext.class)))
        .thenReturn(ApiFutures.immediateFuture(expectedResult));

    List<Result> actualResult = dataClientWrapper.readRowsAsync(query).get();

    assertEquals(expectedResult.size(), actualResult.size());
    assertResult(Result.EMPTY_RESULT, actualResult.get(0));
    assertResult(EXPECTED_RESULT, actualResult.get(1));

    verify(mockDataClient).readRowsCallable(Mockito.<RowResultAdapter>any());
    verify(mockStreamingCallable).all();
    verify(mockUnaryCallable).futureCall(Mockito.eq(query), Mockito.any(GrpcCallContext.class));
  }

  @Test
  public void testReadRowsAsyncWithStreamOb() {
    final Exception readException = new Exception();
    Query request = Query.create(TABLE_ID).rowKey(ROW_KEY);
    StreamObserver<Result> resultStreamOb =
        new StreamObserver<Result>() {
          @Override
          public void onNext(Result result) {
            assertResult(EXPECTED_RESULT, result);
          }

          @Override
          public void onError(Throwable throwable) {
            assertEquals(readException, throwable);
          }

          @Override
          public void onCompleted() {}
        };
    when(mockDataClient.readRowsCallable(Mockito.<RowResultAdapter>any()))
        .thenReturn(mockStreamingCallable);
    doAnswer(
            new Answer() {
              int count = 0;

              @Override
              public Object answer(InvocationOnMock invocationOnMock) {
                ResponseObserver<Result> resObserver = invocationOnMock.getArgument(1);
                resObserver.onStart(null);
                resObserver.onResponse(EXPECTED_RESULT);
                if (count == 0) {
                  resObserver.onComplete();
                } else {
                  resObserver.onError(readException);
                }
                count++;
                return null;
              }
            })
        .when(mockStreamingCallable)
        .call(Mockito.<Query>any(), Mockito.<ResponseObserver<Result>>any());

    dataClientWrapper.readRowsAsync(request, resultStreamOb);
    dataClientWrapper.readRowsAsync(request, resultStreamOb);
    verify(mockDataClient, times(2)).readRowsCallable(Mockito.<RowResultAdapter>any());
    verify(mockStreamingCallable, times(2))
        .call(
            Mockito.<Query>any(),
            Mockito.<ResponseObserver<Result>>any(),
            Mockito.any(GrpcCallContext.class));
  }

  @Test
  public void testClose() {
    doNothing().when(mockDataClient).close();
    dataClientWrapper.close();
    verify(mockDataClient).close();
  }

  private void assertResult(Result expected, Result actual) {
    try {
      Result.compareResults(expected, actual);
    } catch (Throwable throwable) {
      throw new AssertionError("Result did not match", throwable);
    }
  }
}
