/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.Clock;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.Builder;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Test for the {@link RetryingReadRowsOperation}
 */
@RunWith(JUnit4.class)
public class RetryingReadRowsOperationTest {
  static final RetryOptions RETRY_OPTIONS = new RetryOptions.Builder().build();

  private static ReadRowsRequest READ_ENTIRE_TABLE_REQUEST =
      ReadRowsRequest.newBuilder()
          .setRows(RowSet.newBuilder().addRowRanges(RowRange.newBuilder()
              .setStartKeyClosed(ByteString.EMPTY).setEndKeyOpen(ByteString.EMPTY).build()))
          .setRowsLimit(10)
          .build();

  public static ReadRowsResponse buildResponse(ByteString ... keys) throws UnsupportedEncodingException {
    Builder builder = ReadRowsResponse.newBuilder();
    for (ByteString key : keys) {
      builder.addChunks(CellChunk.newBuilder()
        .setRowKey(key)
        .setFamilyName(StringValue.newBuilder().setValue("Family"))
        .setQualifier(BytesValue.newBuilder().setValue(ByteString.copyFrom("qualifier", "UTF-8")))
        .setValue(ByteString.copyFrom("value", "UTF-8"))
        .setCommitRow(true)
        .build());
    }
    return builder.build();
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock
  private StreamObserver<FlatRow> mockFlatRowObserver;

  @Mock
  private ScheduledExecutorService mockRetryExecutorService;

  @Mock
  private BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> mockRetryableRpc;

  @Mock
  private ClientCall<ReadRowsRequest, ReadRowsResponse> mockClientCall;
  @Mock
  private RpcMetrics mockRpcMetrics;
  @Mock
  private Timer.Context mockOperationTimerContext;
  @Mock
  private Timer.Context mockRpcTimerContext;

  @SuppressWarnings("rawtypes")
  @Mock
  ScheduledFuture scheduledFuture;

  private Metadata metaData;

  @SuppressWarnings({ "rawtypes" })
  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    metaData = new Metadata();

    when(mockRetryableRpc.newCall(any(CallOptions.class))).thenReturn(mockClientCall);
    when(mockRetryableRpc.getRpcMetrics()).thenReturn(mockRpcMetrics);
    when(mockRetryableRpc.getMethodDescriptor()).thenReturn(BigtableGrpc.METHOD_READ_ROWS);
    when(mockRpcMetrics.timeOperation()).thenReturn(mockOperationTimerContext);
    when(mockRpcMetrics.timeRpc()).thenReturn(mockRpcTimerContext);

    Answer<ScheduledFuture> runAutomatically = new Answer<ScheduledFuture>() {
      @Override
      public ScheduledFuture answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return scheduledFuture;
      }
    };
    when(
      mockRetryExecutorService.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
          .then(runAutomatically);
    doAnswer(runAutomatically).when(mockRetryExecutorService).execute(any(Runnable.class));
  }

  protected RetryingReadRowsOperation createOperation(StreamObserver<FlatRow> observer) {
    return new RetryingReadRowsOperation(observer, null, RETRY_OPTIONS, READ_ENTIRE_TABLE_REQUEST,
        mockRetryableRpc, CallOptions.DEFAULT, mockRetryExecutorService, metaData);
  }

  @Test
  public void testEmptyResponse() {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);
    ReadRowsResponse response = ReadRowsResponse.getDefaultInstance();

    underTest.onMessage(response);
    verify(mockFlatRowObserver, times(0)).onNext(any(FlatRow.class));
    verify(mockClientCall, times(2)).request(eq(1));

    finishOK(underTest, 0);
  }

  @Test
  public void testSingleResponse() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);
    ByteString key = ByteString.copyFrom("SomeKey", "UTF-8");
    ReadRowsResponse response = buildResponse(key);
    underTest.onMessage(response);
    verify(mockFlatRowObserver, times(1)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key, 9);
    verify(mockClientCall, times(2)).request(eq(1));

    finishOK(underTest, 0);
  }


  @Test
  public void testDoubleResponse() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);
    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    ReadRowsResponse response = buildResponse(key1, key2);
    underTest.onMessage(response);
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key2, 8);
    verify(mockClientCall, times(2)).request(eq(1));

    finishOK(underTest, 0);
  }

  @Test
  public void testSingleResponseWithQueue() throws UnsupportedEncodingException {
    ResponseQueueReader reader = new ResponseQueueReader(10000);
    RetryingReadRowsOperation underTest = createOperation(reader);
    start(underTest);
    ByteString key = ByteString.copyFrom("SomeKey", "UTF-8");
    ReadRowsResponse response = buildResponse(key);
    underTest.onMessage(response);
    checkRetryRequest(underTest, key, 9);

    verify(mockClientCall, times(1)).request(eq(1));

    finishOK(underTest, 0);
  }

  @Test
  public void testMultipleResponses() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key2, 8);
    verify(mockClientCall, times(3)).request(eq(1));

    finishOK(underTest, 0);
  }

  @Test
  public void testMultipleResponsesWithException() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onClose(Status.ABORTED, new Metadata());
    Assert.assertNull(underTest.getRowMerger().getLastCompletedRowKey());
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key2, 8);
    verify(mockClientCall, times(4)).request(eq(1));

    finishOK(underTest, 1);
  }

  @Test
  public void testScanTimeoutSucceed() throws UnsupportedEncodingException, BigtableRetriesExhaustedException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    final AtomicLong time = setupClock(underTest);
    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));

    // a round of successful retries.
    performSuccessfulScanTimeouts(underTest, time);
    Assert.assertNull(underTest.getRowMerger().getLastCompletedRowKey());
    underTest.onClose(Status.ABORTED, new Metadata());
    checkRetryRequest(underTest, key1, 9);

    // a message gets through
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key2, 8);

    // more successful retries
    performSuccessfulScanTimeouts(underTest, time);

    checkRetryRequest(underTest, key2, 8);
    verify(mockClientCall, times(RETRY_OPTIONS.getMaxScanTimeoutRetries() * 2 + 4)).request(eq(1));

    // a succsesful finish.  There were 2 x RETRY_OPTIONS.getMaxScanTimeoutRetries() timeouts,
    // and 1 ABORTED status.
    finishOK(underTest, RETRY_OPTIONS.getMaxScanTimeoutRetries() * 2 + 1);
  }

  @Test
  public void testScanTimeoutFail() throws UnsupportedEncodingException, BigtableRetriesExhaustedException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    final AtomicLong time = setupClock(underTest);

    ByteString key = ByteString.copyFrom("SomeKey1", "UTF-8");
    underTest.onMessage(buildResponse(key));

    // N successful scan timeout retries
    performSuccessfulScanTimeouts(underTest, time);

    checkRetryRequest(underTest, key, 9);

    // one last scan timeout that fails.
    thrown.expect(BigtableRetriesExhaustedException.class);
    performTimeout(underTest, time);
  }

  @Test
  public void testMixScanTimeoutAndStatusExceptions()
      throws UnsupportedEncodingException, BigtableRetriesExhaustedException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    final AtomicLong time = setupClock(underTest);
    int expectedRetryCount = 0;

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onClose(Status.ABORTED, new Metadata());
    Assert.assertNotNull(underTest.getCurrentBackoff());
    expectedRetryCount++;
    checkRetryRequest(underTest, key1, 9);

    // N successful scan timeout retries
    for (int i = 0; i < 2; i++) {
      performTimeout(underTest, time);
      expectedRetryCount++;
    }
    checkRetryRequest(underTest, key1, 9);
    Assert.assertNull(underTest.getCurrentBackoff());
    underTest.onMessage(buildResponse(key2));

    for (int i = 0; i < RETRY_OPTIONS.getMaxScanTimeoutRetries(); i++) {
      underTest.onClose(Status.ABORTED, new Metadata());
      expectedRetryCount++;

      performTimeout(underTest, time);
      Assert.assertNull(underTest.getCurrentBackoff());
      expectedRetryCount++;
    }

    verify(mockRpcMetrics, times(expectedRetryCount)).markRetry();
    verify(mockRpcTimerContext, times(expectedRetryCount)).close();

    thrown.expect(BigtableRetriesExhaustedException.class);
    performTimeout(underTest, time);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testImmediateOnClose() {
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(1, ClientCall.Listener.class).onClose(Status.OK, new Metadata());
        return null;
      }
    }).when(mockRetryableRpc).start(any(ReadRowsRequest.class), any(ClientCall.Listener.class),
      any(Metadata.class), eq(mockClientCall));

    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);

    // The test revolves around this call not throwing an exception.  It did at one point with
    // an invocation of call.request(1) when call is null.
    start(underTest);
  }

  protected void performTimeout(RetryingReadRowsOperation underTest, AtomicLong time)
      throws BigtableRetriesExhaustedException {
    time.addAndGet(RETRY_OPTIONS.getReadPartialRowTimeoutMillis() + 1);
    underTest.handleTimeout(new ScanTimeoutException("scan timeout"));
  }

  private AtomicLong setupClock(RetryingReadRowsOperation underTest) {
    final AtomicLong time = new AtomicLong(0);
    underTest.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return time.get();
      }
    };
    return time;
  }

  private void performSuccessfulScanTimeouts(RetryingReadRowsOperation underTest, AtomicLong time)
      throws BigtableRetriesExhaustedException {
    for (int i = 0; i < RETRY_OPTIONS.getMaxScanTimeoutRetries(); i++) {
      Assert.assertEquals(i, underTest.getTimeoutRetryCount());
      performTimeout(underTest, time);
    }
  }

  private void start(RetryingReadRowsOperation underTest) {
    underTest.getAsyncResult();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));
    verify(mockRetryableRpc, times(1)).start(
      eq(READ_ENTIRE_TABLE_REQUEST),
      same(underTest),
      any(Metadata.class),
      same(mockClientCall));
  }

  private void finishOK(RetryingReadRowsOperation underTest, int expectedRetryCount) {
    underTest.onClose(Status.OK, metaData);

    verify(mockOperationTimerContext, times(1)).close();
    verify(mockRpcMetrics, times(expectedRetryCount)).markRetry();
    verify(mockRpcTimerContext, times(expectedRetryCount + 1)).close();
  }

  private static void checkRetryRequest(RetryingReadRowsOperation underTest, ByteString key,
      int rowCount) {
    ReadRowsRequest request = underTest.buildUpdatedRequst();
    ByteString startKeyOpen = request.getRows().getRowRanges(0).getStartKeyOpen();
    String message = String.format("%s and %s are not equal", key.toStringUtf8(), startKeyOpen.toStringUtf8());
    Assert.assertEquals(message,  key, startKeyOpen);
    Assert.assertEquals(rowCount, request.getRowsLimit());
  }
}
