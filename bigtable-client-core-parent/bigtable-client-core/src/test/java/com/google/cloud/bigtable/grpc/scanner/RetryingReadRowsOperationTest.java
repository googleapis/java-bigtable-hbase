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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.util.ExponentialBackOff;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.OperationClock;
import com.google.cloud.bigtable.grpc.io.Watchdog;
import com.google.cloud.bigtable.grpc.io.Watchdog.StreamWaitTimeoutException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.Builder;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
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

  private static final RetryOptions RETRY_OPTIONS = new RetryOptions.Builder().build();

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

  @Mock
  private StreamObserver<FlatRow> mockFlatRowObserver;

  @Mock
  private ScheduledExecutorService mockRetryExecutorService;

  private OperationClock clock;

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
    when(mockRetryableRpc.getMethodDescriptor()).thenReturn(BigtableGrpc.getReadRowsMethod());
    when(mockRetryableRpc.isRetryable(any(ReadRowsRequest.class))).thenReturn(true);
    when(mockRpcMetrics.timeOperation()).thenReturn(mockOperationTimerContext);
    when(mockRpcMetrics.timeRpc()).thenReturn(mockRpcTimerContext);

    clock = new OperationClock();

    clock.initializeMockSchedule(mockRetryExecutorService, scheduledFuture);
  }

  protected RetryingReadRowsOperation createOperation(StreamObserver<FlatRow> observer) {
    return createOperation(CallOptions.DEFAULT, observer);
  }

  protected RetryingReadRowsOperation createOperation(CallOptions options,
      StreamObserver<FlatRow> observer) {
    return new RetryingReadRowsOperation(observer, RETRY_OPTIONS, READ_ENTIRE_TABLE_REQUEST,
        mockRetryableRpc, options, mockRetryExecutorService, metaData) {
      protected ExponentialBackOff createBackoff(int maxTimeoutMs) {
        return clock.createBackoff(retryOptions, maxTimeoutMs);
      }
    };
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
    ResponseQueueReader reader = new ResponseQueueReader();
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
  public void testFailure_default() throws Exception {
    testFailure(CallOptions.DEFAULT, RETRY_OPTIONS.getMaxElapsedBackoffMillis());
  }

  @Test
  public void testFailure_withDeadline() throws Exception {
    testFailure(clock.createCallOptionsWithDeadline(5, TimeUnit.SECONDS),
        (int) TimeUnit.SECONDS.toMillis(5));
  }

  private void testFailure(CallOptions options, int expectedMaxElapsedTimeMs)
      throws InterruptedException, java.util.concurrent.TimeoutException {
    doAnswer(new Answer<Void>() {
      @Override public Void answer(InvocationOnMock invocation) {
        invocation.getArgumentAt(1, ClientCall.Listener.class)
            .onClose(Status.DEADLINE_EXCEEDED, new Metadata());
        return null;
      }
    }).when(mockRetryableRpc)
        .start(any(ReadRowsRequest.class), any(ClientCall.Listener.class), any(Metadata.class),
            any(ClientCall.class));

    RetryingReadRowsOperation underTest =
        createOperation(options, mockFlatRowObserver);
    try {
      underTest.getAsyncResult().get(100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.assertEquals(BigtableRetriesExhaustedException.class, e.getCause().getClass());
      Assert.assertEquals(Status.DEADLINE_EXCEEDED.getCode(), Status.fromThrowable(e).getCode());
    }

    int actualElapsedTimeMs =
        ((ExponentialBackOff) underTest.getCurrentBackoff()).getMaxElapsedTimeMillis();

    Assert.assertEquals(expectedMaxElapsedTimeMs, actualElapsedTimeMs);
    clock.assertTimeWithinExpectations(TimeUnit.MILLISECONDS.toNanos(actualElapsedTimeMs));
  }

  @Test
  public void testMultipleResponsesWithException() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onClose(Status.ABORTED, new Metadata());
    Assert.assertFalse(underTest.getRowMerger().isComplete());
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key2, 8);
    verify(mockClientCall, times(4)).request(eq(1));

    finishOK(underTest, 1);
  }

  @Test
  public void testScanTimeoutSucceed() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    ByteString key0 = ByteString.copyFrom("SomeKey0", "UTF-8");
    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");

    underTest.onMessage(buildResponse(key0));

    // A partial row is found
    underTest.onMessage(setCommitToFalse(buildResponse(key1)));
    Assert.assertFalse(underTest.getRowMerger().isInNewState());

    // a round of successful retries.
    performSuccessfulScanTimeouts(underTest);
    Assert.assertTrue(underTest.getRowMerger().isInNewState());

    underTest.onClose(Status.ABORTED, new Metadata());
    Assert.assertFalse(underTest.getRowMerger().isComplete());
    checkRetryRequest(underTest, key0, 9);

    // a message gets through
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(underTest, key2, 8);

    // more successful retries
    performSuccessfulScanTimeouts(underTest);

    checkRetryRequest(underTest, key2, 8);
    verify(mockClientCall, atLeast(RETRY_OPTIONS.getMaxScanTimeoutRetries() * 2)).request(eq(1));

    // a successful finish.  There were 2 x RETRY_OPTIONS.getMaxScanTimeoutRetries() timeouts,
    // and 1 ABORTED status.
    finishOK(underTest, RETRY_OPTIONS.getMaxScanTimeoutRetries() * 2 + 1);
  }

  private ReadRowsResponse setCommitToFalse(ReadRowsResponse message) {
    int lastIndex = message.getChunksCount() - 1;
    CellChunk lastChunk = message.getChunks(lastIndex);
    return message.toBuilder()
        .setChunks(lastIndex, lastChunk.toBuilder().setCommitRow(false).build())
        .build();
  }

  @Test
  public void testScanTimeoutFail() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

    ByteString key = ByteString.copyFrom("SomeKey1", "UTF-8");
    underTest.onMessage(buildResponse(key));

    // N successful scan timeout retries
    performSuccessfulScanTimeouts(underTest);

    checkRetryRequest(underTest, key, 9);

    // one last scan timeout that fails.
    performTimeout(underTest);
    verify(mockFlatRowObserver).onError(any(BigtableRetriesExhaustedException.class));
  }

  @Test
  public void testMixScanTimeoutAndStatusExceptions() throws UnsupportedEncodingException {
    RetryingReadRowsOperation underTest = createOperation(mockFlatRowObserver);
    start(underTest);

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
      performTimeout(underTest);
      expectedRetryCount++;
    }
    checkRetryRequest(underTest, key1, 9);
    Assert.assertNull(underTest.getCurrentBackoff());
    underTest.onMessage(buildResponse(key2));

    for (int i = 0; i < RETRY_OPTIONS.getMaxScanTimeoutRetries(); i++) {
      underTest.onClose(Status.ABORTED, new Metadata());
      expectedRetryCount++;

      performTimeout(underTest);
      Assert.assertNull(underTest.getCurrentBackoff());
      expectedRetryCount++;
    }

    verify(mockRpcMetrics, times(expectedRetryCount)).markRetry();
    verify(mockRpcTimerContext, times(expectedRetryCount)).close();

    performTimeout(underTest);
    verify(mockFlatRowObserver).onError(any(BigtableRetriesExhaustedException.class));
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

  protected void performTimeout(RetryingReadRowsOperation underTest) {
    underTest.onClose(
        Status.CANCELLED.withCause(new
            StreamWaitTimeoutException(
                Watchdog.State.WAITING, RETRY_OPTIONS.getReadPartialRowTimeoutMillis())),
        new Metadata()
    );
  }

  private void performSuccessfulScanTimeouts(RetryingReadRowsOperation underTest) {
    for (int i = 0; i < RETRY_OPTIONS.getMaxScanTimeoutRetries(); i++) {
      Assert.assertEquals(i, underTest.getTimeoutRetryCount());
      performTimeout(underTest);
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
    Assert.assertTrue(underTest.getRowMerger().isComplete());
  }

  private static void checkRetryRequest(RetryingReadRowsOperation underTest, ByteString key,
      int rowCount) {
    ReadRowsRequest request = underTest.buildUpdatedRequest();
    Assert.assertEquals(key, request.getRows().getRowRanges(0).getStartKeyOpen());
    Assert.assertEquals(rowCount, request.getRowsLimit());
  }
}
