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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import com.google.api.client.util.Clock;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc.RpcMetrics;
import com.google.cloud.bigtable.metrics.Timer;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.junit.Assert;

/**
 * Test for the {@link ReadRowsRetryListener}
 */
@RunWith(JUnit4.class)
public class ReadRowsRetryListenerTest {
  static final RetryOptions RETRY_OPTIONS = new RetryOptions.Builder().build();

  private static ReadRowsRequest READ_ENTIRE_TABLE_REQUEST =
      ReadRowsRequest.newBuilder()
          .setRows(RowSet.newBuilder().addRowRanges(RowRange.newBuilder()
              .setStartKeyClosed(ByteString.EMPTY).setEndKeyOpen(ByteString.EMPTY).build()))
          .build();

  private static ReadRowsResponse buildResponse(ByteString key) throws UnsupportedEncodingException {
    return ReadRowsResponse.newBuilder()
        .addChunks(CellChunk.newBuilder()
          .setRowKey(key)
          .setFamilyName(StringValue.newBuilder().setValue("Family"))
          .setQualifier(BytesValue.newBuilder().setValue(ByteString.copyFrom("qualifier", "UTF-8")))
          .setValue(ByteString.copyFrom("value", "UTF-8"))
          .setCommitRow(true)
          .build())
        .build();
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

  private ReadRowsRetryListener underTest;

  @SuppressWarnings("rawtypes")
  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    metaData = new Metadata();

    when(mockRetryableRpc.newCall(any(CallOptions.class))).thenReturn(mockClientCall);
    when(mockRetryableRpc.getRpcMetrics()).thenReturn(mockRpcMetrics);
    when(mockRpcMetrics.timeOperation()).thenReturn(mockOperationTimerContext);
    when(mockRpcMetrics.timeRpc()).thenReturn(mockRpcTimerContext);

    underTest =
        new ReadRowsRetryListener(mockFlatRowObserver, RETRY_OPTIONS, READ_ENTIRE_TABLE_REQUEST,
            mockRetryableRpc, CallOptions.DEFAULT, mockRetryExecutorService, metaData);

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

  @Test
  public void testEmptyResponse() {
    start();
    ReadRowsResponse response = ReadRowsResponse.getDefaultInstance();
    underTest.onMessage(response);
    verify(mockFlatRowObserver, times(0)).onNext(any(FlatRow.class));

    finishOK(0);
  }

  @Test
  public void testSingleResponse() throws UnsupportedEncodingException {
    start();
    ByteString key = ByteString.copyFrom("SomeKey", "UTF-8");
    ReadRowsResponse response = buildResponse(key);
    underTest.onMessage(response);
    verify(mockFlatRowObserver, times(1)).onNext(any(FlatRow.class));
    checkRetryRequest(key);

    finishOK(0);
  }

  @Test
  public void testMultipleResponses() throws UnsupportedEncodingException {
    start();

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(key2);

    finishOK(0);
  }

  @Test
  public void testMultipleResponsesWithException() throws UnsupportedEncodingException {
    start();

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    RowMerger rw1 = underTest.getRowMerger();
    underTest.onClose(Status.ABORTED, new Metadata());
    Assert.assertNotSame(rw1, underTest.getRowMerger());
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(key2);

    finishOK(1);
  }

  @Test
  public void testScanTimeoutSucceed() throws UnsupportedEncodingException, BigtableRetriesExhaustedException {
    start();

    final AtomicLong time = setupClock();
    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));

    // a round of successful retries.
    RowMerger rw1 = underTest.getRowMerger();
    performSuccessfulScanTimeouts(time);
    Assert.assertNotSame(rw1, underTest.getRowMerger());
    underTest.onClose(Status.ABORTED, new Metadata());
    checkRetryRequest(key1);

    // a message gets through
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(key2);

    // more successful retries
    performSuccessfulScanTimeouts(time);

    checkRetryRequest(key2);

    // a succsesful finish.  There were 2 x RETRY_OPTIONS.getMaxScanTimeoutRetries() timeouts,
    // and 1 ABORTED status.
    finishOK(RETRY_OPTIONS.getMaxScanTimeoutRetries() * 2 + 1);
  }

  @Test
  public void testScanTimeoutFail() throws UnsupportedEncodingException, BigtableRetriesExhaustedException {
    start();

    final AtomicLong time = setupClock();

    ByteString key = ByteString.copyFrom("SomeKey1", "UTF-8");
    underTest.onMessage(buildResponse(key));

    // N successful scan timeout retries
    performSuccessfulScanTimeouts(time);

    checkRetryRequest(key);

    // one last scan timeout that fails.
    thrown.expect(BigtableRetriesExhaustedException.class);
    performTimeout(time);
  }


  @Test
  public void testMixScanTimeoutAndStatusExceptions()
      throws UnsupportedEncodingException, BigtableRetriesExhaustedException {
    start();

    final AtomicLong time = setupClock();
    int expectedRetryCount = 0;

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onClose(Status.ABORTED, new Metadata());
    Assert.assertNotNull(underTest.getCurrentBackoff());
    expectedRetryCount++;
    checkRetryRequest(key1);

    // N successful scan timeout retries
    for (int i = 0; i < 2; i++) {
      performTimeout(time);
      expectedRetryCount++;
    }
    checkRetryRequest(key1);
    Assert.assertNull(underTest.getCurrentBackoff());
    underTest.onMessage(buildResponse(key2));

    for (int i = 0; i < RETRY_OPTIONS.getMaxScanTimeoutRetries(); i++) {
      underTest.onClose(Status.ABORTED, new Metadata());
      expectedRetryCount++;

      performTimeout(time);
      Assert.assertNull(underTest.getCurrentBackoff());
      expectedRetryCount++;
    }

    verify(mockRpcMetrics, times(expectedRetryCount)).markRetry();
    verify(mockRpcTimerContext, times(expectedRetryCount)).close();

    thrown.expect(BigtableRetriesExhaustedException.class);
    performTimeout(time);
  }

  protected void performTimeout(AtomicLong time) throws BigtableRetriesExhaustedException {
    time.addAndGet(RETRY_OPTIONS.getReadPartialRowTimeoutMillis() + 1);
    underTest.handleTimeout(new ScanTimeoutException("scan timeout"));
  }

  private AtomicLong setupClock() {
    final AtomicLong time = new AtomicLong(0);
    underTest.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return time.get();
      }
    };
    return time;
  }

  private void performSuccessfulScanTimeouts(final AtomicLong time) throws BigtableRetriesExhaustedException {
    for (int i = 0; i < RETRY_OPTIONS.getMaxScanTimeoutRetries(); i++) {
      Assert.assertEquals(i, underTest.getTimeoutRetryCount());
      performTimeout(time);
    }
  }

  private void start() {
    underTest.start();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));
    verify(mockRetryableRpc, times(1)).start(
      same(mockClientCall),
      eq(READ_ENTIRE_TABLE_REQUEST),
      same(underTest),
      any(Metadata.class));
  }

  private void finishOK(int expectedRetryCount) {
    underTest.onClose(Status.OK, metaData);

    verify(mockOperationTimerContext, times(1)).close();
    verify(mockRpcMetrics, times(expectedRetryCount)).markRetry();
    verify(mockRpcTimerContext, times(expectedRetryCount + 1)).close();
  }

  private void checkRetryRequest(ByteString key) {
    Assert.assertEquals(key,
      underTest.getRetryRequest().getRows().getRowRanges(0).getStartKeyOpen());
  }
}
