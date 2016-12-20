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

import static com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScannerTest.RETRY_OPTIONS;

import static com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScannerTest.READ_ROWS_REQUEST_ALL;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
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
        new ReadRowsRetryListener(mockFlatRowObserver, RETRY_OPTIONS, READ_ROWS_REQUEST_ALL,
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
    underTest.onClose(Status.ABORTED, new Metadata());
    underTest.onMessage(buildResponse(key2));
    verify(mockFlatRowObserver, times(2)).onNext(any(FlatRow.class));
    checkRetryRequest(key2);

    finishOK(1);
  }

  protected void finishOK(int retryCount) {
    underTest.onClose(Status.OK, metaData);

    verify(mockOperationTimerContext, times(1)).close();
    verify(mockRpcTimerContext, times(retryCount + 1)).close();
    verify(mockRpcMetrics, times(retryCount)).markRetry();
  }

  public ReadRowsResponse buildResponse(ByteString key) throws UnsupportedEncodingException {
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

  private void start() {
    underTest.start();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));
    verify(mockRetryableRpc, times(1)).start(
      same(mockClientCall),
      eq(READ_ROWS_REQUEST_ALL),
      same(underTest),
      any(Metadata.class));
  }

  private void checkRetryRequest(ByteString key) {
    Assert.assertEquals(key,
      underTest.getRetryRequest().getRows().getRowRanges(0).getStartKeyOpen());
  }
}
