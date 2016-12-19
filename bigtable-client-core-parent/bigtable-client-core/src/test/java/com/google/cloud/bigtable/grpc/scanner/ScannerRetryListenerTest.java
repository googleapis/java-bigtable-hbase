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
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.protobuf.ByteString;
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
 * Test for the {@link ScannerRetryListener}
 */
@RunWith(JUnit4.class)
public class ScannerRetryListenerTest {

  @Mock
  private StreamObserver<ReadRowsResponse> mockResponseObserver;

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

  private Metadata metaData;



  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    metaData = new Metadata();

    when(mockRetryableRpc.newCall(any(CallOptions.class))).thenReturn(mockClientCall);
    when(mockRetryableRpc.getRpcMetrics()).thenReturn(mockRpcMetrics);
    when(mockRpcMetrics.timeOperation()).thenReturn(mockOperationTimerContext);
    when(mockRpcMetrics.timeRpc()).thenReturn(mockRpcTimerContext);
  }

  @Test
  public void testEmptyResponse() {
    ScannerRetryListener underTest =
        new ScannerRetryListener(mockResponseObserver, RETRY_OPTIONS, READ_ROWS_REQUEST_ALL,
            mockRetryableRpc, CallOptions.DEFAULT, mockRetryExecutorService, metaData);

    underTest.start();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));

    ReadRowsResponse response = ReadRowsResponse.getDefaultInstance();
    underTest.onMessage(response);
    verify(mockRetryableRpc, times(1)).start(
      same(mockClientCall),
      eq(READ_ROWS_REQUEST_ALL),
      same(underTest),
      any(Metadata.class));
    verify(mockResponseObserver, times(1)).onNext(same(response));

    underTest.onClose(Status.OK, metaData);
    verify(mockOperationTimerContext, times(1)).close();
    verify(mockRpcTimerContext, times(1)).close();
    
  }

  @Test
  public void testSingleResponse() throws UnsupportedEncodingException {
    ScannerRetryListener underTest =
        new ScannerRetryListener(mockResponseObserver, RETRY_OPTIONS, READ_ROWS_REQUEST_ALL,
            mockRetryableRpc, CallOptions.DEFAULT, mockRetryExecutorService, metaData);

    underTest.start();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));

    ByteString key = ByteString.copyFrom("SomeKey", "UTF-8");
    ReadRowsResponse response = buildResponse(key);
    underTest.onMessage(response);
    verify(mockRetryableRpc, times(1)).start(
      same(mockClientCall),
      eq(READ_ROWS_REQUEST_ALL),
      same(underTest),
      any(Metadata.class));
    verify(mockResponseObserver, times(1)).onNext(same(response));

    underTest.onClose(Status.OK, metaData);
    verify(mockOperationTimerContext, times(1)).close();
    verify(mockRpcTimerContext, times(1)).close();

    Assert.assertEquals(key,
      underTest.getRetryRequest().getRows().getRowRanges(0).getStartKeyOpen());
  }

  @Test
  public void testMultipleResponses() throws UnsupportedEncodingException {
    ScannerRetryListener underTest =
        new ScannerRetryListener(mockResponseObserver, RETRY_OPTIONS, READ_ROWS_REQUEST_ALL,
            mockRetryableRpc, CallOptions.DEFAULT, mockRetryExecutorService, metaData);

    underTest.start();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onMessage(buildResponse(key2));
    verify(mockRetryableRpc, times(1)).start(
      same(mockClientCall),
      eq(READ_ROWS_REQUEST_ALL),
      same(underTest),
      any(Metadata.class));
    verify(mockResponseObserver, times(2)).onNext(any(ReadRowsResponse.class));

    underTest.onClose(Status.OK, metaData);
    verify(mockOperationTimerContext, times(1)).close();
    verify(mockRpcTimerContext, times(1)).close();

    Assert.assertEquals(key2,
      underTest.getRetryRequest().getRows().getRowRanges(0).getStartKeyOpen());
  }


  @Test
  public void testMultipleResponsesWithException() throws UnsupportedEncodingException {
    ScannerRetryListener underTest =
        new ScannerRetryListener(mockResponseObserver, RETRY_OPTIONS, READ_ROWS_REQUEST_ALL,
            mockRetryableRpc, CallOptions.DEFAULT, mockRetryExecutorService, metaData);

    underTest.start();
    verify(mockRpcMetrics, times(1)).timeOperation();
    verify(mockRpcMetrics, times(1)).timeRpc();
    verify(mockRetryableRpc, times(1)).newCall(eq(CallOptions.DEFAULT));

    ByteString key1 = ByteString.copyFrom("SomeKey1", "UTF-8");
    ByteString key2 = ByteString.copyFrom("SomeKey2", "UTF-8");
    underTest.onMessage(buildResponse(key1));
    underTest.onClose(Status.ABORTED, new Metadata());

    // This gets invoked by the scheduler.
    verify(mockRetryExecutorService, times(1)).schedule(underTest, any(Long.class), any(TimeUnit.class));
    underTest.run();

    underTest.onMessage(buildResponse(key2));
    verify(mockRetryableRpc, times(1)).start(
      same(mockClientCall),
      eq(READ_ROWS_REQUEST_ALL),
      same(underTest),
      any(Metadata.class));
    verify(mockResponseObserver, times(2)).onNext(any(ReadRowsResponse.class));

    underTest.onClose(Status.OK, metaData);

    Assert.assertEquals(key2,
      underTest.getRetryRequest().getRows().getRowRanges(0).getStartKeyOpen());

    verify(mockRpcTimerContext, times(2)).close();
    verify(mockOperationTimerContext, times(1)).close();

  }
  public ReadRowsResponse buildResponse(ByteString key) {
    return ReadRowsResponse.newBuilder()
        .addChunks(CellChunk.newBuilder().setRowKey(key).setCommitRow(true).build()).build();
  }

}
