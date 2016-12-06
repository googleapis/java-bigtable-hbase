/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ScanTimeoutException;
import com.google.protobuf.ByteString;

import io.grpc.Status;

/**
 * Test for the {@link ResumingStreamingResultScanner}
 */
@RunWith(JUnit4.class)
public class ResumingStreamingResultScannerTest {

  static final RetryOptions retryOptions = new RetryOptions.Builder()
      .setEnableRetries(true)
      .setRetryOnDeadlineExceeded(true)
      .setInitialBackoffMillis(100)
      .setBackoffMultiplier(2D)
      .setMaxElapsedBackoffMillis(500)
      .build();
  static final ByteString blank = ByteString.copyFrom(new byte[0]);

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Mock
  ResultScanner<FlatRow> mockScanner;
  @Mock
  ResultScanner<FlatRow> mockScannerPostResume;
  @Mock
  BigtableResultScannerFactory<FlatRow> mockScannerFactory;
  @Mock
  Logger logger;
  @Mock
  ReadRowsRequestManager mockReadRowsRequestManager;

  static BigtableAsyncRpc.RpcMetrics metrics =
      BigtableAsyncRpc.RpcMetrics.createRpcMetrics(BigtableGrpc.METHOD_READ_ROWS);

  private static final int MAX_SCAN_TIMEOUT_RETRIES = 3;
  private static ReadRowsRequest readRowsRequest = createRequest(createRowRangeClosedStart(blank, blank));

  protected static ReadRowsRequest createRequest(RowRange range) {
    return ReadRowsRequest.newBuilder().setRows(RowSet.newBuilder().addRowRanges(range)).build();
  }

  protected static RowRange createRowRangeClosedStart(ByteString startClosed, ByteString endOpen) {
    return RowRange.newBuilder().setStartKeyClosed(startClosed).setEndKeyOpen(endOpen).build();
  }

  protected static RowRange createRowRangeOpenedStart(ByteString startOpened, ByteString endOpen) {
    return RowRange.newBuilder().setStartKeyOpen(startOpened).setEndKeyOpen(endOpen).build();
  }

  protected ReadRowsRequest createKeysRequest(Iterable<ByteString> keys) {
    return ReadRowsRequest.newBuilder().setRows(createRowSet(keys)).build();
  }

  protected RowSet createRowSet(Iterable<ByteString> keys) {
    return RowSet.newBuilder().addAllRowKeys(keys).build();
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  static FlatRow buildRow(String rowKey) {
    return FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8(rowKey))
        .build();
  }

  static void assertRowKey(String expectedRowKey, FlatRow row) {
    assertEquals(expectedRowKey, row.getRowKey().toStringUtf8());
  }

  @Test
  public void testInternalErrorsResume() throws IOException {
    doErrorsResume(Status.INTERNAL);
  }

  @Test
  public void testUnavailableErrorsResume() throws IOException {
    doErrorsResume(Status.UNAVAILABLE);
  }

  @Test
  public void testDeadlineExceededErrorsResume() throws IOException {
    doErrorsResume(Status.DEADLINE_EXCEEDED);
  }

  @Test
  public void testAbortedErrorsResume() throws IOException {
    doErrorsResume(Status.ABORTED);
  }

  @Test
  public void testAbortedErrorsResumeWithRowLimit() throws IOException {
    doErrorsResume(
        new IOExceptionWithStatus("Test", Status.ABORTED), 10);
  }

  @Test
  public void testAbortedErrorsResumeWithTooManyRowsReturned() throws IOException {
    doErrorsResume(
        new IOExceptionWithStatus("Test", Status.ABORTED), 1);
  }

  private void doErrorsResume(Status status) throws IOException {
    doErrorsResume(new IOExceptionWithStatus("Test", status));
  }

  @Test
  public void testReadTimeoutResume() throws IOException {
    doErrorsResume(new ScanTimeoutException("ReadTimeoutTest"));
  }

  @Test
  public void testMultipleReadTimeoutResume() throws IOException {
    doErrorsResume(new ScanTimeoutException("ReadTimeoutTest"), 0, MAX_SCAN_TIMEOUT_RETRIES);
  }

  @Test
  public void testMultipleReadTimeoutResumeAndExhaust() throws IOException {
    doErrorsResumeAndExhaust(
        new ScanTimeoutException("ReadTimeoutTest"), 0, MAX_SCAN_TIMEOUT_RETRIES + 1);
  }

  private void doErrorsResume(IOException expectedIOException) throws IOException {
    doErrorsResume(expectedIOException, 0);
  }

  private void doErrorsResume(IOException expectedIOException, long numRowsLimit)
      throws IOException {
    doErrorsResume(expectedIOException, numRowsLimit, 1);
  }

  @SuppressWarnings("unchecked")
  private void doErrorsResume(IOException expectedIOException, long numRowsLimit, int numExceptions)
      throws IOException {
    FlatRow row1 = buildRow("row1");
    FlatRow row2 = buildRow("row2");
    FlatRow row3 = buildRow("row3");
    FlatRow row4 = buildRow("row4");

    ReadRowsRequest originalRequest = readRowsRequest;
    if (numRowsLimit != 0) {
      originalRequest = originalRequest.toBuilder().setRowsLimit(numRowsLimit).build();
    }

    ReadRowsRequest expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row2"), blank));
    if (numRowsLimit > 2) {
      expectedResumeRequest =
          expectedResumeRequest.toBuilder().setRowsLimit(numRowsLimit - 2).build();
    }

    // If we are doing more than one failure then create a scanner for each.
    List<ResultScanner<FlatRow>> additionalMockScanners = new ArrayList<>();
    for (int i = 1; i < numExceptions; i++) {
      ResultScanner<FlatRow> mock = mock(ResultScanner.class);
      additionalMockScanners.add(mock);
    }

    when(mockScannerFactory.createScanner(eq(originalRequest))).thenReturn(mockScanner);
    OngoingStubbing<ResultScanner<FlatRow>> afterFailureStub =
        when(mockScannerFactory.createScanner(eq(expectedResumeRequest)));
    for (ResultScanner<FlatRow> mock : additionalMockScanners) {
      afterFailureStub = afterFailureStub.thenReturn(mock);
    }
    afterFailureStub.thenReturn(mockScannerPostResume);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(expectedIOException)
        .thenThrow(
            new IOException(
                "Next invoked on scanner post-exception. This is most "
                    + "likely due to the mockClient not returning the "
                    + "post-resume scanner properly"));

    for (ResultScanner<FlatRow> mock : additionalMockScanners) {
      when(mock.next())
          .thenThrow(expectedIOException)
          .thenThrow(
              new IOException(
                  "Next invoked on scanner post-exception. This is most "
                      + "likely due to the mockClient not returning the "
                      + "post-resume scanner properly"));
    }

    when(mockScannerPostResume.next())
        .thenReturn(row3)
        .thenReturn(row4);

    when(this.mockReadRowsRequestManager.getUpdatedRequest()).thenReturn(expectedResumeRequest);
    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        originalRequest, mockScannerFactory, metrics, mockReadRowsRequestManager, logger);

    assertRowKey("row1", scanner.next());
    assertRowKey("row2", scanner.next());
    // {@code numRowsLimit} with 1 or 2 give over the row limit error on rescan.
    if (numRowsLimit == 1 || numRowsLimit == 2) {
      thrown.expect(IllegalArgumentException.class);
    }
    assertRowKey("row3", scanner.next());
    assertRowKey("row4", scanner.next());

    verify(mockScannerFactory, times(1)).createScanner(eq(originalRequest));
    verify(mockScanner, times(1)).close();
    if (numRowsLimit != 1 && numRowsLimit != 2) {
      verify(mockScannerFactory, times(numExceptions)).createScanner(eq(expectedResumeRequest));
    }
    scanner.close();
  }

  private void doErrorsResumeAndExhaust(IOException expectedIOException, long numRowsLimit,
      int numExceptions) throws IOException {
    thrown.expect(BigtableRetriesExhaustedException.class);
    doErrorsResume(expectedIOException, numRowsLimit, numExceptions);
  }

  @Test
  public void testFailedPreconditionErrorsDoNotResume() throws IOException {
    doErrorsDoNotResume(retryOptions, Status.FAILED_PRECONDITION);
  }

  @Test
  public void testDeadlineExceededErrorsDoNotResume_flagDisabled() throws IOException {
    RetryOptions retryOptions = new RetryOptions.Builder()
        .setEnableRetries(true)
        .setRetryOnDeadlineExceeded(false) // Disable retryOnDeadlineExceeded
        .setInitialBackoffMillis(100)
        .setBackoffMultiplier(2D)
        .setMaxElapsedBackoffMillis(500)
        .setMaxScanTimeoutRetries(MAX_SCAN_TIMEOUT_RETRIES)
        .build();
    doErrorsDoNotResume(retryOptions, Status.DEADLINE_EXCEEDED);
  }

  private void doErrorsDoNotResume(RetryOptions retryOptions, Status status) throws IOException {
    FlatRow row1 = buildRow("row1");
    FlatRow row2 = buildRow("row2");

    ReadRowsRequest expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row2"), blank));

    when(mockScannerFactory.createScanner(eq(expectedResumeRequest)))
        .thenReturn(mockScannerPostResume);

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class)))
        .thenReturn(mockScanner);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        readRowsRequest, mockScannerFactory, metrics, mockReadRowsRequestManager, logger);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(new IOExceptionWithStatus("Test", status));

    assertRowKey("row1", scanner.next());
    assertRowKey("row2", scanner.next());

    try {
      scanner.next();
      fail("Scanner should have thrown when encountering a non-INTERNAL grpc error");
    } catch (IOException ioe) {
      // Expected and ignored.
    }

    verify(mockScannerFactory, times(1)).createScanner(eq(readRowsRequest));
    verifyNoMoreInteractions(mockScannerPostResume, mockScannerFactory);
    scanner.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  /**
   * Test successfully retrying while receiving a mixture of timeouts and retryable exceptions
   */
  public void testTimeoutsMixedWithErrors() throws IOException {
    FlatRow row1 = buildRow("row1");
    FlatRow row2 = buildRow("row2");
    FlatRow row3 = buildRow("row3");
    FlatRow row4 = buildRow("row4");

    IOException retryableException =
        new IOExceptionWithStatus("Test", Status.UNAVAILABLE);
    IOException timeoutException = new ScanTimeoutException("Test");

    when(mockScannerFactory.createScanner(eq(readRowsRequest))).thenReturn(mockScanner);
    ResultScanner<FlatRow> afterError1Mock = mock(ResultScanner.class);
    ResultScanner<FlatRow> afterTimeoutMock = mock(ResultScanner.class);
    ResultScanner<FlatRow> afterError2Mock = mock(ResultScanner.class);

    ReadRowsRequest expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row1"), blank));

    when(mockScannerFactory.createScanner(eq(expectedResumeRequest)))
        .thenReturn(afterError1Mock, afterTimeoutMock);

    expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row2"), blank));
    when(mockScannerFactory.createScanner(eq(expectedResumeRequest))).thenReturn(afterError2Mock,
      mockScannerPostResume);

    ResumingStreamingResultScanner scanner = createScanner();

    when(mockScanner.next())
        .thenReturn(row1)
        .thenThrow(retryableException)
        .thenThrow(
            new IOException(
                "Next invoked on scanner post-exception. This is most "
                    + "likely due to the mockClient not returning the "
                    + "post-resume scanner properly"));

    when(afterError1Mock.next())
        .thenThrow(timeoutException);
    when(afterTimeoutMock.next())
        .thenReturn(row2)
        .thenThrow(retryableException);
    when(afterError2Mock.next())
        .thenThrow(timeoutException);
    when(mockScannerPostResume.next())
        .thenReturn(row3)
        .thenReturn(row4);
    when(mockReadRowsRequestManager.getUpdatedRequest())
        .thenReturn(expectedResumeRequest)
        .thenReturn(expectedResumeRequest);

    assertRowKey("row1", scanner.next());
    assertRowKey("row2", scanner.next());
    assertRowKey("row3", scanner.next());
    assertRowKey("row4", scanner.next());

    verify(mockScannerFactory, times(1)).createScanner(eq(readRowsRequest));
    verify(mockScanner, times(1)).close();
    verify(mockScannerFactory, times(2)).createScanner(eq(expectedResumeRequest));
    scanner.close();
  }

  private ResumingStreamingResultScanner createScanner() {
    return new ResumingStreamingResultScanner(retryOptions, readRowsRequest, mockScannerFactory,
        metrics, mockReadRowsRequestManager, logger);
  }

}
