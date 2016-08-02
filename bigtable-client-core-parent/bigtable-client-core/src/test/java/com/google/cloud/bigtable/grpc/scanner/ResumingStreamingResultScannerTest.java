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
import java.util.Arrays;
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
import com.google.bigtable.v2.Row;
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
  ResultScanner<Row> mockScanner;
  @Mock
  ResultScanner<Row> mockScannerPostResume;
  @Mock
  BigtableResultScannerFactory<ReadRowsRequest, Row> mockScannerFactory;
  @Mock
  Logger logger;

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

  static Row buildRow(String rowKey) {
    return Row.newBuilder()
        .setKey(ByteString.copyFromUtf8(rowKey))
        .build();
  }

  static void assertRowKey(String expectedRowKey, Row row) {
    assertEquals(expectedRowKey, row.getKey().toStringUtf8());
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
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");
    Row row3 = buildRow("row3");
    Row row4 = buildRow("row4");

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
    List<ResultScanner<Row>> additionalMockScanners = new ArrayList<>();
    for (int i = 1; i < numExceptions; i++) {
      ResultScanner<Row> mock = mock(ResultScanner.class);
      additionalMockScanners.add(mock);
    }

    when(mockScannerFactory.createScanner(eq(originalRequest))).thenReturn(mockScanner);
    OngoingStubbing<ResultScanner<Row>> afterFailureStub =
        when(mockScannerFactory.createScanner(eq(expectedResumeRequest)));
    for (ResultScanner<Row> mock : additionalMockScanners) {
      afterFailureStub = afterFailureStub.thenReturn(mock);
    }
    afterFailureStub.thenReturn(mockScannerPostResume);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        originalRequest, mockScannerFactory, metrics, logger);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(expectedIOException)
        .thenThrow(
            new IOException(
                "Next invoked on scanner post-exception. This is most "
                    + "likely due to the mockClient not returning the "
                    + "post-resume scanner properly"));

    for (ResultScanner<Row> mock : additionalMockScanners) {
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
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");

    ReadRowsRequest expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row2"), blank));

    when(mockScannerFactory.createScanner(eq(expectedResumeRequest)))
        .thenReturn(mockScannerPostResume);

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class)))
        .thenReturn(mockScanner);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        readRowsRequest, mockScannerFactory, metrics, logger);

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
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");
    Row row3 = buildRow("row3");
    Row row4 = buildRow("row4");

    IOException retryableException =
        new IOExceptionWithStatus("Test", Status.UNAVAILABLE);
    IOException timeoutException = new ScanTimeoutException("Test");

    when(mockScannerFactory.createScanner(eq(readRowsRequest))).thenReturn(mockScanner);
    ResultScanner<Row> afterError1Mock = mock(ResultScanner.class);
    ResultScanner<Row> afterTimeoutMock = mock(ResultScanner.class);
    ResultScanner<Row> afterError2Mock = mock(ResultScanner.class);

    ReadRowsRequest expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row1"), blank));

    when(mockScannerFactory.createScanner(eq(expectedResumeRequest)))
        .thenReturn(afterError1Mock, afterTimeoutMock);

    expectedResumeRequest =
        createRequest(createRowRangeOpenedStart(ByteString.copyFromUtf8("row2"), blank));
    when(mockScannerFactory.createScanner(eq(expectedResumeRequest))).thenReturn(afterError2Mock,
      mockScannerPostResume);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        readRowsRequest, mockScannerFactory, metrics, logger);

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

    assertRowKey("row1", scanner.next());
    assertRowKey("row2", scanner.next());
    assertRowKey("row3", scanner.next());
    assertRowKey("row4", scanner.next());

    verify(mockScannerFactory, times(1)).createScanner(eq(readRowsRequest));
    verify(mockScanner, times(1)).close();
    verify(mockScannerFactory, times(2)).createScanner(eq(expectedResumeRequest));
    scanner.close();
  }

  /**
   * Test a single, full table scan scenario for {@Link ResumingStreamingResultScanner#filterRows()}
   * .
   * @throws IOException
   */
  @Test
  public void test_filterRows_testAllRange() throws IOException{
    ByteString key1 = ByteString.copyFrom("row1".getBytes());

    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class))).thenReturn(mockScanner);

    try (ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        readRowsRequest, mockScannerFactory, metrics, logger)) {

      verify(mockScannerFactory, times(1))
          .createScanner(eq(readRowsRequest));

      when(mockScanner.next())
          .thenReturn(row1)
          .thenThrow(new IOExceptionWithStatus("Test", Status.INTERNAL))
          .thenReturn(row2);

      scanner.next();
      scanner.next();

      verify(mockScannerFactory, times(1)).createScanner(
          eq(createRequest(createRowRangeOpenedStart(key1, blank))));
    }
  }


  /**
   * Test rowKeys scenario for {@Link ResumingStreamingResultScanner#filterRows()}.
   * @throws IOException
   */
  @Test
  public void test_filterRows_rowKeys() throws IOException{
    ByteString key1 = ByteString.copyFrom("row1".getBytes());
    ByteString key2 = ByteString.copyFrom("row2".getBytes());
    ByteString key3 = ByteString.copyFrom("row3".getBytes());

    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class))).thenReturn(mockScanner);

    ReadRowsRequest originalRequest = createKeysRequest(Arrays.asList(key1, key2, key3));
    try (ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        originalRequest, mockScannerFactory, metrics, logger)) {

      when(mockScanner.next())
          .thenThrow(new IOExceptionWithStatus("Test", Status.INTERNAL))
          .thenReturn(row1)
          .thenThrow(new IOExceptionWithStatus("Test", Status.INTERNAL))
          .thenReturn(row2);

      verify(mockScannerFactory, times(1)).createScanner(eq(originalRequest));
      scanner.next();
      verify(mockScannerFactory, times(2)).createScanner(eq(originalRequest));

      scanner.next();

      verify(mockScannerFactory, times(1))
          .createScanner(eq(createKeysRequest(Arrays.asList(key2, key3))));
    }
  }

  /**
   * Test multiple rowset filter scenarios for {@Link ResumingStreamingResultScanner#filterRows()}.
   * @throws IOException
   */
  @Test
  public void test_filterRows_multiRowSetFilters() throws IOException{
    ByteString key1 = ByteString.copyFrom("row1".getBytes());
    ByteString key2 = ByteString.copyFrom("row2".getBytes());
    ByteString key3 = ByteString.copyFrom("row3".getBytes());

    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class))).thenReturn(mockScanner);

    RowSet fullRowSet = RowSet.newBuilder()
        .addAllRowKeys(Arrays.asList(key1, key2, key3)) // row1 should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(blank).setEndKeyClosed(key1)) // should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(blank).setEndKeyOpen(key1)) // should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyOpen(key2)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key1).setEndKeyOpen(key2)) // should be converted (key1 -> key2)
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key1).setEndKeyClosed(key2)) // should be converted (key1 -> key2]
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key2).setEndKeyOpen(key3)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key2).setEndKeyOpen(key3)) // should stay
        .build();

    RowSet filteredRowSet = RowSet.newBuilder()
        .addAllRowKeys(Arrays.asList(key2, key3)) // row1 should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyOpen(key2)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyOpen(key2)) // should be converted (key1 -> key2)
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyClosed(key2)) // should be converted (key1 -> key2]
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key2).setEndKeyOpen(key3)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key2).setEndKeyOpen(key3)) // should stay
        .build();

    ReadRowsRequest originalRequest = ReadRowsRequest.newBuilder().setRows(fullRowSet).build();
    ReadRowsRequest filteredRequest = ReadRowsRequest.newBuilder().setRows(filteredRowSet).build();
    try (ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(retryOptions,
        originalRequest, mockScannerFactory, metrics, logger)) {

      when(mockScanner.next())
          .thenReturn(row1)
          .thenThrow(new IOExceptionWithStatus("Test", Status.INTERNAL))
          .thenReturn(row2);

      scanner.next();
      verify(mockScannerFactory, times(1)).createScanner(eq(originalRequest));
      scanner.next();
      verify(mockScannerFactory, times(1)).createScanner(eq(filteredRequest));
    }
  }
}
