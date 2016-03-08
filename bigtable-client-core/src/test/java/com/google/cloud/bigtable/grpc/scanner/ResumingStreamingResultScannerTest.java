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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for the {@link ResumingStreamingResultScanner}
 */
@RunWith(JUnit4.class)
public class ResumingStreamingResultScannerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Mock
  ResultScanner<Row> mockScanner;
  @Mock
  ResultScanner<Row> mockScannerPostResume;
  @Mock
  BigtableResultScannerFactory mockScannerFactory;
  @Mock
  Logger logger;

  private static final int MAX_SCAN_TIMEOUT_RETRIES = 3;

  RetryOptions retryOptions;
  ReadRowsRequest readRowsRequest = ReadRowsRequest.getDefaultInstance();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    retryOptions = new RetryOptions.Builder()
        .setEnableRetries(true)
        .setRetryOnDeadlineExceeded(true)
        .setInitialBackoffMillis(100)
        .setBackoffMultiplier(2D)
        .setMaxElapsedBackoffMillis(500)
        .build();
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
  public void testNextRowKey() {
    ByteString previous = ByteString.copyFromUtf8("row1");
    byte[] previousBytes = previous.toByteArray();
    byte[] expected = new byte[previousBytes.length + 1];
    System.arraycopy(previousBytes, 0, expected, 0, previousBytes.length);
    expected[previousBytes.length] = 0;

    ByteString next = ResumingStreamingResultScanner.nextRowKey(previous);
    assertArrayEquals(expected, next.toByteArray());
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
        new IOExceptionWithStatus("Test", new StatusRuntimeException(Status.ABORTED)), 10);
  }

  @Test
  public void testAbortedErrorsResumeWithTooManyRowsReturned() throws IOException {
    doErrorsResume(
        new IOExceptionWithStatus("Test", new StatusRuntimeException(Status.ABORTED)), 1);
  }

  private void doErrorsResume(Status status) throws IOException {
    doErrorsResume(new IOExceptionWithStatus("Test", new StatusRuntimeException(status)));
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

  private void doErrorsResume(IOException expectedIOException, long numRowsLimit, int numExceptions)
      throws IOException {
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");
    Row row3 = buildRow("row3");
    Row row4 = buildRow("row4");

    ReadRowsRequest.Builder originalRequest = readRowsRequest.toBuilder();
    if (numRowsLimit != 0) {
      originalRequest.setNumRowsLimit(numRowsLimit);
    }

    ReadRowsRequest.Builder expectedResumeRequest = originalRequest.build().toBuilder();
    expectedResumeRequest.getRowRangeBuilder()
        .setStartKey(ResumingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row2")));
    if (numRowsLimit > 2) {
      expectedResumeRequest.setNumRowsLimit(numRowsLimit - 2);
    }

    // If we are doing more than one failure then create a scanner for each.
    List<ResultScanner<Row>> additionalMockScanners = new ArrayList<>();
    for (int i = 1; i < numExceptions; i++) {
      ResultScanner<Row> mock = mock(ResultScanner.class);
      additionalMockScanners.add(mock);
    }

    when(mockScannerFactory.createScanner(eq(originalRequest.build()))).thenReturn(mockScanner);
    OngoingStubbing<ResultScanner<Row>> afterFailureStub =
        when(mockScannerFactory.createScanner(eq(expectedResumeRequest.build())));
    for (ResultScanner<Row> mock : additionalMockScanners) {
      afterFailureStub = afterFailureStub.thenReturn(mock);
    }
    afterFailureStub.thenReturn(mockScannerPostResume);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(
        retryOptions, originalRequest.build(), mockScannerFactory, logger);

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

    verify(mockScannerFactory, times(1)).createScanner(eq(originalRequest.build()));
    verify(mockScanner, times(1)).close();
    if (numRowsLimit != 1 && numRowsLimit != 2) {
      verify(mockScannerFactory, times(numExceptions)).createScanner(eq(expectedResumeRequest.build()));
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
    doErrorsDoNotResume(Status.FAILED_PRECONDITION);
  }

  @Test
  public void testDeadlineExceededErrorsDoNotResume_flagDisabled() throws IOException {
    retryOptions = new RetryOptions.Builder()
        .setEnableRetries(true)
        .setRetryOnDeadlineExceeded(false) // Disable retryOnDeadlineExceeded
        .setInitialBackoffMillis(100)
        .setBackoffMultiplier(2D)
        .setMaxElapsedBackoffMillis(500)
        .setMaxScanTimeoutRetries(MAX_SCAN_TIMEOUT_RETRIES)
        .build();
    doErrorsDoNotResume(Status.DEADLINE_EXCEEDED);
  }

  private void doErrorsDoNotResume(Status status) throws IOException {
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");

    ReadRowsRequest.Builder expectedResumeRequest = readRowsRequest.toBuilder();
    expectedResumeRequest.getRowRangeBuilder()
        .setStartKey(ResumingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row2")));

    when(mockScannerFactory.createScanner(eq(expectedResumeRequest.build())))
        .thenReturn(mockScannerPostResume);

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class)))
        .thenReturn(mockScanner);

    ResumingStreamingResultScanner scanner =
        new ResumingStreamingResultScanner(retryOptions, readRowsRequest, mockScannerFactory,
            logger);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(new IOExceptionWithStatus("Test", new StatusRuntimeException(status)));

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
        new IOExceptionWithStatus("Test", new StatusRuntimeException(Status.UNAVAILABLE));
    IOException timeoutException = new ScanTimeoutException("Test");

    ReadRowsRequest.Builder originalRequest = readRowsRequest.toBuilder();

   when(mockScannerFactory.createScanner(eq(originalRequest.build())))
        .thenReturn(mockScanner);
    ResultScanner<Row> afterError1Mock = mock(ResultScanner.class);
    ResultScanner<Row> afterTimeoutMock = mock(ResultScanner.class);
    ResultScanner<Row> afterError2Mock = mock(ResultScanner.class);

    ReadRowsRequest.Builder expectedResumeRequest = originalRequest.build().toBuilder();
    ByteString startKey = ResumingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row1"));
    expectedResumeRequest.getRowRangeBuilder().setStartKey(startKey);
    when(mockScannerFactory.createScanner(eq(expectedResumeRequest.build())))
        .thenReturn(afterError1Mock, afterTimeoutMock);

    expectedResumeRequest = originalRequest.build().toBuilder();
    startKey = ResumingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row2"));
    expectedResumeRequest.getRowRangeBuilder().setStartKey(startKey);
    when(mockScannerFactory.createScanner(eq(expectedResumeRequest.build())))
        .thenReturn(afterError2Mock, mockScannerPostResume);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(
        retryOptions, originalRequest.build(), mockScannerFactory, logger);

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

    verify(mockScannerFactory, times(1)).createScanner(eq(originalRequest.build()));
    verify(mockScanner, times(1)).close();
    verify(mockScannerFactory, times(2)).createScanner(eq(expectedResumeRequest.build()));
    scanner.close();
  }
}
