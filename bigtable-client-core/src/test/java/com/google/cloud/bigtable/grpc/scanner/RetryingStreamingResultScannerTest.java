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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.ScanTimeoutException;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RetryingStreamingResultScanner;
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;

/**
 * Test for the {@link RetryingStreamingResultScanner}
 */
@RunWith(JUnit4.class)
public class RetryingStreamingResultScannerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Mock
  ResultScanner<Row> mockScanner;
  @Mock
  ResultScanner<Row> mockScannerPostRetry;
  @Mock
  BigtableResultScannerFactory mockScannerFactory;

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
  public void testInternalErrorsRetry() throws IOException {
    doErrorsRetry(Status.INTERNAL);
  }

  @Test
  public void testUnavailableErrorsRetry() throws IOException {
    doErrorsRetry(Status.UNAVAILABLE);
  }

  @Test
  public void testDeadlineExceededErrorsRetry() throws IOException {
    doErrorsRetry(Status.DEADLINE_EXCEEDED);
  }

  @Test
  public void testAbortedErrorsRetry() throws IOException {
    doErrorsRetry(Status.ABORTED);
  }

  @Test
  public void testAbortedErrorsRetryWithRowLimit() throws IOException {
    doErrorsRetry(
        new IOExceptionWithStatus("Test", new StatusRuntimeException(Status.ABORTED)), 10);
  }

  @Test
  public void testAbortedErrorsRetryWithTooManyRowsReturned() throws IOException {
    doErrorsRetry(
        new IOExceptionWithStatus("Test", new StatusRuntimeException(Status.ABORTED)), 1);
  }

  private void doErrorsRetry(Status status) throws IOException {
    doErrorsRetry(new IOExceptionWithStatus("Test", new StatusRuntimeException(status)));
  }

  @Test
  public void testReadTimeoutRetry() throws IOException {
    doErrorsRetry(new ScanTimeoutException("ReadTimeoutTest"));
  }

  private void doErrorsRetry(IOException expectedIOException) throws IOException {
    doErrorsRetry(expectedIOException, 0);
  }

  private void doErrorsRetry(IOException expectedIOException, long numRowsLimit)
      throws IOException {
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");
    Row row3 = buildRow("row3");
    Row row4 = buildRow("row4");

    ReadRowsRequest.Builder originalRequest = readRowsRequest.toBuilder();
    if (numRowsLimit != 0) {
      originalRequest.setNumRowsLimit(numRowsLimit);
    }

    ReadRowsRequest.Builder expectedRetryRequest = originalRequest.build().toBuilder();
    expectedRetryRequest.getRowRangeBuilder()
        .setStartKey(RetryingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row2")));
    if (numRowsLimit > 2) {
      expectedRetryRequest.setNumRowsLimit(numRowsLimit - 2);
    }

    when(mockScannerFactory.createScanner(eq(originalRequest.build())))
        .thenReturn(mockScanner);
    when(mockScannerFactory.createScanner(eq(expectedRetryRequest.build())))
        .thenReturn(mockScannerPostRetry);

    RetryingStreamingResultScanner scanner = new RetryingStreamingResultScanner(
        retryOptions, originalRequest.build(), mockScannerFactory);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(expectedIOException)
        .thenThrow(
            new IOException(
                "Next invoked on scanner post-exception. This is most "
                    + "likely due to the mockClient not returning the "
                    + "post-Retry scanner properly"));

    when(mockScannerPostRetry.next())
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
      verify(mockScannerFactory, times(1)).createScanner(eq(expectedRetryRequest.build()));
    }
  }

  @Test
  public void testFailedPreconditionErrorsDoNotRetry() throws IOException {
    doErrorsDoNotRetry(Status.FAILED_PRECONDITION);
  }

  @Test
  public void testDeadlineExceededErrorsDoNotRetry_flagDisabled() throws IOException {
    retryOptions = new RetryOptions.Builder()
        .setEnableRetries(true)
        .setRetryOnDeadlineExceeded(false) // Disable retryOnDeadlineExceeded
        .setInitialBackoffMillis(100)
        .setBackoffMultiplier(2D)
        .setMaxElapsedBackoffMillis(500)
        .build();
    doErrorsDoNotRetry(Status.DEADLINE_EXCEEDED);
  }

  private void doErrorsDoNotRetry(Status status) throws IOException {
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");

    ReadRowsRequest.Builder expectedRetryRequest = readRowsRequest.toBuilder();
    expectedRetryRequest.getRowRangeBuilder()
        .setStartKey(RetryingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row2")));

    when(mockScannerFactory.createScanner(eq(expectedRetryRequest.build())))
        .thenReturn(mockScannerPostRetry);

    when(mockScannerFactory.createScanner(any(ReadRowsRequest.class)))
        .thenReturn(mockScanner);

    RetryingStreamingResultScanner scanner =
        new RetryingStreamingResultScanner(retryOptions, readRowsRequest, mockScannerFactory);

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
    verifyNoMoreInteractions(mockScannerPostRetry, mockScannerFactory);
  }
}
