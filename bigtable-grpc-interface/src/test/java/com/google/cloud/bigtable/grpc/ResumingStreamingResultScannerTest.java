/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import static org.junit.Assert.assertArrayEquals;
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
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.Status;
import io.grpc.Status.OperationRuntimeException;

import java.io.IOException;

/**
 * Test for the {@link ResumingStreamingResultScanner}
 */
@RunWith(JUnit4.class)
public class ResumingStreamingResultScannerTest {

  @Mock
  ResultScanner<Row> mockScanner;
  @Mock
  ResultScanner<Row> mockScannerPostResume;
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

  private void doErrorsResume(Status status) throws IOException {
    doErrorsResume(new IOExceptionWithStatus("Test", new OperationRuntimeException(status)));
  }

  @Test
  public void testReadTimeoutResume() throws IOException {
    doErrorsResume(new ReadTimeoutException("ReadTimeoutTest"));
  }

  private void doErrorsResume(IOException expectedIOException) throws IOException {
    Row row1 = buildRow("row1");
    Row row2 = buildRow("row2");
    Row row3 = buildRow("row3");
    Row row4 = buildRow("row4");

    ReadRowsRequest.Builder expectedResumeRequest = readRowsRequest.toBuilder();
    expectedResumeRequest.getRowRangeBuilder()
        .setStartKey(ResumingStreamingResultScanner.nextRowKey(ByteString.copyFromUtf8("row2")));

    when(mockScannerFactory.createScanner(eq(readRowsRequest)))
        .thenReturn(mockScanner);
    when(mockScannerFactory.createScanner(eq(expectedResumeRequest.build())))
        .thenReturn(mockScannerPostResume);

    ResumingStreamingResultScanner scanner =
        new ResumingStreamingResultScanner(retryOptions, readRowsRequest, mockScannerFactory);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(expectedIOException)
        .thenThrow(
            new IOException(
                "Next invoked on scanner post-exception. This is most "
                    + "likely due to the mockClient not returning the "
                    + "post-resume scanner properly"));

    when(mockScannerPostResume.next())
        .thenReturn(row3)
        .thenReturn(row4);

    assertRowKey("row1", scanner.next());
    assertRowKey("row2", scanner.next());
    assertRowKey("row3", scanner.next());
    assertRowKey("row4", scanner.next());

    verify(mockScannerFactory, times(1)).createScanner(eq(readRowsRequest));
    verify(mockScanner, times(1)).close();
    verify(mockScannerFactory, times(1)).createScanner(eq(expectedResumeRequest.build()));
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
        new ResumingStreamingResultScanner(retryOptions, readRowsRequest, mockScannerFactory);

    when(mockScanner.next())
        .thenReturn(row1)
        .thenReturn(row2)
        .thenThrow(new IOExceptionWithStatus("Test", new OperationRuntimeException(status)));

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
  }
}
