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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

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

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.protobuf.ByteString;

/**
 * Test for the {@link ResumingStreamingResultScanner}
 */
@RunWith(JUnit4.class)
public class ResumingStreamingResultScannerTest {

  static final RetryOptions RETRY_OPTIONS = new RetryOptions.Builder()
      .setEnableRetries(true)
      .setRetryOnDeadlineExceeded(true)
      .setInitialBackoffMillis(100)
      .setBackoffMultiplier(2D)
      .setMaxElapsedBackoffMillis(500)
      .build();

  private static final ScanTimeoutException SCAN_TIMEOUT_EXCEPTION =
      new ScanTimeoutException("ReadTimeoutTest");

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock
  private Logger mockLogger;
  @Mock
  private ResponseQueueReader mockResponseQueueReader;
  @Mock
  private ScannerRetryListener mockScannerRetryListener;

  private static final int MAX_SCAN_TIMEOUT_RETRIES = 3;
  static ReadRowsRequest READ_ROWS_REQUEST_ALL =
      ReadRowsRequest.newBuilder().setRows(RowSet.newBuilder().addRowRanges(
        RowRange.getDefaultInstance()))
      .build();

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
  public void testReadTimeoutResume() throws IOException {
    doErrorsResume(1);
  }

  @Test
  public void testMultipleReadTimeoutResume() throws IOException {
    doErrorsResume(MAX_SCAN_TIMEOUT_RETRIES);
  }

  @Test
  public void testMultipleReadTimeoutResumeAndExhaust() throws IOException {
    thrown.expect(BigtableRetriesExhaustedException.class);
    doErrorsResume(MAX_SCAN_TIMEOUT_RETRIES + 1);
  }

  private void doErrorsResume(final int numExceptions)
      throws IOException {
    final FlatRow row1 = buildRow("row1");
    final FlatRow row2 = buildRow("row2");
    final FlatRow row3 = buildRow("row3");
    final FlatRow row4 = buildRow("row4");

    Answer<FlatRow> answer = new Answer<FlatRow>(){
      int count = 0;
      @Override
      public FlatRow answer(InvocationOnMock invocation) throws Throwable {
        count++;
        if (count == 1) {
          return row1;
        } else if (count == 2) {
          return row2;
        } else if (count < 3 + numExceptions) {
          throw SCAN_TIMEOUT_EXCEPTION;
        } else if (count == 3 + numExceptions) {
          return row3;
        } else if (count == 4 + numExceptions ) {
          return row4;
        } else {
          throw new IllegalStateException("There should not be " + count + " requests");
        }
      }
    };
    when(mockResponseQueueReader.getNextMergedRow()).then(answer);

    ResumingStreamingResultScanner scanner = new ResumingStreamingResultScanner(
        mockResponseQueueReader, RETRY_OPTIONS, READ_ROWS_REQUEST_ALL, mockScannerRetryListener, mockLogger);

    assertRowKey("row1", scanner.next());
    assertRowKey("row2", scanner.next());
    assertRowKey("row3", scanner.next());
    assertRowKey("row4", scanner.next());

    scanner.close();
    verify(mockScannerRetryListener, times(numExceptions)).run();
    verify(mockScannerRetryListener, times(numExceptions)).resetBackoff();
    verify(mockScannerRetryListener, times(1)).cancel();
  }
}
