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

import io.grpc.Status;
import io.grpc.Status.OperationRuntimeException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

  RetryOptions retryOptions = new RetryOptions(true, 100, 2, 500);
  ReadRowsRequest readRowsRequest = ReadRowsRequest.getDefaultInstance();

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
        .thenThrow(
            new IOExceptionWithStatus("Test", new OperationRuntimeException(Status.INTERNAL)))
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
    verify(mockScannerFactory, times(1)).createScanner(eq(expectedResumeRequest.build()));
  }

  @Test
  public void testNonInternalErrorsDoNotResume() throws IOException {
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
        .thenThrow(
            new IOExceptionWithStatus(
                "Test", new OperationRuntimeException(Status.FAILED_PRECONDITION)));

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
