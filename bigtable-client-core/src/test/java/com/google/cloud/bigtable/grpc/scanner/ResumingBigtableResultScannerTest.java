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
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowRange;
import com.google.cloud.bigtable.grpc.scanner.BigtableResultScannerFactory;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Test for the {@link ResumingBigtableResultScanner}.
 */
@RunWith(JUnit4.class)
public class ResumingBigtableResultScannerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Mock
  ResultScanner<Row> mockScanner;
  @Mock
  BigtableResultScannerFactory mockScannerFactory;

  ResumingBigtableResultScanner scanner;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    ReadRowsRequest request = ReadRowsRequest.getDefaultInstance();
    when(mockScannerFactory.createScanner(eq(request))).thenReturn(mockScanner);
    scanner = new ResumingBigtableResultScanner(request, mockScannerFactory);
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

    ByteString next = ResumingBigtableResultScanner.nextRowKey(previous);
    assertArrayEquals(expected, next.toByteArray());
  }

  @Test
  public void testClose() throws IOException {
    scanner.close();
    verify(mockScanner, times(1)).close();
  }
  
  @Test
  public void testNext_withException() throws IOException {
    assertNull(scanner.getLastRowKey());
    assertEquals(0, scanner.getRowCount());

    IOException expectedException = new IOException("dummy");
    when(mockScanner.next()).thenThrow(expectedException);
    thrown.expect(IOException.class);
    thrown.expectMessage("dummy");

    scanner.next();
    assertNull(scanner.getLastRowKey());
    assertEquals(0, scanner.getRowCount());
  }

  @Test
  public void testNext() throws IOException {
    assertNull(scanner.getLastRowKey());
    assertEquals(0, scanner.getRowCount());

    Row row1 = buildRow("row1");
    when(mockScanner.next()).thenReturn(row1);

    scanner.next();
    assertEquals(row1.getKey(), scanner.getLastRowKey());
    assertEquals(1, scanner.getRowCount());

    Row row2 = buildRow("row2");
    when(mockScanner.next()).thenReturn(row2);

    scanner.next();
    assertEquals(row2.getKey(), scanner.getLastRowKey());
    assertEquals(2, scanner.getRowCount());

    verify(mockScanner, times(2)).next();
  }

  @Test
  public void testResume_firstRow() throws IOException {
    assertResume(false, 0);
  }
  
  @Test
  public void testResume_firstRow_withNumRowsLimit() throws IOException {
    assertResume(false, 10);
  }

  @Test
  public void testResume_firstRow_skipLastRow() throws IOException {
    assertResume(true, 0);
  }

  @Test
  public void testResume_firstRow_skipLastRow_withNumRowsLimit() throws IOException {
    assertResume(true, 10);
  }

  @Test
  public void testResume_nextRow() throws IOException {
    Row row1 = buildRow("row1");
    when(mockScanner.next()).thenReturn(row1);
    scanner.next();

    assertResume(false, 0);
    verify(mockScanner, times(1)).next();
  }
  
  @Test
  public void testResume_nextRow_withNumRowsLimit() throws IOException {
    Row row1 = buildRow("row1");
    when(mockScanner.next()).thenReturn(row1);
    scanner.next();

    assertResume(false, 10);
    verify(mockScanner, times(1)).next();
  }

  @Test
  public void testResume_nextRow_skipLastRow() throws IOException {
    Row row1 = buildRow("row1");
    when(mockScanner.next()).thenReturn(row1);
    scanner.next();

    assertResume(true, 0);
    verify(mockScanner, times(1)).next();
  }

  @Test
  public void testResume_nextRow_skipLastRow_withNumRowsLimit() throws IOException {
    Row row1 = buildRow("row1");
    when(mockScanner.next()).thenReturn(row1);
    scanner.next();

    assertResume(true, 10);
    verify(mockScanner, times(1)).next();
  }

  private void assertResume(boolean skipLastRow, long numRowsLimit) throws IOException {
    ByteString rowKey = buildRow("row").getKey();
    ReadRowsRequest newRequest = ReadRowsRequest.newBuilder()
        .setRowRange(RowRange.newBuilder().setStartKey(rowKey))
        .setNumRowsLimit(numRowsLimit)
        .build();
    scanner.resume(newRequest.toBuilder(), skipLastRow);

    verify(mockScanner, times(1)).close();
 
    ByteString expectedRowKey = scanner.getLastRowKey();
    if (expectedRowKey == null) {
      expectedRowKey = rowKey;
    } else if (skipLastRow) {
      expectedRowKey = ResumingBigtableResultScanner.nextRowKey(expectedRowKey);
    }

    long exepctedNumRowsLimit = numRowsLimit;
    if (numRowsLimit > 0) {
      exepctedNumRowsLimit = numRowsLimit - scanner.getRowCount();
      if (!skipLastRow && scanner.getRowCount() > 0) {
        exepctedNumRowsLimit++;
      }
    }
    ReadRowsRequest expectedRequest =
        newRequest.toBuilder()
            .setRowRange(RowRange.newBuilder().setStartKey(expectedRowKey))
            .setNumRowsLimit(exepctedNumRowsLimit)
            .build();
    verify(mockScannerFactory, times(1)).createScanner(eq(expectedRequest));
  }
}
