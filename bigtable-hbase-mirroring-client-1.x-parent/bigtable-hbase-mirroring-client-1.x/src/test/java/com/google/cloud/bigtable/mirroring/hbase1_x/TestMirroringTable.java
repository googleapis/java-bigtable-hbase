/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringTable {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock MismatchDetector mismatchDetector;

  ExecutorService executorService;
  MirroringTable mirroringTable;

  @Before
  public void setUp() {
    this.executorService = Executors.newSingleThreadExecutor();
    this.mirroringTable =
        new MirroringTable(primaryTable, secondaryTable, this.executorService, mismatchDetector);
  }

  private Result createResult(String key, String... values) {
    ArrayList<Cell> cells = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      cells.add(CellUtil.createCell(key.getBytes(), values[i].getBytes()));
    }
    return Result.create(cells);
  }

  private Get createGet(String key) {
    return new Get(key.getBytes());
  }

  private List<Get> createGets(String... keys) {
    List<Get> result = new ArrayList<>();
    for (String key : keys) {
      result.add(createGet(key));
    }
    return result;
  }

  private void waitForExecutor() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetSingle() throws IOException {
    Get get = createGets("test").get(0);
    Result expectedResult = createResult("test", "value");

    when(primaryTable.get(get)).thenReturn(expectedResult);
    when(secondaryTable.get(get)).thenReturn(expectedResult);

    Result result = mirroringTable.get(get);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never())
        .get(ArgumentMatchers.<Get>anyList(), any(Result[].class), any(Result[].class));
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnSingleGet()
      throws IOException {
    Get request = createGet("test");
    Result expectedResult = createResult("test", "value");

    when(primaryTable.get(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.get(request)).thenThrow(expectedException);

    Result result = mirroringTable.get(request);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetMultiple() throws IOException {
    List<Get> get = Arrays.asList(createGets("test").get(0));
    Result[] expectedResult = new Result[] {createResult("test", "value")};

    when(primaryTable.get(get)).thenReturn(expectedResult);
    when(secondaryTable.get(get)).thenReturn(expectedResult);

    Result[] result = mirroringTable.get(get);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never()).get((Get) any(), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnGetMultiple()
      throws IOException {
    List<Get> request = createGets("test1", "test2");
    Result[] expectedResult =
        new Result[] {createResult("test1", "value1"), createResult("test2", "value2")};

    when(primaryTable.get(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.get(request)).thenThrow(expectedException);

    Result[] result = mirroringTable.get(request);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnExists() throws IOException {
    Get get = createGet("test");
    boolean expectedResult = true;

    when(primaryTable.exists(get)).thenReturn(expectedResult);
    when(secondaryTable.exists(get)).thenReturn(expectedResult);

    boolean result = mirroringTable.exists(get);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).exists(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).exists((Get) any(), (Throwable) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExists() throws IOException {
    Get request = createGet("test");
    boolean expectedResult = true;

    when(primaryTable.exists(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.exists(request)).thenThrow(expectedException);

    boolean result = mirroringTable.exists(request);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).exists(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnExistsAll() throws IOException {
    List<Get> get = Arrays.asList(createGets("test").get(0));
    boolean[] expectedResult = new boolean[] {true, false};

    when(primaryTable.existsAll(get)).thenReturn(expectedResult);
    when(secondaryTable.existsAll(get)).thenReturn(expectedResult);

    boolean[] result = mirroringTable.existsAll(get);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).existsAll(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).existsAll(ArgumentMatchers.<Get>anyList(), (Throwable) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExistsAll()
      throws IOException {
    List<Get> request = createGets("test1", "test2");
    boolean[] expectedResult = new boolean[] {true, false};

    when(primaryTable.existsAll(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.existsAll(request)).thenThrow(expectedException);

    boolean[] result = mirroringTable.existsAll(request);
    waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).existsAll(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnScannerNextOne() throws IOException {
    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next()).thenReturn(expected1, expected2, null);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next()).thenReturn(expected1, expected2, null);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);
    Result result1 = mirroringScanner.next();
    Result result2 = mirroringScanner.next();
    Result result3 = mirroringScanner.next();
    mirroringScanner.close();

    assertThat(result1).isEqualTo(expected1);
    assertThat(result2).isEqualTo(expected2);
    assertThat(result3).isNull();

    waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, expected1, expected1);
    verify(mismatchDetector, times(1)).scannerNext(scan, 1, expected2, expected2);
    verify(mismatchDetector, times(1)).scannerNext(scan, 2, (Result) null, null);
    verify(mismatchDetector, times(3))
        .scannerNext(eq(scan), anyInt(), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnScannerNextOne()
      throws IOException {
    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next()).thenReturn(expected1, expected2, null);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    IOException expectedException = new IOException("expected");
    when(secondaryScannerMock.next())
        .thenReturn(expected1)
        .thenThrow(expectedException)
        .thenReturn(null);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);
    Result result1 = mirroringScanner.next();
    Result result2 = mirroringScanner.next();
    Result result3 = mirroringScanner.next();
    mirroringScanner.close();

    assertThat(result1).isEqualTo(expected1);
    assertThat(result2).isEqualTo(expected2);
    assertThat(result3).isNull();

    waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, expected1, expected1);
    verify(mismatchDetector, times(1)).scannerNext(scan, 1, expectedException);
    verify(mismatchDetector, times(1)).scannerNext(scan, 2, (Result) null, null);
    verify(mismatchDetector, times(2))
        .scannerNext(eq(scan), anyInt(), (Result) any(), (Result) any());
  }

  @Test
  public void testMismatchDetectorIsCalledOnScannerNextMultiple() throws IOException {
    Result[] expected =
        new Result[] {createResult("test1", "value1"), createResult("test2", "value2")};

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next(anyInt())).thenReturn(expected);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next(anyInt())).thenReturn(expected);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);
    Result[] result = mirroringScanner.next(2);
    mirroringScanner.close();

    assertThat(result).isEqualTo(expected);

    waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, expected, expected);
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnScannerNextMultiple()
      throws IOException {
    Result[] expected =
        new Result[] {createResult("test1", "value1"), createResult("test2", "value2")};

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next(anyInt())).thenReturn(expected);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    IOException expectedException = new IOException("expected");
    when(secondaryScannerMock.next(anyInt())).thenThrow(expectedException);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);
    Result[] result = mirroringScanner.next(2);
    mirroringScanner.close();

    assertThat(result).isEqualTo(expected);

    waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, 2, expectedException);
  }

  @Test
  public void testScannerClose() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);
    mirroringScanner.close();

    waitForExecutor();
    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerMock, times(1)).close();
  }

  @Test
  public void testScannerRenewLease() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.renewLease()).thenReturn(true);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.renewLease()).thenReturn(false);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);

    // primary scanner lease was renewed, so we waited for the second one, and it returned false.
    assertThat(mirroringScanner.renewLease()).isFalse();

    waitForExecutor();
    verify(secondaryScannerMock, times(1)).renewLease();
  }
}
