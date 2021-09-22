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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestMirroringTable {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;

  MirroringTable mirroringTable;

  @Before
  public void setUp() {
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer));
  }

  private void mockFlowController() {
    ResourceReservation resourceReservationMock = mock(ResourceReservation.class);

    SettableFuture<ResourceReservation> resourceReservationFuture = SettableFuture.create();
    resourceReservationFuture.set(resourceReservationMock);

    doReturn(resourceReservationFuture)
        .when(flowController)
        .asyncRequestResource(any(RequestResourcesDescription.class));
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

  private void waitForMirroringScanner(ResultScanner mirroringScanner)
      throws InterruptedException, ExecutionException, TimeoutException {
    ((MirroringResultScanner) mirroringScanner).asyncClose().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetSingle() throws IOException {
    mockFlowController();
    Get get = createGets("test").get(0);
    Result expectedResult = createResult("test", "value");

    when(primaryTable.get(get)).thenReturn(expectedResult);
    when(secondaryTable.get(get)).thenReturn(expectedResult);

    Result result = mirroringTable.get(get);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never())
        .get(ArgumentMatchers.<Get>anyList(), any(Result[].class), any(Result[].class));
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnSingleGet()
      throws IOException {
    mockFlowController();
    Get request = createGet("test");
    Result expectedResult = createResult("test", "value");

    when(primaryTable.get(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.get(request)).thenThrow(expectedException);

    Result result = mirroringTable.get(request);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetMultiple() throws IOException {
    mockFlowController();
    List<Get> get = Arrays.asList(createGets("test").get(0));
    Result[] expectedResult = new Result[] {createResult("test", "value")};

    when(primaryTable.get(get)).thenReturn(expectedResult);
    when(secondaryTable.get(get)).thenReturn(expectedResult);

    Result[] result = mirroringTable.get(get);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never()).get((Get) any(), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnGetMultiple()
      throws IOException {
    mockFlowController();
    List<Get> request = createGets("test1", "test2");
    Result[] expectedResult =
        new Result[] {createResult("test1", "value1"), createResult("test2", "value2")};

    when(primaryTable.get(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.get(request)).thenThrow(expectedException);

    Result[] result = mirroringTable.get(request);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnExists() throws IOException {
    mockFlowController();
    Get get = createGet("test");
    boolean expectedResult = true;

    when(primaryTable.exists(get)).thenReturn(expectedResult);
    when(secondaryTable.exists(get)).thenReturn(expectedResult);

    boolean result = mirroringTable.exists(get);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).exists(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).exists((Get) any(), (Throwable) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExists() throws IOException {
    mockFlowController();
    Get request = createGet("test");
    boolean expectedResult = true;

    when(primaryTable.exists(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.exists(request)).thenThrow(expectedException);

    boolean result = mirroringTable.exists(request);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).exists(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnExistsAll() throws IOException {
    mockFlowController();
    List<Get> get = Arrays.asList(createGets("test").get(0));
    boolean[] expectedResult = new boolean[] {true, false};

    when(primaryTable.existsAll(get)).thenReturn(expectedResult);
    when(secondaryTable.existsAll(get)).thenReturn(expectedResult);

    boolean[] result = mirroringTable.existsAll(get);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).existsAll(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).existsAll(ArgumentMatchers.<Get>anyList(), (Throwable) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExistsAll()
      throws IOException {
    mockFlowController();
    List<Get> request = createGets("test1", "test2");
    boolean[] expectedResult = new boolean[] {true, false};

    when(primaryTable.existsAll(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.existsAll(request)).thenThrow(expectedException);

    boolean[] result = mirroringTable.existsAll(request);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).existsAll(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnScannerNextOne()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    mockFlowController();
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

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, expected1, expected1);
    verify(mismatchDetector, times(1)).scannerNext(scan, 1, expected2, expected2);
    verify(mismatchDetector, times(1)).scannerNext(scan, 2, (Result) null, null);
    verify(mismatchDetector, times(3))
        .scannerNext(eq(scan), anyInt(), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnScannerNextOne()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    mockFlowController();
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

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, expected1, expected1);
    verify(mismatchDetector, times(1)).scannerNext(scan, 1, expectedException);
    verify(mismatchDetector, times(1)).scannerNext(scan, 2, (Result) null, null);
    verify(mismatchDetector, times(2))
        .scannerNext(eq(scan), anyInt(), (Result) any(), (Result) any());
  }

  @Test
  public void testMismatchDetectorIsCalledOnScannerNextMultiple()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    mockFlowController();
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

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, expected, expected);
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnScannerNextMultiple()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    mockFlowController();
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

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();

    verify(mismatchDetector, times(1)).scannerNext(scan, 0, 2, expectedException);
  }

  @Test
  public void testScannerClose()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);
    mirroringScanner.close();

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();
    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerMock, times(1)).close();
  }

  @Test
  public void testScannerRenewLease()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
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

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();
    verify(secondaryScannerMock, times(1)).renewLease();
  }

  @Test
  public void testClosingTableWithFutureDecreasesListenableCounter()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    listenableReferenceCounter.holdReferenceUntilClosing(mirroringTable);

    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();
    final ListenableFuture<Void> closingFuture = mirroringTable.asyncClose();
    closingFuture.get(3, TimeUnit.SECONDS);
    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }

  @Test
  public void testClosingTableWithoutFutureDecreasesListenableCounter() throws IOException {
    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    listenableReferenceCounter.holdReferenceUntilClosing(mirroringTable);

    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();

    IOException expectedException = new IOException("expected");
    doThrow(expectedException).when(secondaryTable).close();

    mirroringTable.close();
    executorServiceRule.waitForExecutor();

    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }

  @Test
  public void testSecondaryIsClosedWhenPrimaryThrowsAnException() throws IOException {
    doThrow(new IOException("expected")).when(primaryTable).close();

    IOException exception =
        assertThrows(
            IOException.class,
            new ThrowingRunnable() {
              @Override
              public void run() throws Throwable {
                mirroringTable.close();
              }
            });
    assertThat(exception).hasMessageThat().contains("expected");
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(1)).close();
  }

  @Test
  public void testListenersAreCalledOnClose()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    final SettableFuture<Integer> listenerFuture1 = SettableFuture.create();
    mirroringTable.addOnCloseListener(
        new Runnable() {
          @Override
          public void run() {
            listenerFuture1.set(1);
          }
        });

    final SettableFuture<Integer> listenerFuture2 = SettableFuture.create();
    mirroringTable.addOnCloseListener(
        new Runnable() {
          @Override
          public void run() {
            listenerFuture2.set(2);
          }
        });

    mirroringTable.asyncClose().get(3, TimeUnit.SECONDS);
    assertThat(listenerFuture1.get(3, TimeUnit.SECONDS)).isEqualTo(1);
    assertThat(listenerFuture2.get(3, TimeUnit.SECONDS)).isEqualTo(2);
  }

  @Test
  public void testListenersAreNotCalledAfterSecondClose()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    final SettableFuture<Integer> listenerFuture1 = SettableFuture.create();

    Runnable onCloseAction =
        spy(
            new Runnable() {
              @Override
              public void run() {
                listenerFuture1.set(1);
              }
            });

    mirroringTable.addOnCloseListener(onCloseAction);

    mirroringTable.asyncClose().get(3, TimeUnit.SECONDS);
    assertThat(listenerFuture1.get(3, TimeUnit.SECONDS)).isEqualTo(1);
    mirroringTable.asyncClose().get(3, TimeUnit.SECONDS);

    verify(onCloseAction, times(1)).run();
  }

  private Put createPut(String row, String family, String qualifier, String value) {
    Put put = new Put(row.getBytes());
    put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
    return put;
  }

  @Test
  public void testPutIsMirrored() throws IOException, InterruptedException {
    mockFlowController();
    Put put = createPut("test", "f1", "q1", "v1");
    List<Put> puts = Arrays.asList(put);

    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                // secondary
                result[0] = Result.create(new Cell[0]);
                return null;
              }
            })
        .when(primaryTable)
        .batch(eq(puts), any(Object[].class));

    mirroringTable.put(put);
    mirroringTable.put(puts);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(1)).put(put);

    // put(List<Put>) is mirrored using batch, we because we have to detect partially applied
    // writes.
    verify(primaryTable, times(1)).batch(eq(puts), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(puts), any(Object[].class));
  }

  @Test
  public void testPutWithErrorIsNotMirrored() throws IOException {
    final Put put = createPut("test", "f1", "q1", "v1");
    doThrow(new IOException("test exception")).when(primaryTable).put(put);

    assertThrows(
        IOException.class,
        new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            mirroringTable.put(put);
          }
        });
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(0)).put(put);
  }

  @Test
  public void testPutWithSecondaryErrorCallsErrorHandler() throws IOException {
    mockFlowController();
    Put put = createPut("test", "f1", "q1", "v1");
    doThrow(new IOException("test exception")).when(secondaryTable).put(put);
    doNothing().when(primaryTable).put(put);

    mirroringTable.put(put);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(1)).put(put);

    ArgumentCaptor<List<Row>> argument = ArgumentCaptor.forClass(List.class);
    verify(secondaryWriteErrorConsumer, times(1)).consume(argument.capture());
    assertThat(argument.getValue().size()).isEqualTo(1);
    assertThat(argument.getValue().get(0)).isEqualTo(put);
  }

  @Test
  public void testBatchGetAndPutGetsAreVerifiedOnSuccess()
      throws IOException, InterruptedException {
    mockFlowController();
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Get get1 = createGet("get1");

    List<Row> requests = Arrays.asList(new Row[] {put1, get1});

    // op   | p    | s
    // put1 | ok   | ok
    // get1 | ok   | ok

    final Result get1Result = createResult("get1", "value1");
    final Result get3Result = createResult("get3", "value3");

    // primary
    Object[] results = new Object[2];
    results[0] = Result.create(new Cell[0]);
    results[1] = get1Result;

    List<Row> secondaryRequests = Arrays.asList(new Row[] {put1, get1});

    doNothing().when(primaryTable).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                // secondary
                result[0] = Result.create(new Cell[0]);
                result[1] = get1Result;
                return null;
              }
            })
        .when(secondaryTable)
        .batch(eq(secondaryRequests), any(Object[].class));

    mirroringTable.batch(requests, results);
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(primaryTable, times(1)).batch(requests, results);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests), argument.capture());
    assertThat(argument.getValue().length).isEqualTo(2);

    // successful secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get1), new Result[] {get1Result}, new Result[] {get1Result});
  }

  @Test
  public void testBatchGetAndPut() throws IOException, InterruptedException {
    mockFlowController();
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Put put2 = createPut("test2", "f2", "q2", "v2");
    Put put3 = createPut("test3", "f3", "q3", "v3");
    Get get1 = createGet("get1");
    Get get2 = createGet("get2");
    Get get3 = createGet("get3");

    List<Row> requests = Arrays.asList(new Row[] {put1, put2, put3, get1, get2, get3});

    // op   | p    | s
    // put1 | ok   | fail
    // put2 | fail | x
    // put3 | ok   | ok

    // get1 | ok   | fail
    // get2 | fail | x
    // get3 | ok   | ok

    final Result get1Result = createResult("get1", "value1");
    final Result get3Result = createResult("get3", "value3");

    // primary
    Object[] results = new Object[6];
    results[0] = Result.create(new Cell[0]); // put1 - ok
    results[1] = null; // put2 - failed
    results[2] = Result.create(new Cell[0]); // put3 - ok
    results[3] = get1Result; // get1 - ok
    results[4] = null; // get2 - fail
    results[5] = get3Result; // get3 - ok

    List<Row> secondaryRequests = Arrays.asList(new Row[] {put1, put3, get1, get3});

    doThrow(new IOException("test1"))
        .when(primaryTable)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                // secondary
                result[0] = null; // put1 failed on secondary
                result[1] = Result.create(new Cell[0]); // put3 ok on secondary
                result[2] = null; // get1 - failed on secondary
                result[3] = get3Result; // get3 - ok
                throw new IOException("test2");
              }
            })
        .when(secondaryTable)
        .batch(eq(secondaryRequests), any(Object[].class));

    try {
      mirroringTable.batch(requests, results);
      fail("should have thrown");
    } catch (IOException e) {
      assertThat(e).hasMessageThat().contains("test1");
    }
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(primaryTable, times(1)).batch(requests, results);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests), argument.capture());
    assertThat(argument.getValue().length).isEqualTo(4);

    // failed secondary writes were reported
    verify(secondaryWriteErrorConsumer, times(1)).consume(Arrays.asList(put1));

    // successful secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get3), new Result[] {get3Result}, new Result[] {get3Result});

    // successful secondary reads were reported
    verify(mismatchDetector, times(1)).batch(eq(Arrays.asList(get1)), any(IOException.class));
  }

  @Test
  public void testBatchGetsPrimaryFailsSecondaryOk() throws IOException, InterruptedException {
    mockFlowController();
    Get get1 = createGet("get1");
    Get get2 = createGet("get2");

    List<Row> requests = Arrays.asList(new Row[] {get1, get2});

    // op   | p    | s
    // get1 | fail | x
    // get2 | ok   | ok

    final Result get2Result = createResult("get2", "value2");

    // primary
    Object[] results = new Object[6];
    results[0] = null;
    results[1] = get2Result;

    List<Row> secondaryRequests = Arrays.asList(new Row[] {get2});

    doThrow(new IOException("test1"))
        .when(primaryTable)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                // secondary
                result[0] = get2Result;
                return null;
              }
            })
        .when(secondaryTable)
        .batch(eq(secondaryRequests), any(Object[].class));

    try {
      mirroringTable.batch(requests, results);
      fail("should have thrown");
    } catch (IOException e) {
      assertThat(e).hasMessageThat().contains("test1");
    }
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(primaryTable, times(1)).batch(requests, results);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests), argument.capture());
    assertThat(argument.getValue().length).isEqualTo(1);

    // successful secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get2), new Result[] {get2Result}, new Result[] {get2Result});

    // no read errors reported
    verify(mismatchDetector, never())
        .batch(ArgumentMatchers.<Get>anyList(), any(IOException.class));
  }

  @Test
  public void testConditionalWriteHappensWhenConditionIsMet() throws IOException {
    mockFlowController();
    Put put = new Put("r1".getBytes());
    when(primaryTable.checkAndMutate(
            any(byte[].class),
            any(byte[].class),
            any(byte[].class),
            eq(CompareOp.EQUAL),
            any(byte[].class),
            any(RowMutations.class)))
        .thenReturn(true);

    mirroringTable.checkAndPut(
        "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), "v1".getBytes(), put);
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<RowMutations> argument = ArgumentCaptor.forClass(RowMutations.class);
    verify(secondaryTable, times(1)).mutateRow(argument.capture());
    assertThat(argument.getValue().getRow()).isEqualTo("r1".getBytes());
    assertThat(argument.getValue().getMutations().get(0)).isEqualTo(put);
  }

  @Test
  public void testConditionalWriteDoesntHappenWhenConditionIsNotMet() throws IOException {
    Put put = new Put("r1".getBytes());
    when(primaryTable.checkAndMutate(
            any(byte[].class),
            any(byte[].class),
            any(byte[].class),
            eq(CompareOp.EQUAL),
            any(byte[].class),
            any(RowMutations.class)))
        .thenReturn(false);

    mirroringTable.checkAndPut(
        "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), "v1".getBytes(), put);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, never()).mutateRow(any(RowMutations.class));
  }

  @Test
  public void testCheckAndPut() throws IOException {
    mockFlowController();
    Put put = new Put("r1".getBytes());
    when(primaryTable.checkAndMutate(
            any(byte[].class),
            any(byte[].class),
            any(byte[].class),
            any(CompareOp.class),
            any(byte[].class),
            any(RowMutations.class)))
        .thenReturn(true);

    mirroringTable.checkAndPut(
        "r1".getBytes(),
        "f1".getBytes(),
        "q1".getBytes(),
        CompareOp.GREATER_OR_EQUAL,
        "v1".getBytes(),
        put);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(1)).mutateRow(any(RowMutations.class));
  }

  @Test
  public void testCheckAndDelete() throws IOException {
    mockFlowController();
    Delete delete = new Delete("r1".getBytes());
    when(primaryTable.checkAndMutate(
            any(byte[].class),
            any(byte[].class),
            any(byte[].class),
            any(CompareOp.class),
            any(byte[].class),
            any(RowMutations.class)))
        .thenReturn(true);

    mirroringTable.checkAndDelete(
        "r1".getBytes(),
        "f1".getBytes(),
        "q1".getBytes(),
        CompareOp.GREATER_OR_EQUAL,
        "v1".getBytes(),
        delete);

    mirroringTable.checkAndDelete(
        "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), "v1".getBytes(), delete);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(2)).mutateRow(any(RowMutations.class));
  }

  @Test
  public void testCheckAndMutate() throws IOException {
    mockFlowController();
    RowMutations mutations = new RowMutations("r1".getBytes());
    when(primaryTable.checkAndMutate(
            any(byte[].class),
            any(byte[].class),
            any(byte[].class),
            any(CompareOp.class),
            any(byte[].class),
            any(RowMutations.class)))
        .thenReturn(true);

    mirroringTable.checkAndMutate(
        "r1".getBytes(),
        "f1".getBytes(),
        "q1".getBytes(),
        CompareOp.GREATER_OR_EQUAL,
        "v1".getBytes(),
        mutations);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(1)).mutateRow(any(RowMutations.class));
  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    mockFlowController();
    Delete delete = new Delete("r1".getBytes());
    mirroringTable.delete(delete);

    List<Delete> deletes = new ArrayList<>();
    deletes.add(delete);

    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                // secondary
                result[0] = Result.create(new Cell[0]);
                return null;
              }
            })
        .when(primaryTable)
        .batch(eq(deletes), any(Object[].class));

    mirroringTable.delete(deletes);
    executorServiceRule.waitForExecutor();
    verify(secondaryTable, times(1)).delete(delete);
    verify(secondaryTable, times(1)).batch(eq(Arrays.asList(delete)), any(Object[].class));
  }

  @Test
  public void testMutateRow() throws IOException {
    mockFlowController();
    RowMutations mutations = new RowMutations("r1".getBytes());
    mirroringTable.mutateRow(mutations);
    executorServiceRule.waitForExecutor();
    verify(secondaryTable, times(1)).mutateRow(mutations);
  }

  @Test
  public void testIncrement() throws IOException {
    mockFlowController();
    Increment increment = new Increment("r1".getBytes());
    when(primaryTable.increment(any(Increment.class)))
        .thenReturn(
            Result.create(
                new Cell[] {
                  CellUtil.createCell(
                      "r1".getBytes(),
                      "f1".getBytes(),
                      "q1".getBytes(),
                      12,
                      Type.Put.getCode(),
                      Longs.toByteArray(142))
                }));
    mirroringTable.increment(increment);
    mirroringTable.incrementColumnValue("r1".getBytes(), "f1".getBytes(), "q1".getBytes(), 3L);
    mirroringTable.incrementColumnValue(
        "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), 3L, Durability.SYNC_WAL);
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Increment> argument = ArgumentCaptor.forClass(Increment.class);
    verify(secondaryTable, times(3)).increment(argument.capture());
    assertThat(argument.getAllValues().get(0)).isEqualTo(increment);
  }

  @Test
  public void testAppend() throws IOException {
    mockFlowController();
    Append append = new Append("r1".getBytes());
    when(primaryTable.append(any(Append.class)))
        .thenReturn(
            Result.create(
                new Cell[] {
                  CellUtil.createCell(
                      "r1".getBytes(),
                      "f1".getBytes(),
                      "q1".getBytes(),
                      12,
                      Type.Put.getCode(),
                      Longs.toByteArray(142))
                }));
    mirroringTable.append(append);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(1)).append(append);
  }

  @Test
  public void testBatchWithCallback() throws IOException, InterruptedException {
    mockFlowController();
    List<Get> mutations = Arrays.asList(createGet("get1"));
    Object[] results = new Object[] {createResult("test")};
    Callback<?> callback = mock(Callback.class);
    mirroringTable.batchCallback(mutations, results, callback);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batchCallback(mutations, results, callback);
    verify(secondaryTable, times(1)).batch(eq(mutations), any(Object[].class));
  }

  @Test
  public void testBatchWithAllFailedDoesntCallSecondary() throws IOException, InterruptedException {
    List<Get> mutations = Arrays.asList(createGet("get1"));
    Object[] results = new Object[] {null};
    Callback<?> callback = mock(Callback.class);
    mirroringTable.batchCallback(mutations, results, callback);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batchCallback(mutations, results, callback);
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testBatchWithoutResultParameter() throws IOException, InterruptedException {
    List<Get> mutations = Arrays.asList(createGet("get1"));
    mirroringTable.batch(mutations);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batch(eq(mutations), any(Object[].class));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testBatchCallbackWithoutResultParameter() throws IOException, InterruptedException {
    List<Get> mutations = Arrays.asList(createGet("get1"));
    Callback<?> callback = mock(Callback.class);
    mirroringTable.batchCallback(mutations, callback);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batchCallback(eq(mutations), any(Object[].class), eq(callback));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }
}
