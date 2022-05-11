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

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createDelete;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGets;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createMockBatchAnswer;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.mockBatch;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerToRejectRequests;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringMetricsRecorder;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanFactory;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector.ScannerResultVerifier;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
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
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  @Mock ReferenceCounter referenceCounter;
  @Mock MirroringMetricsRecorder mirroringMetricsRecorder;
  Timestamper timestamper = new NoopTimestamper();

  MismatchDetector mismatchDetector;
  MirroringTable mirroringTable;
  MirroringTracer mirroringTracer;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringTracer =
        new MirroringTracer(
            new MirroringSpanFactory(Tracing.getTracer(), mirroringMetricsRecorder),
            mirroringMetricsRecorder);
    this.mismatchDetector = spy(new DefaultMismatchDetector(this.mirroringTracer, 100));
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new ReadSampler(100),
                this.timestamper,
                false,
                false,
                this.mirroringTracer,
                this.referenceCounter,
                1000));
  }

  private void waitForMirroringScanner(ResultScanner mirroringScanner)
      throws InterruptedException, ExecutionException, TimeoutException {
    ((MirroringResultScanner) mirroringScanner)
        .closePrimaryAndScheduleSecondaryClose()
        .get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetSingle() throws IOException {
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
  public void testPrimaryReadExceptionDoesntCallSecondaryNorVerification() throws IOException {
    Get request = createGet("test");
    IOException expectedException = new IOException("expected");
    when(primaryTable.get(request)).thenThrow(expectedException);

    try {
      mirroringTable.get(request);
      fail("should have thrown");
    } catch (IOException e) {
      assertThat(e).isEqualTo(expectedException);
    }
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, never()).get(any(Get.class));
    verify(mismatchDetector, never()).get(request, expectedException);
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
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(request, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetMultiple() throws IOException {
    List<Get> get = Arrays.asList(createGet("test"));
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

    verify(mismatchDetector, times(1))
        .scannerNext(eq(scan), any(ScannerResultVerifier.class), eq(expected1), eq(expected1));
    verify(mismatchDetector, times(1))
        .scannerNext(eq(scan), any(ScannerResultVerifier.class), eq(expected2), eq(expected2));
    verify(mismatchDetector, times(1))
        .scannerNext(
            eq(scan), any(ScannerResultVerifier.class), eq((Result) null), eq((Result) null));
    verify(mismatchDetector, times(3))
        .scannerNext(eq(scan), any(ScannerResultVerifier.class), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnScannerNextOne()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
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

    verify(mismatchDetector, times(1))
        .scannerNext(eq(scan), any(ScannerResultVerifier.class), eq(expected1), eq(expected1));
    verify(mismatchDetector, times(1)).scannerNext(scan, expectedException);
    verify(mismatchDetector, times(1))
        .scannerNext(
            eq(scan), any(ScannerResultVerifier.class), eq((Result) null), eq((Result) null));
    verify(mismatchDetector, times(2))
        .scannerNext(eq(scan), any(ScannerResultVerifier.class), (Result) any(), (Result) any());
  }

  @Test
  public void testMismatchDetectorIsCalledOnScannerNextMultiple()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
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

    verify(mismatchDetector, times(1))
        .scannerNext(eq(scan), any(ScannerResultVerifier.class), eq(expected), eq(expected));
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnScannerNextMultiple()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
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

    verify(mismatchDetector, times(1)).scannerNext(scan, 2, expectedException);
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
  public void testScannerRenewLeaseSecondaryFailed()
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
  public void testScannerRenewLeaseSecondaryUnsupported()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.renewLease()).thenReturn(true);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.renewLease())
        .thenThrow(new UnsupportedOperationException("expected"));
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);

    // Secondary's renewLease thrown UnsupportedOperationException, thus we assume that it is a
    // Bigtable scanner and renewing the lease is not needed. Primary succeeded and we should
    // return true.
    assertThat(mirroringScanner.renewLease()).isTrue();

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();
    verify(secondaryScannerMock, times(1)).renewLease();
  }

  @Test
  public void testScannerRenewLeasePrimaryUnsupported()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.renewLease()).thenThrow(new UnsupportedOperationException("expected"));
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.renewLease()).thenReturn(true);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = mirroringTable.getScanner(scan);

    // Primary's renewLease thrown UnsupportedOperationException, thus we assume that it is a
    // Bigtable scanner and renewing the lease is not needed. Secondary succeeded and we should
    // return true.
    assertThat(mirroringScanner.renewLease()).isTrue();

    waitForMirroringScanner(mirroringScanner);
    executorServiceRule.waitForExecutor();
    verify(secondaryScannerMock, times(1)).renewLease();
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
  public void testPutIsMirrored() throws IOException, InterruptedException {
    Put put = createPut("test", "f1", "q1", "v1");
    List<Put> puts = new ArrayList<>();
    puts.add(put);

    mockBatch(primaryTable, put, new Result());

    mirroringTable.put(put);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).batch(eq(puts), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(puts), any(Object[].class));
  }

  @Test
  public void testPutListIsMirrored() throws IOException, InterruptedException {
    Put put = createPut("test", "f1", "q1", "v1");
    List<Put> puts = Arrays.asList(put);

    mockBatch(primaryTable, put, new Result());

    mirroringTable.put(puts);
    executorServiceRule.waitForExecutor();

    // put(List<Put>) is mirrored using batch, we because we have to detect partially applied
    // writes.
    verify(primaryTable, times(1)).batch(eq(puts), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(puts), any(Object[].class));
  }

  @Test
  public void testPutWithErrorIsNotMirrored() throws IOException, InterruptedException {
    final Put put = createPut("test", "f1", "q1", "v1");
    mockBatch(this.primaryTable, put, new IOException("test"));

    assertThrows(
        IOException.class,
        new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            mirroringTable.put(put);
          }
        });
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).batch(eq(Arrays.asList(put)), any(Object[].class));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testPutWithSecondaryErrorCallsErrorHandler()
      throws IOException, InterruptedException {
    Put put = createPut("test", "f1", "q1", "v1");
    mockBatch(primaryTable);
    mockBatch(secondaryTable, put, new IOException("test exception"));

    mirroringTable.put(put);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).batch(eq(Collections.singletonList(put)), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(Collections.singletonList(put)), any(Object[].class));

    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.BATCH), eq(put), any(Throwable.class));
  }

  @Test
  public void testBatchGetAndPutGetsAreVerifiedOnSuccess()
      throws IOException, InterruptedException {
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Get get1 = createGet("get1");

    List<Row> requests = Arrays.asList(new Row[] {put1, get1});

    // op   | p    | s
    // put1 | ok   | ok
    // get1 | ok   | ok

    final Result get1Result = createResult("get1", "value1");

    // primary
    Object[] results = new Object[2];

    List<Row> secondaryRequests = Arrays.asList(new Row[] {put1, get1});

    mockBatch(primaryTable, secondaryTable, put1, Result.create(new Cell[0]), get1, get1Result);

    mirroringTable.batch(requests, results);
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(primaryTable, times(1)).batch(requests, results);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests), argument.capture());
    assertThat(argument.getValue().length).isEqualTo(2);

    // failed secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get1), new Result[] {get1Result}, new Result[] {get1Result});
  }

  @Test
  public void testBatchGetAndPut() throws IOException, InterruptedException {
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
    mockBatch(
        primaryTable,
        put1,
        Result.create(new Cell[0]),
        put2,
        new IOException("test1"),
        put3,
        Result.create(new Cell[0]),
        get1,
        get1Result,
        get2,
        new IOException("test1"),
        get3,
        get3Result);

    List<Row> secondaryRequests = Arrays.asList(new Row[] {put1, put3, get1, get3});

    mockBatch(
        secondaryTable,
        put1,
        null,
        put3,
        Result.create(new Cell[0]),
        get1,
        new IOException("test"),
        get3,
        get3Result);

    try {
      mirroringTable.batch(requests, results);
      fail("should have thrown");
    } catch (IOException ignored) {
    }
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(primaryTable, times(1)).batch(requests, results);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests), argument.capture());
    assertThat(argument.getValue().length).isEqualTo(4);

    // failed secondary writes were reported
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.BATCH), eq(put1), (Throwable) isNull());

    // failed secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get3), new Result[] {get3Result}, new Result[] {get3Result});

    // failed secondary reads were reported
    verify(mismatchDetector, times(1)).batch(eq(Arrays.asList(get1)), any(IOException.class));
  }

  @Test
  public void testBatchGetsPrimaryFailsSecondaryOk() throws IOException, InterruptedException {
    Get get1 = createGet("get1");
    Get get2 = createGet("get2");

    List<Row> requests = Arrays.asList(new Row[] {get1, get2});

    // op   | p    | s
    // get1 | fail | x
    // get2 | ok   | ok

    final Result get2Result = createResult("get2", "value2");

    // primary
    Object[] results = new Object[2];

    List<Row> secondaryRequests = Arrays.asList(new Row[] {get2});

    mockBatch(primaryTable, get1, new IOException("test"), get2, get2Result);
    mockBatch(secondaryTable, get2, get2Result);

    try {
      mirroringTable.batch(requests, results);
      fail("should have thrown");
    } catch (IOException ignored) {
    }
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(primaryTable, times(1)).batch(requests, results);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests), argument.capture());
    assertThat(argument.getValue().length).isEqualTo(1);

    // failed secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get2), new Result[] {get2Result}, new Result[] {get2Result});

    // no read errors reported
    verify(mismatchDetector, never())
        .batch(ArgumentMatchers.<Get>anyList(), any(IOException.class));
  }

  @Test
  public void testConditionalWriteHappensWhenConditionIsMet() throws IOException {
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
        "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), "v1".getBytes(), put);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(1)).mutateRow(any(RowMutations.class));
  }

  @Test
  public void testCheckAndDelete() throws IOException {
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
        "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), "v1".getBytes(), delete);
    executorServiceRule.waitForExecutor();

    verify(secondaryTable, times(1)).mutateRow(any(RowMutations.class));
  }

  @Test
  public void testCheckAndMutate() throws IOException {
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
    Delete delete = new Delete("r1".getBytes());

    mockBatch(primaryTable, secondaryTable, delete, new Result());

    mirroringTable.delete(delete);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).batch(eq(Arrays.asList(delete)), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(Arrays.asList(delete)), any(Object[].class));
  }

  @Test
  public void testDeleteList() throws IOException, InterruptedException {
    List<Delete> deletes = new ArrayList<>();
    deletes.add(new Delete("r1".getBytes()));

    List<Delete> originalDeletes = new ArrayList<>(deletes);

    mockBatch(primaryTable, secondaryTable, deletes.get(0), new Result());

    mirroringTable.delete(deletes);
    assertThat(deletes).isEmpty();
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batch(eq(originalDeletes), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(originalDeletes), any(Object[].class));
  }

  @Test
  public void testMutateRow() throws IOException, InterruptedException {
    RowMutations mutations = new RowMutations("r1".getBytes());
    List<? extends Row> listOfMutations = Arrays.asList(mutations);
    mockBatch(primaryTable, secondaryTable, mutations, new Result());
    mirroringTable.mutateRow(mutations);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batch(eq(listOfMutations), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(listOfMutations), any(Object[].class));
  }

  @Test
  public void testIncrement() throws IOException {
    byte[] row = "r1".getBytes();
    byte[] family = "f1".getBytes();
    byte[] qualifier = "q1".getBytes();
    long ts = 12;
    byte[] value = Longs.toByteArray(142);

    Put expectedPut = new Put(row);
    expectedPut.addColumn(family, qualifier, ts, value);

    Increment increment = new Increment(row);
    when(primaryTable.increment(any(Increment.class)))
        .thenReturn(
            Result.create(
                new Cell[] {
                  CellUtil.createCell(row, family, qualifier, ts, Type.Put.getCode(), value)
                }));
    mirroringTable.increment(increment);
    mirroringTable.incrementColumnValue(row, family, qualifier, 3L);
    mirroringTable.incrementColumnValue(row, family, qualifier, 3L, Durability.SYNC_WAL);
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Put> argument = ArgumentCaptor.forClass(Put.class);
    verify(primaryTable, times(3)).increment(any(Increment.class));
    verify(secondaryTable, never()).increment(any(Increment.class));
    verify(secondaryTable, times(3)).put(argument.capture());

    assertPutsAreEqual(argument.getAllValues().get(0), expectedPut);
    assertPutsAreEqual(argument.getAllValues().get(1), expectedPut);
    assertPutsAreEqual(argument.getAllValues().get(2), expectedPut);
  }

  @Test
  public void testAppend() throws IOException {
    byte[] row = "r1".getBytes();
    byte[] family = "f1".getBytes();
    byte[] qualifier = "q1".getBytes();
    long ts = 12;
    byte[] value = "v1".getBytes();

    Append append = new Append(row);
    Put expectedPut = new Put(row);
    expectedPut.addColumn(family, qualifier, ts, value);

    when(primaryTable.append(any(Append.class)))
        .thenReturn(
            Result.create(
                new Cell[] {
                  CellUtil.createCell(row, family, qualifier, ts, Type.Put.getCode(), value)
                }));
    mirroringTable.append(append);
    verify(primaryTable, times(1)).append(append);
    executorServiceRule.waitForExecutor();

    ArgumentCaptor<Put> argument = ArgumentCaptor.forClass(Put.class);
    verify(secondaryTable, times(1)).put(argument.capture());
    assertPutsAreEqual(expectedPut, argument.getValue());

    verify(secondaryTable, never()).append(any(Append.class));
  }

  private void assertPutsAreEqual(Put expectedPut, Put value) {
    TestHelpers.assertPutsAreEqual(
        expectedPut,
        value,
        new TestHelpers.CellComparatorCompat() {
          @Override
          public int compare(Cell a, Cell b) {
            return new CellComparator().compare(a, b);
          }
        });
  }

  @Test
  public void testAppendWhichDoesntWantResult() throws IOException {
    final byte[] row = "r1".getBytes();
    final byte[] family = "f1".getBytes();
    final byte[] qualifier = "q1".getBytes();
    final long ts = 12;
    final byte[] value = "v1".getBytes();

    Append appendIgnoringResult = new Append(row).setReturnResults(false);

    when(primaryTable.append(any(Append.class)))
        .thenReturn(
            Result.create(
                new Cell[] {
                  CellUtil.createCell(row, family, qualifier, ts, Type.Put.getCode(), value)
                }));
    Result appendWithoutResult = mirroringTable.append(appendIgnoringResult);

    ArgumentCaptor<Append> appendCaptor = ArgumentCaptor.forClass(Append.class);
    verify(primaryTable, times(1)).append(appendCaptor.capture());
    assertThat(appendCaptor.getValue().isReturnResults()).isTrue();
    assertThat(appendWithoutResult).isNull();
  }

  @Test
  public void testIncrementWhichDoesntWantResult() throws IOException {
    final byte[] row = "r1".getBytes();
    final byte[] family = "f1".getBytes();
    final byte[] qualifier = "q1".getBytes();
    final long ts = 12;
    final byte[] value = "v1".getBytes();

    Increment incrementIgnoringResult = new Increment(row).setReturnResults(false);

    when(primaryTable.increment(any(Increment.class)))
        .thenReturn(
            Result.create(
                new Cell[] {
                  CellUtil.createCell(row, family, qualifier, ts, Type.Put.getCode(), value)
                }));
    Result incrementWithoutResult = mirroringTable.increment(incrementIgnoringResult);

    ArgumentCaptor<Increment> incrementCaptor = ArgumentCaptor.forClass(Increment.class);
    verify(primaryTable, times(1)).increment(incrementCaptor.capture());
    assertThat(incrementCaptor.getValue().isReturnResults()).isTrue();
    assertThat(incrementWithoutResult.value()).isNull();
  }

  @Test
  public void testBatchAppendWhichDoesntWantResult() throws IOException, InterruptedException {
    final byte[] row = "r1".getBytes();
    final byte[] family = "f1".getBytes();
    final byte[] qualifier = "q1".getBytes();
    final long ts = 12;
    final byte[] value = "v1".getBytes();

    List<Append> batchAppendIgnoringResult =
        Collections.singletonList(new Append(row).setReturnResults(false));

    mockBatch(
        primaryTable,
        batchAppendIgnoringResult.get(0),
        Result.create(
            new Cell[] {
              CellUtil.createCell(row, family, qualifier, ts, Type.Put.getCode(), value)
            }));
    Object[] batchAppendWithoutResult = mirroringTable.batch(batchAppendIgnoringResult);

    ArgumentCaptor<List<Row>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(primaryTable, times(1)).batch(listCaptor.capture(), any(Object[].class));
    assertThat(listCaptor.getValue().size()).isEqualTo(1);
    assertThat(((Append) listCaptor.getValue().get(0)).isReturnResults()).isTrue();
    assertThat(((Result) batchAppendWithoutResult[0]).value()).isNull();
  }

  @Test
  public void testBatchIncrementWhichDoesntWantResult() throws IOException, InterruptedException {
    final byte[] row = "r1".getBytes();
    final byte[] family = "f1".getBytes();
    final byte[] qualifier = "q1".getBytes();
    final long ts = 12;
    final byte[] value = "v1".getBytes();

    List<Increment> batchIncrementIgnoringResult =
        Collections.singletonList(new Increment(row).setReturnResults(false));

    mockBatch(
        primaryTable,
        batchIncrementIgnoringResult.get(0),
        Result.create(
            new Cell[] {
              CellUtil.createCell(row, family, qualifier, ts, Type.Put.getCode(), value)
            }));
    Object[] batchIncrementWithoutResult = mirroringTable.batch(batchIncrementIgnoringResult);

    ArgumentCaptor<List<Row>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(primaryTable, times(1)).batch(listCaptor.capture(), any(Object[].class));
    assertThat(listCaptor.getValue().size()).isEqualTo(1);
    assertThat(((Increment) listCaptor.getValue().get(0)).isReturnResults()).isTrue();
    assertThat(((Result) batchIncrementWithoutResult[0]).value()).isNull();
  }

  @Test
  public void testBatchWithCallback() throws IOException, InterruptedException {
    List<Get> mutations = Arrays.asList(createGet("get1"));
    Object[] expectedResults = new Object[] {createResult("test")};
    Object[] results = new Object[] {null};

    doAnswer(createMockBatchAnswer(mutations.get(0), expectedResults[0]))
        .when(primaryTable)
        .batchCallback(ArgumentMatchers.<Row>anyList(), any(Object[].class), any(Callback.class));

    Callback<?> callback = mock(Callback.class);
    mirroringTable.batchCallback(mutations, results, callback);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batchCallback(mutations, expectedResults, callback);
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

  @Test
  public void testFlowControllerExceptionInGetPreventsSecondaryOperation() throws IOException {
    setupFlowControllerToRejectRequests(flowController);

    Get request = createGet("test");
    Result expectedResult = createResult("test", "value");

    when(primaryTable.get(request)).thenReturn(expectedResult);

    Result result = mirroringTable.get(request);
    executorServiceRule.waitForExecutor();

    assertThat(result).isEqualTo(expectedResult);

    verify(primaryTable, times(1)).get(request);
    verify(secondaryTable, never()).get(any(Get.class));
  }

  @Test
  public void testFlowControllerExceptionInPutExecutesWriteErrorHandler()
      throws IOException, InterruptedException {
    setupFlowControllerToRejectRequests(flowController);

    Put request = createPut("test", "f1", "q1", "v1");

    mockBatch(primaryTable, request, new Result());

    mirroringTable.put(request);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1))
        .batch(eq(Collections.singletonList(request)), any(Object[].class));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.BATCH), eq(ImmutableList.of(request)), any(Throwable.class));
  }

  @Test
  public void testFlowControllerExceptionInBatchExecutesWriteErrorHandler()
      throws IOException, InterruptedException {
    setupFlowControllerToRejectRequests(flowController);

    Put put1 = createPut("test0", "f1", "q1", "v1");
    Put put2 = createPut("test1", "f1", "q2", "v1");
    Get get1 = createGet("test2");
    List<? extends Row> request = ImmutableList.of(put1, put2, get1);

    mockBatch(
        primaryTable,
        put1,
        Result.create(new Cell[0]),
        put2,
        Result.create(new Cell[0]),
        get1,
        Result.create(new Cell[0]));

    Object[] results = new Object[3];
    mirroringTable.batch(request, results);
    executorServiceRule.waitForExecutor();

    verify(primaryTable, times(1)).batch(request, results);
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.BATCH), eq(ImmutableList.of(put1, put2)), any(Throwable.class));
  }

  @Test
  public void testBatchWithAppendsAndIncrements() throws IOException, InterruptedException {
    Increment increment = new Increment("i".getBytes());
    increment.addColumn("f".getBytes(), "q".getBytes(), 1);

    Append append = new Append("a".getBytes());
    append.add("f".getBytes(), "q".getBytes(), "v".getBytes());

    Put put = createPut("p", "f", "q", "v");
    Get get = createGet("g");
    List<? extends Row> operations = Arrays.asList(increment, append, put, get);
    mockBatch(
        primaryTable,
        increment,
        createResult("i", "f", "q", 1, "1"),
        append,
        createResult("a", "f", "q", 2, "2"),
        put,
        new Result(),
        get,
        createResult("g", "f", "q", 3, "3"));
    Object[] results = new Object[operations.size()];

    List<? extends Row> expectedSecondaryOperations =
        Arrays.asList(
            createPut("i", "f", "q", 1, "1"),
            createPut("a", "f", "q", 2, "2"),
            createPut("p", "f", "q", "v"),
            createGet("g"));
    mirroringTable.batch(operations, results);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).batch(operations, results);
    ArgumentCaptor<List<? extends Row>> argumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(secondaryTable, times(1)).batch(argumentCaptor.capture(), any(Object[].class));

    assertPutsAreEqual(
        (Put) argumentCaptor.getValue().get(0), (Put) expectedSecondaryOperations.get(0));
    assertPutsAreEqual(
        (Put) argumentCaptor.getValue().get(1), (Put) expectedSecondaryOperations.get(1));
    assertPutsAreEqual(
        (Put) argumentCaptor.getValue().get(2), (Put) expectedSecondaryOperations.get(2));
    assertThat(argumentCaptor.getValue().get(3)).isEqualTo(expectedSecondaryOperations.get(3));
  }

  @Test
  public void testConcurrentWritesAreFlowControlledBeforePrimaryAction()
      throws IOException, InterruptedException {
    boolean performWritesConcurrently = true;
    boolean waitForSecondaryWrites = true;
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new ReadSampler(100),
                this.timestamper,
                performWritesConcurrently,
                waitForSecondaryWrites,
                this.mirroringTracer,
                this.referenceCounter,
                10));
    Put put1 = createPut("r1", "f1", "q1", "v1");

    // Both batches should be called even if first one fails.
    mockBatch(primaryTable, secondaryTable, put1, new IOException());

    InOrder inOrder = Mockito.inOrder(primaryTable, flowController);
    try {
      this.mirroringTable.put(put1);
      fail("should fail");
    } catch (IOException ignored) {
    }

    this.executorServiceRule.waitForExecutor();

    inOrder.verify(flowController).asyncRequestResource(any(RequestResourcesDescription.class));
    inOrder.verify(primaryTable).batch(eq(Arrays.asList(put1)), any(Object[].class));

    verify(primaryTable).batch(eq(Arrays.asList(put1)), any(Object[].class));
    verify(secondaryTable).batch(eq(Arrays.asList(put1)), any(Object[].class));
  }

  @Test
  public void testNonConcurrentOpsWontBePerformedConcurrently()
      throws IOException, InterruptedException {
    setupConcurrentMirroringTableWithDirectExecutor();
    Get get = createGet("get1");
    Increment increment = new Increment("row".getBytes());
    Append append = new Append("row".getBytes());

    Put put = createPut("test1", "f1", "q1", "v1");
    Delete delete = createDelete("test2");

    Put putMutation = createPut("test3", "f1", "q1", "v1");
    Delete deleteMutation = createDelete("test3");
    RowMutations rowMutations = new RowMutations("test3".getBytes());
    rowMutations.add(putMutation);
    rowMutations.add(deleteMutation);

    mockBatch(
        primaryTable,
        secondaryTable,
        get,
        createResult(),
        increment,
        createResult("row", "v1"),
        append,
        createResult("row", "v2"));

    // Only Puts and Deletes (and RowMutations which can contain only Puts and Deletes)
    // can be performed concurrently. Other operations force us to wait for primary result
    // (e.g. we implement Increment on secondary as a Put of result from primary).
    // We expect that even though our MirroringTable is concurrent, the operations which cannot be
    // performed concurrently will be performed sequentially.

    // Batch contains an operation which causes the batch to be performed sequentially.
    checkBatchCalledSequentially(Arrays.asList(get));
    checkBatchCalledSequentially(Arrays.asList(increment));
    checkBatchCalledSequentially(Arrays.asList(append));

    // Batch contains only operations which can be performed concurrently.
    checkBatchCalledConcurrently(Arrays.asList(put));
    checkBatchCalledConcurrently(Arrays.asList(delete));
    checkBatchCalledConcurrently(Arrays.asList(rowMutations));
    checkBatchCalledConcurrently(Arrays.asList(put, delete, rowMutations));

    // Batch contains an operation which causes the batch to be performed sequentially.
    checkBatchCalledSequentially(Arrays.asList(put, delete, rowMutations, get));
    checkBatchCalledSequentially(Arrays.asList(put, delete, rowMutations, increment));
    checkBatchCalledSequentially(Arrays.asList(put, delete, rowMutations, append));
  }

  private void setupConcurrentMirroringTableWithDirectExecutor() {
    boolean performWritesConcurrently = true;
    boolean waitForSecondaryWrites = true;
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService()),
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new ReadSampler(100),
                this.timestamper,
                performWritesConcurrently,
                waitForSecondaryWrites,
                this.mirroringTracer,
                this.referenceCounter,
                10));
  }

  private void checkBatchCalledSequentially(List<? extends Row> requests)
      throws IOException, InterruptedException {
    InOrder inOrder = Mockito.inOrder(primaryTable, flowController, secondaryTable);
    this.mirroringTable.batch(requests, new Object[requests.size()]);
    inOrder.verify(primaryTable).batch(eq(requests), any(Object[].class));
    inOrder.verify(flowController).asyncRequestResource(any(RequestResourcesDescription.class));
    inOrder.verify(secondaryTable).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  private void checkBatchCalledConcurrently(List<? extends Row> requests)
      throws IOException, InterruptedException {
    InOrder inOrder = Mockito.inOrder(primaryTable, flowController, secondaryTable);
    this.mirroringTable.batch(requests, new Object[requests.size()]);
    inOrder.verify(flowController).asyncRequestResource(any(RequestResourcesDescription.class));
    // When batch is performed concurrently first secondary database request is scheduled
    // asynchronously, then primary request is executed synchronously.
    // Then depending on configuration the mirroring client may wait for secondary result.
    // In order to be able to verify that the batch is called concurrently we configure the
    // MirroringTable to wait for secondary results and use DirectExecutor.
    // That guarantees us that the method on secondary table is called first.
    inOrder.verify(secondaryTable).batch(eq(requests), any(Object[].class));
    inOrder.verify(primaryTable).batch(eq(requests), any(Object[].class));
  }

  @Test
  public void testConcurrentWritesWithErrors() throws IOException, InterruptedException {
    setupConcurrentMirroringTableWithDirectExecutor();

    Put put1 = createPut("test1", "f1", "q1", "v1");
    Put put2 = createPut("test2", "f2", "q2", "v2");
    Put put3 = createPut("test3", "f3", "q3", "v3");
    Put put4 = createPut("test4", "f4", "q4", "v4");
    Delete delete1 = createDelete("delete1");
    Delete delete2 = createDelete("delete2");
    Delete delete3 = createDelete("delete3");
    Delete delete4 = createDelete("delete4");

    List<? extends Row> requests =
        Arrays.asList(put1, put2, put3, put4, delete1, delete2, delete3, delete4);

    //           |  p1  |  p2  |  p3  |  p4  |  d1  |  d2  |  d3  |  d4
    // primary   |  v   |  v   |  x   |  x   |  v   |  v   |  x   |  x
    // secondary |  v   |  x   |  v   |  x   |  v   |  x   |  v   |  x
    // All errors should be visible in the results.

    IOException put2exception = new IOException("put2");
    IOException put3exception = new IOException("put3");
    IOException put4exception = new IOException("put4");

    IOException delete2exception = new IOException("delete2");
    IOException delete3exception = new IOException("delete3");
    IOException delete4exception = new IOException("delete4");

    mockBatch(
        primaryTable,
        put1,
        new Result(),
        put2,
        new Result(),
        put3,
        put3exception,
        put4,
        put4exception,
        delete1,
        new Result(),
        delete2,
        new Result(),
        delete3,
        delete3exception,
        delete4,
        delete4exception);

    mockBatch(
        secondaryTable,
        put1,
        new Result(),
        put2,
        put2exception,
        put3,
        new Result(),
        put4,
        put4exception,
        delete1,
        new Result(),
        delete2,
        delete2exception,
        delete3,
        new Result(),
        delete4,
        delete4exception);

    Object[] results = new Object[8];
    try {
      this.mirroringTable.batch(requests, results);
      fail("should throw");
    } catch (IOException ignored) {
    }
    assertThat(results[0]).isInstanceOf(Result.class);
    assertThat(results[1]).isEqualTo(put2exception);
    assertThat(results[2]).isEqualTo(put3exception);
    assertThat(results[3]).isEqualTo(put4exception);

    assertThat(results[4]).isInstanceOf(Result.class);
    assertThat(results[5]).isEqualTo(delete2exception);
    assertThat(results[6]).isEqualTo(delete3exception);
    assertThat(results[7]).isEqualTo(delete4exception);

    verify(secondaryWriteErrorConsumer, never())
        .consume(any(HBaseOperation.class), any(Put.class), any(Throwable.class));
  }

  @Test
  public void testConcurrentOpsAreRunConcurrently() throws IOException, InterruptedException {
    boolean performWritesConcurrently = true;
    boolean waitForSecondaryWrites = true;
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new ReadSampler(100),
                this.timestamper,
                performWritesConcurrently,
                waitForSecondaryWrites,
                this.mirroringTracer,
                this.referenceCounter,
                10));

    Put put = createPut("test1", "f1", "q1", "v1");
    mockBatch(primaryTable, secondaryTable);

    final SettableFuture<Void> bothRun = SettableFuture.create();
    final AtomicBoolean firstRun = new AtomicBoolean(false);
    final AtomicBoolean oneWaited = new AtomicBoolean(false);

    Answer<Void> answer =
        new Answer() {
          @Override
          public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            if (firstRun.getAndSet(true)) {
              // Second thread sets this.
              bothRun.set(null);
            } else {
              // First thread sets this.
              oneWaited.set(true);
            }
            // First thread will wait here.
            bothRun.get(3, TimeUnit.SECONDS);
            return null;
          }
        };

    doAnswer(answer).when(primaryTable).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    doAnswer(answer)
        .when(secondaryTable)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));

    mirroringTable.put(put);

    verify(primaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    assertThat(oneWaited.get()).isTrue();
  }

  @Test
  public void testConcurrentOpsAreNotPerformedWhenFlowControllerRejectsRequest()
      throws IOException, InterruptedException {
    IOException flowControllerExpection = setupFlowControllerToRejectRequests(flowController);
    setupConcurrentMirroringTableWithDirectExecutor();

    Put put = createPut("test1", "f1", "q1", "v1");
    try {
      mirroringTable.put(put);
      fail("should throw");
    } catch (IOException e) {
      // FlowController exception is wrapped in IOException by mirroringTable.
      assertThat(e).hasCauseThat().isEqualTo(flowControllerExpection);
    }

    verify(primaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testUnmatchedScannerResultQueuesAreFlushedWhenResultScannerIsClosed()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 100));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next()).thenReturn(expected1);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next()).thenReturn(expected2);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    verify(mirroringMetricsRecorder, never())
        .recordReadMismatches(any(HBaseOperation.class), eq(1));
    waitForMirroringScanner(mirroringScanner);

    verify(verifier, times(1)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, never())
        .recordReadMatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, times(2))
        .recordReadMismatches(any(HBaseOperation.class), eq(1));
  }

  @Test
  public void testScannerResultVerifierWithBufferSizeZero()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 0));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");
    Result expected3 = createResult("test3", "value3");
    Result expected4 = createResult("test4", "value4");
    Result expected5 = createResult("test5", "value5");
    Result expected6 = createResult("test6", "value6");
    Result expected7 = createResult("test7", "value7");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next())
        .thenReturn(expected1, expected2, expected3, expected4, expected5, expected6, expected7);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next())
        .thenReturn(expected1, expected3, expected5, expected7, null, null, null);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    assertThat(mirroringScanner.next()).isEqualTo(expected2);
    assertThat(mirroringScanner.next()).isEqualTo(expected3);
    assertThat(mirroringScanner.next()).isEqualTo(expected4);
    assertThat(mirroringScanner.next()).isEqualTo(expected5);
    assertThat(mirroringScanner.next()).isEqualTo(expected6);
    assertThat(mirroringScanner.next()).isEqualTo(expected7);
    waitForMirroringScanner(mirroringScanner);

    verify(verifier, times(7)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, times(1)).recordReadMatches(any(HBaseOperation.class), eq(1));
    verify(mirroringMetricsRecorder, times(9))
        .recordReadMismatches(any(HBaseOperation.class), eq(1));
  }

  @Test
  public void testScannerUnmatchedBufferSpaceRunsOut()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 2));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");
    Result expected3 = createResult("test3", "value3");
    Result expected4 = createResult("test4", "value4");
    Result expected5 = createResult("test5", "value5");
    Result expected6 = createResult("test6", "value6");
    Result expected7 = createResult("test7", "value7");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next()).thenReturn(expected1, expected2, expected3, expected4);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next()).thenReturn(expected4, expected5, expected6, expected7);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    assertThat(mirroringScanner.next()).isEqualTo(expected2);
    assertThat(mirroringScanner.next()).isEqualTo(expected3);
    assertThat(mirroringScanner.next()).isEqualTo(expected4);
    waitForMirroringScanner(mirroringScanner);

    verify(verifier, times(4)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, never())
        .recordReadMatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, times(8))
        .recordReadMismatches(any(HBaseOperation.class), eq(1));
  }

  @Test
  public void testScannerUnmatchedBufferSpaceRunsOutThenReturnsMatches()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 2));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");
    Result expected3 = createResult("test3", "value3");
    Result expected4 = createResult("test4", "value4");
    Result expected5 = createResult("test5", "value5");
    Result expected6 = createResult("test6", "value6");
    Result expected7 = createResult("test7", "value7");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next())
        .thenReturn(expected1, expected2, expected3, expected4, expected1, expected2, expected3);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next())
        .thenReturn(expected4, expected5, expected6, expected7, expected1, expected2, expected3);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    assertThat(mirroringScanner.next()).isEqualTo(expected2);
    assertThat(mirroringScanner.next()).isEqualTo(expected3);
    assertThat(mirroringScanner.next()).isEqualTo(expected4);
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    assertThat(mirroringScanner.next()).isEqualTo(expected2);
    assertThat(mirroringScanner.next()).isEqualTo(expected3);
    waitForMirroringScanner(mirroringScanner);

    verify(verifier, times(7)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, times(3))
        .recordReadMatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, times(8))
        .recordReadMismatches(any(HBaseOperation.class), eq(1));
  }

  @Test
  public void testScannerSkippedSomeResults()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 100));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");
    Result expected3 = createResult("test3", "value3");
    Result expected4 = createResult("test4", "value4");
    Result expected5 = createResult("test5", "value5");
    Result expected6 = createResult("test6", "value6");
    Result expected7 = createResult("test7", "value7");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next())
        .thenReturn(expected1, expected2, expected3, expected5, expected6, expected7);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next())
        .thenReturn(expected1, expected3, expected4, expected5, expected6, null);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    assertThat(mirroringScanner.next()).isEqualTo(expected2);
    assertThat(mirroringScanner.next()).isEqualTo(expected3);
    assertThat(mirroringScanner.next()).isEqualTo(expected5);
    assertThat(mirroringScanner.next()).isEqualTo(expected6);
    assertThat(mirroringScanner.next()).isEqualTo(expected7);
    waitForMirroringScanner(mirroringScanner);

    verify(verifier, times(6)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, times(4))
        .recordReadMatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, times(3))
        .recordReadMismatches(any(HBaseOperation.class), eq(1));
  }

  @Test
  public void testScannerValueMismatchIsDetected()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 100));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected1 = createResult("test1", "value1");
    Result expected2 = createResult("test2", "value2");
    Result expected31 = createResult("test3", "value31");
    Result expected32 = createResult("test3", "value32");
    Result expected4 = createResult("test4", "value4");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next()).thenReturn(expected1, expected2, expected31, expected4);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next()).thenReturn(expected1, expected2, expected32, expected4);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected1);
    assertThat(mirroringScanner.next()).isEqualTo(expected2);
    assertThat(mirroringScanner.next()).isEqualTo(expected31);
    assertThat(mirroringScanner.next()).isEqualTo(expected4);
    waitForMirroringScanner(mirroringScanner);

    verify(verifier, times(4)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, times(3)).recordReadMatches(HBaseOperation.NEXT, 1);
    verify(mirroringMetricsRecorder, times(1)).recordReadMismatches(HBaseOperation.NEXT, 1);
  }

  @Test
  public void testScannerRowsResynchronization()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    ScannerResultVerifier verifier =
        spy(mismatchDetector.createScannerResultVerifier(new Scan(), 100));
    when(mismatchDetector.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(verifier);

    Result expected11 = createResult("test11", "value1");
    Result expected12 = createResult("test12", "value1");
    Result expected21 = createResult("test21", "value2");
    Result expected22 = createResult("test22", "value2");
    Result expected31 = createResult("test31", "value3");
    Result expected32 = createResult("test32", "value3");
    Result expected4 = createResult("test4", "value4");
    Result expected51 = createResult("test51", "value5");
    Result expected52 = createResult("test52", "value5");

    ResultScanner primaryScannerMock = mock(ResultScanner.class);
    when(primaryScannerMock.next())
        .thenReturn(expected11, expected21, expected31, expected4, expected51);
    when(primaryTable.getScanner((Scan) any())).thenReturn(primaryScannerMock);

    ResultScanner secondaryScannerMock = mock(ResultScanner.class);
    when(secondaryScannerMock.next())
        .thenReturn(expected12, expected22, expected32, expected4, expected52);
    when(secondaryTable.getScanner((Scan) any())).thenReturn(secondaryScannerMock);

    Scan scan = new Scan();
    ResultScanner mirroringScanner = spy(mirroringTable.getScanner(scan));
    assertThat(mirroringScanner.next()).isEqualTo(expected11);
    assertThat(mirroringScanner.next()).isEqualTo(expected21);
    assertThat(mirroringScanner.next()).isEqualTo(expected31);
    assertThat(mirroringScanner.next()).isEqualTo(expected4);
    assertThat(mirroringScanner.next()).isEqualTo(expected51);
    executorServiceRule.waitForExecutor();

    verify(verifier, times(5)).verify(any(Result[].class), any(Result[].class));
    verify(mirroringMetricsRecorder, times(1)).recordReadMatches(HBaseOperation.NEXT, 1);
    // 51 and 52 were not yet compared
    verify(mirroringMetricsRecorder, times(6)).recordReadMismatches(HBaseOperation.NEXT, 1);
  }
}
