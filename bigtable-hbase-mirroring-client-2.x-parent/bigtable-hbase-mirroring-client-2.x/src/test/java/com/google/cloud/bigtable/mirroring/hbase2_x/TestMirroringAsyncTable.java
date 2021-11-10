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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGets;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerToRejectRequests;
import static com.google.common.truth.Truth.assertThat;
import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.OperationUtils;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.apache.hadoop.hbase.io.TimeRange;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringAsyncTable {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock AsyncTable<ScanResultConsumerBase> primaryTable;
  @Mock AsyncTable<ScanResultConsumerBase> secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  @Mock ListenableReferenceCounter referenceCounter;
  @Mock AsyncTable.CheckAndMutateBuilder primaryBuilder;

  MirroringAsyncTable<ScanResultConsumerBase> mirroringTable;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringTable =
        spy(
            new MirroringAsyncTable<>(
                primaryTable,
                secondaryTable,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new MirroringTracer(),
                new ReadSampler(100),
                referenceCounter));

    lenient()
        .doReturn(primaryBuilder)
        .when(primaryTable)
        .checkAndMutate(any(byte[].class), any(byte[].class));
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetSingle()
      throws ExecutionException, InterruptedException {
    Get get = createGets("test").get(0);
    Result expectedResult = createResult("test", "value");
    CompletableFuture<Result> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Result> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.get(get)).thenReturn(primaryFuture);
    when(secondaryTable.get(get)).thenReturn(secondaryFuture);

    verify(referenceCounter, never()).incrementReferenceCount();
    CompletableFuture<Result> resultFuture = mirroringTable.get(get);
    verify(referenceCounter, times(1)).incrementReferenceCount();
    primaryFuture.complete(expectedResult);
    verify(referenceCounter, never()).decrementReferenceCount();
    secondaryFuture.complete(expectedResult);
    verify(referenceCounter, times(1)).decrementReferenceCount();
    Result result = resultFuture.get();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).get((Get) any(), any());
    verify(mismatchDetector, never()).get(anyList(), any(Result[].class), any(Result[].class));
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnSingleGet()
      throws ExecutionException, InterruptedException {
    Get get = createGet("test");
    Result expectedResult = createResult("test", "value");
    CompletableFuture<Result> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Result> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.get(get)).thenReturn(primaryFuture);
    when(secondaryTable.get(get)).thenReturn(secondaryFuture);

    IOException expectedException = new IOException("expected");

    verify(referenceCounter, never()).incrementReferenceCount();
    CompletableFuture<Result> resultFuture = mirroringTable.get(get);
    verify(referenceCounter, times(1)).incrementReferenceCount();
    primaryFuture.complete(expectedResult);
    verify(referenceCounter, never()).decrementReferenceCount();
    secondaryFuture.completeExceptionally(expectedException);
    verify(referenceCounter, times(1)).decrementReferenceCount();
    Result result = resultFuture.get();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetMultiple()
      throws ExecutionException, InterruptedException {
    List<Get> get = createGets("test");
    Result[] expectedResultArray = {createResult("test", "value")};
    CompletableFuture<Result> expectedFuture = new CompletableFuture<>();
    List<CompletableFuture<Result>> expectedResultFutureList =
        Collections.singletonList(expectedFuture);

    when(primaryTable.get(get)).thenReturn(expectedResultFutureList);
    when(secondaryTable.get(get)).thenReturn(expectedResultFutureList);

    List<CompletableFuture<Result>> resultFutures = mirroringTable.get(get);
    assertThat(resultFutures.size()).isEqualTo(1);

    expectedFuture.complete(expectedResultArray[0]);
    Result result = resultFutures.get(0).get();
    assertThat(result).isEqualTo(expectedResultArray[0]);

    verify(mismatchDetector, times(1))
        .batch(eq(get), eq(expectedResultArray), eq(expectedResultArray));
    verify(mismatchDetector, never()).batch(anyList(), any());
    verify(mismatchDetector, never()).get(any(Get.class), any());
    verify(mismatchDetector, never()).get(any(Get.class), any(), any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnGetMultiple()
      throws ExecutionException, InterruptedException {
    List<Get> get = createGets("test1", "test2");
    Result[] expectedResultArray = {
      createResult("test1", "value1"), createResult("test2", "value2")
    };
    CompletableFuture<Result> expectedFuture1 = new CompletableFuture<>();
    CompletableFuture<Result> expectedFuture2 = new CompletableFuture<>();
    CompletableFuture<Result> exceptionalFuture = new CompletableFuture<>();
    List<CompletableFuture<Result>> expectedResultFutureList =
        Arrays.asList(expectedFuture1, expectedFuture2);
    List<CompletableFuture<Result>> exceptionalResultFutureList =
        Arrays.asList(exceptionalFuture, exceptionalFuture);

    when(primaryTable.get(get)).thenReturn(expectedResultFutureList);
    when(secondaryTable.get(get)).thenReturn(exceptionalResultFutureList);

    List<CompletableFuture<Result>> resultFutures = mirroringTable.get(get);
    assertThat(resultFutures.size()).isEqualTo(2);

    expectedFuture1.complete(expectedResultArray[0]);
    expectedFuture2.complete(expectedResultArray[1]);
    IOException ioe = new IOException("expected");
    exceptionalFuture.completeExceptionally(ioe);
    Result result1 = resultFutures.get(0).get();
    assertThat(result1).isEqualTo(expectedResultArray[0]);
    Result result2 = resultFutures.get(1).get();
    assertThat(result2).isEqualTo(expectedResultArray[1]);

    ArgumentCaptor<CompletionException> argument =
        ArgumentCaptor.forClass(CompletionException.class);
    verify(mismatchDetector, times(1)).batch(eq(get), argument.capture());
    assertThat(argument.getValue().getCause()).isEqualTo(ioe);

    verify(mismatchDetector, never()).batch(anyList(), any(), any());
    verify(mismatchDetector, never()).get(any(Get.class), any());
    verify(mismatchDetector, never()).get(any(Get.class), any(), any());
  }

  @Test
  public void testMismatchDetectorIsCalledOnExistsSingle()
      throws ExecutionException, InterruptedException {
    Get get = createGet("test");
    final boolean expectedResult = true;
    CompletableFuture<Boolean> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Boolean> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.exists(get)).thenReturn(primaryFuture);
    when(secondaryTable.exists(get)).thenReturn(secondaryFuture);

    CompletableFuture<Boolean> resultFuture = mirroringTable.exists(get);
    primaryFuture.complete(expectedResult);
    secondaryFuture.complete(expectedResult);
    Boolean result = resultFuture.get();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).exists(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).exists(any(Get.class), any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExists()
      throws ExecutionException, InterruptedException {
    Get get = createGet("test");
    final boolean expectedResult = true;
    CompletableFuture<Boolean> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Boolean> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.exists(get)).thenReturn(primaryFuture);
    when(secondaryTable.exists(get)).thenReturn(secondaryFuture);

    IOException expectedException = new IOException("expected");

    CompletableFuture<Boolean> resultFuture = mirroringTable.exists(get);
    primaryFuture.complete(expectedResult);
    secondaryFuture.completeExceptionally(expectedException);
    Boolean result = resultFuture.get();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).exists(get, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnExistsMultiple()
      throws ExecutionException, InterruptedException {
    List<Get> get = createGets("test");
    boolean[] expectedResultArray = {false};
    CompletableFuture<Boolean> expectedFuture = new CompletableFuture<>();
    List<CompletableFuture<Boolean>> expectedResultFutureList =
        Collections.singletonList(expectedFuture);

    when(primaryTable.exists(get)).thenReturn(expectedResultFutureList);
    when(secondaryTable.exists(get)).thenReturn(expectedResultFutureList);

    List<CompletableFuture<Boolean>> resultFutures = mirroringTable.exists(get);
    assertThat(resultFutures.size()).isEqualTo(1);

    expectedFuture.complete(expectedResultArray[0]);
    Boolean result = resultFutures.get(0).get();
    assertThat(result).isEqualTo(expectedResultArray[0]);

    verify(mismatchDetector, times(1))
        .existsAll(eq(get), eq(expectedResultArray), eq(expectedResultArray));
    verify(mismatchDetector, never()).batch(anyList(), any());
    verify(mismatchDetector, never()).get(any(Get.class), any());
    verify(mismatchDetector, never()).get(any(Get.class), any(), any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExistsMultiple()
      throws ExecutionException, InterruptedException {
    List<Get> gets = createGets("test1", "test2");
    boolean[] expectedResultArray = {true, false};
    CompletableFuture<Boolean> expectedFuture1 = new CompletableFuture<>();
    CompletableFuture<Boolean> expectedFuture2 = new CompletableFuture<>();
    CompletableFuture<Boolean> exceptionalFuture = new CompletableFuture<>();
    List<CompletableFuture<Boolean>> expectedResultFutureList =
        Arrays.asList(expectedFuture1, expectedFuture2);
    List<CompletableFuture<Boolean>> exceptionalResultFutureList =
        Arrays.asList(exceptionalFuture, exceptionalFuture);

    when(primaryTable.exists(gets)).thenReturn(expectedResultFutureList);
    when(secondaryTable.exists(gets)).thenReturn(exceptionalResultFutureList);

    List<CompletableFuture<Boolean>> resultFutures = mirroringTable.exists(gets);
    assertThat(resultFutures.size()).isEqualTo(2);

    expectedFuture1.complete(expectedResultArray[0]);
    expectedFuture2.complete(expectedResultArray[1]);
    IOException ioe = new IOException("expected");
    exceptionalFuture.completeExceptionally(ioe);
    Boolean result1 = resultFutures.get(0).get();
    assertThat(result1).isEqualTo(expectedResultArray[0]);
    Boolean result2 = resultFutures.get(1).get();
    assertThat(result2).isEqualTo(expectedResultArray[1]);

    verify(mismatchDetector, times(1)).existsAll(eq(gets), any(Throwable.class));

    verify(mismatchDetector, never()).batch(anyList(), any(), any());
    verify(mismatchDetector, never()).get(any(Get.class), any());
    verify(mismatchDetector, never()).get(any(Get.class), any(), any());
  }

  @Test
  public void testPutIsMirrored() throws InterruptedException, ExecutionException {
    Put put = createPut("test", "f1", "q1", "v1");
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.put(put)).thenReturn(primaryFuture);
    when(secondaryTable.put(put)).thenReturn(secondaryFuture);

    verify(referenceCounter, never()).incrementReferenceCount();
    CompletableFuture<Void> resultFuture = mirroringTable.put(put);
    verify(referenceCounter, times(1)).incrementReferenceCount();
    primaryFuture.complete(null);
    verify(referenceCounter, never()).decrementReferenceCount();
    secondaryFuture.complete(null);
    verify(referenceCounter, times(1)).decrementReferenceCount();
    resultFuture.get();

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(1)).put(put);
  }

  @Test
  public void testPutWithErrorIsNotMirrored() {
    final Put put = createPut("test", "f1", "q1", "v1");
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    when(primaryTable.put(put)).thenReturn(primaryFuture);

    verify(referenceCounter, never()).incrementReferenceCount();
    CompletableFuture<Void> resultFuture = mirroringTable.put(put);
    verify(referenceCounter, times(1)).incrementReferenceCount();

    IOException expectedException = new IOException("expected");
    verify(referenceCounter, never()).decrementReferenceCount();
    primaryFuture.completeExceptionally(expectedException);
    verify(referenceCounter, times(1)).decrementReferenceCount();

    assertThat(resultFuture.isCompletedExceptionally());

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(0)).put(put);
  }

  @Test
  public void testPutWithSecondaryErrorCallsErrorHandler()
      throws ExecutionException, InterruptedException {
    final Put put = createPut("test", "f1", "q1", "v1");
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.put(put)).thenReturn(primaryFuture);
    when(secondaryTable.put(put)).thenReturn(secondaryFuture);

    CompletableFuture<Void> resultFuture = mirroringTable.put(put);
    primaryFuture.complete(null);
    IOException expectedException = new IOException("expected");
    secondaryFuture.completeExceptionally(expectedException);
    resultFuture.get();

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(1)).put(put);
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.PUT), eq(Collections.singletonList(put)), eq(expectedException));
  }

  <T> List<T> waitForAll(List<CompletableFuture<T>> futures) {
    List<T> results = new ArrayList<>(futures.size());
    for (CompletableFuture<T> future : futures) {
      try {
        results.add(future.get());
      } catch (Exception e) {
        results.add(null);
      }
    }
    return results;
  }

  @Test
  public void testEmptyBatch() {
    List<Get> requests = Collections.emptyList();
    when(primaryTable.batch(requests)).thenReturn(Collections.emptyList());

    verify(referenceCounter, never()).decrementReferenceCount();
    verify(referenceCounter, never()).incrementReferenceCount();
    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    verify(referenceCounter, times(1)).decrementReferenceCount();
    verify(referenceCounter, times(1)).incrementReferenceCount();

    assertThat(resultFutures.size()).isEqualTo(0);

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, never()).batch(requests);
    verify(mismatchDetector, never()).batch(any(), any(), any());
    verify(mismatchDetector, never()).batch(any(), any());
  }

  @Test
  public void testBatchGetAndPutGetsAreVerifiedOnSuccess() {
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Get get1 = createGet("get1");

    List<Row> requests = Arrays.asList(new Row[] {put1, get1});

    // op   | p    | s
    // put1 | ok   | ok
    // get1 | ok   | ok

    final Result get1Result = createResult("get1", "value1");

    List<CompletableFuture<Object>> primaryFutures =
        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
    List<CompletableFuture<Object>> secondaryFutures =
        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());

    when(primaryTable.batch(requests)).thenReturn(primaryFutures);
    when(secondaryTable.batch(requests)).thenReturn(secondaryFutures);

    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    primaryFutures.get(0).complete(null);
    primaryFutures.get(1).complete(get1Result);
    secondaryFutures.get(0).complete(null);
    secondaryFutures.get(1).complete(get1Result);
    List<Object> results = waitForAll(resultFutures);
    assertThat(results.size()).isEqualTo(2);

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, times(1)).batch(eq(requests));

    verify(mismatchDetector, times(1))
        .batch(
            Collections.singletonList(get1), new Result[] {get1Result}, new Result[] {get1Result});
    verify(secondaryWriteErrorConsumer, never())
        .consume(eq(HBaseOperation.BATCH), any(Mutation.class), any(Throwable.class));
  }

  @Test
  public void testBatchAllPrimaryFailed()
      throws IOException, InterruptedException, ExecutionException {
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Get get1 = createGet("get1");

    List<Row> requests = Arrays.asList(new Row[] {put1, get1});

    // op   | p    | s
    // put1 | fail | x
    // get1 | fail | x

    final Result get1Result = createResult("get1", "value1");

    List<CompletableFuture<Object>> primaryFutures =
        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());

    when(primaryTable.batch(requests)).thenReturn(primaryFutures);

    verify(referenceCounter, never()).incrementReferenceCount();
    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    verify(referenceCounter, times(1)).incrementReferenceCount();

    IOException ioe = new IOException("expected");
    primaryFutures.get(0).completeExceptionally(ioe);
    verify(referenceCounter, never()).decrementReferenceCount();
    primaryFutures.get(1).completeExceptionally(ioe);
    verify(referenceCounter, times(1)).decrementReferenceCount();

    List<Object> results = waitForAll(resultFutures);
    assertThat(results.size()).isEqualTo(2);

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, never()).batch(anyList());

    verify(mismatchDetector, never()).batch(anyList(), eq(ioe));
    verify(secondaryWriteErrorConsumer, never())
        .consume(eq(HBaseOperation.BATCH), anyList(), any(Throwable.class));
  }

  @Test
  public void testBatchGetAndPut() {
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Put put2 = createPut("test2", "f2", "q2", "v2");
    Put put3 = createPut("test3", "f3", "q3", "v3");
    Get get1 = createGet("get1");
    Get get2 = createGet("get2");
    Get get3 = createGet("get3");

    List<Row> requests = Arrays.asList(new Row[] {put1, put2, put3, get1, get2, get3});
    List<Row> secondaryRequests = Arrays.asList(new Row[] {put1, put3, get1, get3});

    // op   | p    | s
    // put1 | ok   | fail
    // put2 | fail | x
    // put3 | ok   | ok

    // get1 | ok   | fail
    // get2 | fail | x
    // get3 | ok   | ok

    final Result get1Result = createResult("get1", "value1");
    final Result get3Result = createResult("get3", "value3");

    List<CompletableFuture<Object>> primaryFutures =
        Arrays.asList(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>());
    List<CompletableFuture<Object>> secondaryFutures =
        Arrays.asList(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>());

    when(primaryTable.batch(requests)).thenReturn(primaryFutures);
    when(secondaryTable.batch(secondaryRequests)).thenReturn(secondaryFutures);

    verify(referenceCounter, never()).incrementReferenceCount();
    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    verify(referenceCounter, times(1)).incrementReferenceCount();
    IOException ioe = new IOException("expected");

    primaryFutures.get(0).complete(null); // put1 - ok
    primaryFutures.get(1).completeExceptionally(ioe); // put2 - failed
    primaryFutures.get(2).complete(null); // put3 - ok
    primaryFutures.get(3).complete(get1Result); // get1 - ok
    primaryFutures.get(4).completeExceptionally(ioe); // get2 - failed
    primaryFutures.get(5).complete(get3Result); // get3 - ok

    secondaryFutures.get(0).completeExceptionally(ioe); // put1 - failed
    secondaryFutures.get(1).complete(null); // put3 - ok
    secondaryFutures.get(2).completeExceptionally(ioe); // get1 - failed
    verify(referenceCounter, never()).decrementReferenceCount();
    secondaryFutures.get(3).complete(get3Result); // get3 - ok
    verify(referenceCounter, times(1)).decrementReferenceCount();

    List<Object> results = waitForAll(resultFutures);
    assertThat(results.size()).isEqualTo(primaryFutures.size());

    assertThat(results.get(0)).isEqualTo(null); // put1
    assertThat(resultFutures.get(1).isCompletedExceptionally()); // put2
    assertThat(results.get(2)).isEqualTo(null); // put3

    assertThat(results.get(3)).isEqualTo(get1Result);
    assertThat(resultFutures.get(4).isCompletedExceptionally());
    assertThat(results.get(5)).isEqualTo(get3Result);

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests));

    verify(mismatchDetector, times(1))
        .batch(
            eq(Collections.singletonList(get3)),
            eq(new Result[] {get3Result}),
            eq(new Result[] {get3Result}));
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.BATCH), eq(put1), eq(ioe));
  }

  @Test
  public void testBatchGetsPrimaryFailsSecondaryOk() {
    Get get1 = createGet("get1");
    Get get2 = createGet("get2");

    List<Row> requests = Arrays.asList(new Row[] {get1, get2});
    List<Row> secondaryRequests = Arrays.asList(new Row[] {get2});

    // op   | p    | s
    // get1 | fail | x
    // get2 | ok   | ok

    final Result get2Result = createResult("get2", "value2");

    List<CompletableFuture<Object>> primaryFutures =
        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
    List<CompletableFuture<Object>> secondaryFutures =
        Collections.singletonList(new CompletableFuture<>());

    when(primaryTable.batch(requests)).thenReturn(primaryFutures);
    when(secondaryTable.batch(secondaryRequests)).thenReturn(secondaryFutures);

    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    IOException ioe = new IOException("expected");

    primaryFutures.get(0).completeExceptionally(ioe); // get1 - failed
    primaryFutures.get(1).complete(get2Result); // get2 - ok
    secondaryFutures.get(0).complete(get2Result); // get2 - ok

    List<Object> results = waitForAll(resultFutures);
    assertThat(results.size()).isEqualTo(primaryFutures.size());

    assertThat(resultFutures.get(0).isCompletedExceptionally()); // get1
    assertThat(results.get(1)).isEqualTo(get2Result); // put3

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests));

    // successful secondary reads were reported
    verify(mismatchDetector, times(1))
        .batch(
            Collections.singletonList(get2), new Result[] {get2Result}, new Result[] {get2Result});

    // no read errors reported
    verify(mismatchDetector, never()).batch(anyList(), any(IOException.class));
  }

  @Test
  public void testConditionalWriteHappensWhenConditionIsMet()
      throws ExecutionException, InterruptedException {
    Put put = new Put("r1".getBytes());
    CompletableFuture<Boolean> primaryFuture = new CompletableFuture<>();
    when(primaryBuilder.thenPut(put)).thenReturn(primaryFuture);

    verify(referenceCounter, never()).incrementReferenceCount();
    verify(referenceCounter, never()).decrementReferenceCount();
    CompletableFuture<Boolean> resultFuture =
        mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()).thenPut(put);

    verify(referenceCounter, times(1)).incrementReferenceCount();
    primaryFuture.complete(true);
    // The reference count is incremented once at the beginning of checkAndMutate() and then for the
    // second time in writeWithControlFlow().
    // It's done this way so that the reference counting invariant isn't violated when refactoring
    // brittle code around forwarding result of writeWithFlowControl().
    resultFuture.get();
    verify(referenceCounter, times(2)).incrementReferenceCount();
    verify(referenceCounter, times(2)).decrementReferenceCount();

    verify(secondaryTable, times(1)).put(put);
  }

  @Test
  public void testConditionalWriteDoesntHappenWhenConditionIsNotMet()
      throws ExecutionException, InterruptedException {
    Put put = new Put("r1".getBytes());
    CompletableFuture<Boolean> primaryFuture = new CompletableFuture<>();
    when(primaryBuilder.thenPut(put)).thenReturn(primaryFuture);

    CompletableFuture<Boolean> resultFuture =
        mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()).thenPut(put);
    verify(referenceCounter, times(1)).incrementReferenceCount();

    primaryFuture.complete(false);
    verify(referenceCounter, times(1)).decrementReferenceCount();

    resultFuture.get();
    verify(secondaryTable, never()).put(put);
  }

  @Test
  public void testConditionalWriteWhenPrimaryErred()
      throws ExecutionException, InterruptedException {
    Put put = new Put("r1".getBytes());
    CompletableFuture<Boolean> primaryFuture = new CompletableFuture<>();
    IOException ioe = new IOException("expected");

    when(primaryBuilder.thenPut(put)).thenReturn(primaryFuture);
    CompletableFuture<Boolean> resultFuture =
        mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()).thenPut(put);
    verify(referenceCounter, times(1)).incrementReferenceCount();
    verify(referenceCounter, never()).decrementReferenceCount();
    primaryFuture.completeExceptionally(ioe);
    verify(referenceCounter, times(1)).decrementReferenceCount();

    try {
      resultFuture.get();
      fail("should've thrown");
    } catch (ExecutionException e) {
      assertThat(e.getCause()).isEqualTo(ioe);
    }
    verify(secondaryTable, never()).put(any(Put.class));
  }

  @Test
  public void testCheckAndMutateBuilderChainingWhenInPlace() {
    byte[] qual = "q1".getBytes();
    TimeRange timeRange = new TimeRange();

    when(primaryBuilder.ifNotExists()).thenReturn(primaryBuilder);
    when(primaryBuilder.qualifier(any(byte[].class))).thenReturn(primaryBuilder);
    when(primaryBuilder.timeRange(any(TimeRange.class))).thenReturn(primaryBuilder);

    AsyncTable.CheckAndMutateBuilder builder =
        spy(mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()))
            .ifNotExists()
            .qualifier(qual)
            .timeRange(timeRange);

    verify(primaryBuilder, times(1)).ifNotExists();
    verify(primaryBuilder, times(1)).qualifier(qual);
    verify(primaryBuilder, times(1)).timeRange(timeRange);
  }

  @Test
  public void testCheckAndPut() throws ExecutionException, InterruptedException {
    Put put = new Put("r1".getBytes());
    when(primaryBuilder.thenPut(put)).thenReturn(CompletableFuture.completedFuture(true));
    mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()).thenPut(put);
    verify(secondaryTable, times(1)).put(put);
  }

  @Test
  public void testCheckAndDelete() throws ExecutionException, InterruptedException {
    Delete delete = new Delete("r1".getBytes());
    when(primaryBuilder.thenDelete(delete)).thenReturn(CompletableFuture.completedFuture(true));
    mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()).thenDelete(delete);
    verify(secondaryTable, times(1)).delete(delete);
  }

  @Test
  public void testCheckAndMutate() throws ExecutionException, InterruptedException {
    RowMutations mutations = new RowMutations("r1".getBytes());
    when(primaryBuilder.thenMutate(mutations)).thenReturn(CompletableFuture.completedFuture(true));
    mirroringTable.checkAndMutate("r1".getBytes(), "f1".getBytes()).thenMutate(mutations).get();
    verify(secondaryTable, times(1)).mutateRow(mutations);
  }

  @Test
  public void testDelete() throws InterruptedException, ExecutionException {
    Delete delete = new Delete("r1".getBytes());
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.delete(delete)).thenReturn(primaryFuture);
    when(secondaryTable.delete(delete)).thenReturn(secondaryFuture);

    CompletableFuture<Void> resultFuture = mirroringTable.delete(delete);
    primaryFuture.complete(null);
    secondaryFuture.complete(null);
    resultFuture.get();
    verify(secondaryTable, times(1)).delete(delete);
  }

  @Test
  public void testMutateRow() throws ExecutionException, InterruptedException {
    RowMutations mutations = new RowMutations("r1".getBytes());
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.mutateRow(mutations)).thenReturn(primaryFuture);
    when(secondaryTable.mutateRow(mutations)).thenReturn(secondaryFuture);

    CompletableFuture<Void> resultFuture = mirroringTable.mutateRow(mutations);
    primaryFuture.complete(null);
    secondaryFuture.complete(null);
    resultFuture.get();
    verify(secondaryTable, times(1)).mutateRow(mutations);
  }

  private void assertPutsAreEqual(Put expectedPut, Put value) {
    TestHelpers.assertPutsAreEqual(
        expectedPut, value, (a, b) -> CellComparator.getInstance().compare(a, b));
  }

  @Test
  public void testIncrement() throws ExecutionException, InterruptedException {
    Increment increment = new Increment("r1".getBytes());
    Result incrementResult =
        Result.create(
            new Cell[] {
              CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                  .setRow("r1".getBytes())
                  .setFamily("f1".getBytes())
                  .setQualifier("q1".getBytes())
                  .setTimestamp(12)
                  .setType(Cell.Type.Put)
                  .setValue(Longs.toByteArray(142))
                  .build()
            });
    Put expectedPut = OperationUtils.makePutFromResult(incrementResult);

    // increment() and append() modify the reference counter twice to make logic less brittle
    when(primaryTable.increment(any(Increment.class)))
        .thenReturn(CompletableFuture.completedFuture(incrementResult));
    verify(referenceCounter, never()).decrementReferenceCount();
    verify(referenceCounter, never()).incrementReferenceCount();
    mirroringTable.increment(increment).get();
    verify(referenceCounter, times(2)).decrementReferenceCount();
    verify(referenceCounter, times(2)).incrementReferenceCount();
    mirroringTable
        .incrementColumnValue("r1".getBytes(), "f1".getBytes(), "q1".getBytes(), 3L)
        .get();
    verify(referenceCounter, times(4)).decrementReferenceCount();
    verify(referenceCounter, times(4)).incrementReferenceCount();
    mirroringTable
        .incrementColumnValue(
            "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), 3L, Durability.SYNC_WAL)
        .get();
    verify(referenceCounter, times(6)).decrementReferenceCount();
    verify(referenceCounter, times(6)).incrementReferenceCount();

    ArgumentCaptor<Put> argument = ArgumentCaptor.forClass(Put.class);
    verify(secondaryTable, never()).increment(any(Increment.class));
    verify(secondaryTable, times(3)).put(argument.capture());

    assertPutsAreEqual(argument.getAllValues().get(0), expectedPut);
    assertPutsAreEqual(argument.getAllValues().get(1), expectedPut);
    assertPutsAreEqual(argument.getAllValues().get(2), expectedPut);
  }

  @Test
  public void testAppend() throws ExecutionException, InterruptedException {
    Append append = new Append("r1".getBytes());
    Result appendResult =
        Result.create(
            new Cell[] {
              CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                  .setRow("r1".getBytes())
                  .setFamily("f1".getBytes())
                  .setQualifier("q1".getBytes())
                  .setTimestamp(12)
                  .setType(Cell.Type.Put)
                  .setValue(Longs.toByteArray(142))
                  .build()
            });
    Put expectedPut = OperationUtils.makePutFromResult(appendResult);

    when(primaryTable.append(any(Append.class)))
        .thenReturn(CompletableFuture.completedFuture(appendResult));

    // increment() and append() modify the reference counter twice to make logic less brittle
    verify(referenceCounter, never()).decrementReferenceCount();
    verify(referenceCounter, never()).incrementReferenceCount();
    mirroringTable.append(append).get();
    verify(referenceCounter, times(2)).decrementReferenceCount();
    verify(referenceCounter, times(2)).incrementReferenceCount();

    ArgumentCaptor<Put> argument = ArgumentCaptor.forClass(Put.class);
    verify(secondaryTable, times(1)).put(argument.capture());
    assertPutsAreEqual(expectedPut, argument.getValue());

    verify(secondaryTable, never()).append(any(Append.class));
  }

  @Test
  public void testExceptionalFlowControllerAndWriteInBatch()
      throws ExecutionException, InterruptedException {
    IOException flowControllerException = setupFlowControllerToRejectRequests(flowController);
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Put put2 = createPut("test2", "f2", "q2", "v2");
    List<Put> requests = Arrays.asList(put1, put2);

    CompletableFuture<Void> exceptionalFuture = new CompletableFuture<>();
    IOException expectedFuture = new IOException("expected");
    exceptionalFuture.completeExceptionally(expectedFuture);

    List<CompletableFuture<Void>> primaryResults =
        Arrays.asList(exceptionalFuture, CompletableFuture.completedFuture(null));

    when(primaryTable.<Void>batch(requests)).thenReturn(primaryResults);

    List<CompletableFuture<Void>> resultFutures = mirroringTable.batch(requests);
    assertThat(resultFutures.size()).isEqualTo(2);
    assertThat(resultFutures.get(0).isCompletedExceptionally());
    assertThat(resultFutures.get(1).get()).isEqualTo(null);

    verify(secondaryTable, never()).batch(any());
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.BATCH), eq(Arrays.asList(put2)), eq(flowControllerException));
  }

  @Test
  public void testBatchWithAppendsAndIncrements() {
    Increment increment = new Increment("i".getBytes());
    increment.addColumn("f".getBytes(), "q".getBytes(), 1);

    Append append = new Append("a".getBytes());
    append.add("f".getBytes(), "q".getBytes(), "v".getBytes());

    List<? extends Row> operations =
        Arrays.asList(increment, append, createPut("p", "f", "q", "v"), createGet("g"));
    when(primaryTable.batch(operations))
        .thenReturn(
            Arrays.asList(
                CompletableFuture.completedFuture(createResult("i", "f", "q", 1, "1")),
                CompletableFuture.completedFuture(createResult("a", "f", "q", 2, "2")),
                CompletableFuture.completedFuture(new Result()),
                CompletableFuture.completedFuture(createResult("g", "f", "q", 3, "3"))));

    List<? extends Row> expectedSecondaryOperations =
        Arrays.asList(
            createPut("i", "f", "q", 1, "1"),
            createPut("a", "f", "q", 2, "2"),
            createPut("p", "f", "q", "v"),
            createGet("g"));

    mirroringTable.batch(operations);

    verify(primaryTable, times(1)).batch(operations);
    ArgumentCaptor<List<? extends Row>> argumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(secondaryTable, times(1)).batch(argumentCaptor.capture());

    assertPutsAreEqual(
        (Put) argumentCaptor.getValue().get(0), (Put) expectedSecondaryOperations.get(0));
    assertPutsAreEqual(
        (Put) argumentCaptor.getValue().get(1), (Put) expectedSecondaryOperations.get(1));
    assertPutsAreEqual(
        (Put) argumentCaptor.getValue().get(2), (Put) expectedSecondaryOperations.get(2));
    assertThat(argumentCaptor.getValue().get(3)).isEqualTo(expectedSecondaryOperations.get(3));
  }
}
