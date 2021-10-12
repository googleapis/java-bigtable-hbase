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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringAsyncTable {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock AsyncTable primaryTable;
  @Mock AsyncTable secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;

  MirroringAsyncTable<ScanResultConsumerBase> mirroringTable;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringTable =
        spy(
            new MirroringAsyncTable<ScanResultConsumerBase>(
                primaryTable,
                secondaryTable,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new MirroringTracer()));
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetSingle()
      throws IOException, ExecutionException, InterruptedException {
    Get get = createGets("test").get(0);
    Result expectedResult = createResult("test", "value");
    CompletableFuture<Result> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Result> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.get(get)).thenReturn(primaryFuture);
    when(secondaryTable.get(get)).thenReturn(secondaryFuture);

    CompletableFuture<Result> resultFuture = mirroringTable.get(get);
    primaryFuture.complete(expectedResult);
    secondaryFuture.complete(expectedResult);
    Result result = resultFuture.get();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedResult, expectedResult);
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never())
        .get(ArgumentMatchers.<Get>anyList(), any(Result[].class), any(Result[].class));
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnSingleGet()
      throws IOException, ExecutionException, InterruptedException {
    Get get = createGet("test");
    Result expectedResult = createResult("test", "value");
    CompletableFuture<Result> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Result> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.get(get)).thenReturn(primaryFuture);
    when(secondaryTable.get(get)).thenReturn(secondaryFuture);

    IOException expectedException = new IOException("expected");
    CompletableFuture<Throwable> exceptionalFuture = new CompletableFuture<Throwable>();

    CompletableFuture<Result> resultFuture = mirroringTable.get(get);
    primaryFuture.complete(expectedResult);
    secondaryFuture.completeExceptionally(expectedException);
    Result result = resultFuture.get();

    assertThat(result).isEqualTo(expectedResult);

    verify(mismatchDetector, times(1)).get(get, expectedException);
  }

  @Test
  public void testMismatchDetectorIsCalledOnExists()
      throws IOException, ExecutionException, InterruptedException {
    Get get = createGet("test");
    boolean expectedResult = true;
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
    verify(mismatchDetector, never()).exists((Get) any(), (Throwable) any());
  }

  @Test
  public void testMismatchDetectorIsCalledOnGetMultiple()
      throws IOException, ExecutionException, InterruptedException {
    List<Get> get = createGets("test");
    Result[] expectedResultArray = {createResult("test", "value")};
    CompletableFuture<Result> expectedFuture = new CompletableFuture<Result>();
    List<CompletableFuture<Result>> expectedResultFutureList = Arrays.asList(expectedFuture);

    when(primaryTable.batch(get)).thenReturn(expectedResultFutureList);
    when(secondaryTable.batch(get)).thenReturn(expectedResultFutureList);

    List<CompletableFuture<Result>> resultFutures = mirroringTable.get(get);
    assertThat(resultFutures.size()).isEqualTo(1);

    expectedFuture.complete(expectedResultArray[0]);
    Result result = resultFutures.get(0).get();
    assertThat(result).isEqualTo(expectedResultArray[0]);

    verify(mismatchDetector, times(1))
        .batch(eq(get), eq(expectedResultArray), eq(expectedResultArray));
    verify(mismatchDetector, never()).batch((List<Get>) any(), (Throwable) any());
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never()).get((Get) any(), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnGetMultiple()
      throws IOException, ExecutionException, InterruptedException {
    List<Get> get = createGets("test1", "test2");
    Result[] expectedResultArray = {
      createResult("test1", "value1"), createResult("test2", "value2")
    };
    CompletableFuture<Result> expectedFuture1 = new CompletableFuture<Result>();
    CompletableFuture<Result> expectedFuture2 = new CompletableFuture<Result>();
    CompletableFuture<Result> exceptionalFuture = new CompletableFuture<Result>();
    List<CompletableFuture<Result>> expectedResultFutureList =
        Arrays.asList(expectedFuture1, expectedFuture2);
    List<CompletableFuture<Result>> exceptionalResultFutureList =
        Arrays.asList(exceptionalFuture, exceptionalFuture);

    when(primaryTable.batch(get)).thenReturn(expectedResultFutureList);
    when(secondaryTable.batch(get)).thenReturn(exceptionalResultFutureList);

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

    verify(mismatchDetector, never()).batch((List<Get>) any(), (Result[]) any(), (Result[]) any());
    verify(mismatchDetector, never()).get((Get) any(), (Throwable) any());
    verify(mismatchDetector, never()).get((Get) any(), (Result) any(), (Result) any());
  }

  @Test
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExists()
      throws IOException, ExecutionException, InterruptedException {
    Get get = createGet("test");
    boolean expectedResult = true;
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
  public void testPutIsMirrored() throws IOException, InterruptedException, ExecutionException {
    Put put = createPut("test", "f1", "q1", "v1");
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryFuture = new CompletableFuture<>();
    when(primaryTable.put(put)).thenReturn(primaryFuture);
    when(secondaryTable.put(put)).thenReturn(secondaryFuture);

    CompletableFuture<Void> resultFuture = mirroringTable.put(put);
    primaryFuture.complete(null);
    secondaryFuture.complete(null);
    resultFuture.get();

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(1)).put(put);
  }

  @Test
  public void testPutWithErrorIsNotMirrored() throws IOException {
    final Put put = createPut("test", "f1", "q1", "v1");
    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    when(primaryTable.put(put)).thenReturn(primaryFuture);

    CompletableFuture<Void> resultFuture = mirroringTable.put(put);

    IOException expectedException = new IOException("expected");
    primaryFuture.completeExceptionally(expectedException);

    assertThat(resultFuture.isCompletedExceptionally());

    verify(primaryTable, times(1)).put(put);
    verify(secondaryTable, times(0)).put(put);
  }

  @Test
  public void testPutWithSecondaryErrorCallsErrorHandler()
      throws IOException, ExecutionException, InterruptedException {
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
        .consume(HBaseOperation.PUT, Collections.singletonList(put));
  }

  <T> List<T> waitForAll(List<CompletableFuture<T>> futures) {
    List<T> results = new ArrayList<>(futures.size());
    int numberOfFutures = futures.size();
    for (int i = 0; i < numberOfFutures; i++) {
      try {
        results.add(futures.get(i).get());
      } catch (Exception e) {
        results.add(null);
      }
    }
    return results;
  }

  @Test
  public void testEmptyBatch() {
    List<Get> requests = Arrays.asList();
    when(primaryTable.batch(requests)).thenReturn(Arrays.asList());

    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    assertThat(resultFutures.size()).isEqualTo(0);

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, never()).batch(requests);
    verify(mismatchDetector, never()).batch((List<Get>) any(), (Result[]) any(), (Result[]) any());
    verify(mismatchDetector, never()).batch((List<Get>) any(), (Throwable) any());
  }

  @Test
  public void testBatchGetAndPutGetsAreVerifiedOnSuccess()
      throws IOException, InterruptedException, ExecutionException {
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Get get1 = createGet("get1");

    List<Row> requests = Arrays.asList(new Row[] {put1, get1});
    List<Row> secondaryRequests = requests;

    // op   | p    | s
    // put1 | ok   | ok
    // get1 | ok   | ok

    final Result get1Result = createResult("get1", "value1");

    List<CompletableFuture<Object>> primaryFutures =
        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
    List<CompletableFuture<Object>> secondaryFutures =
        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());

    when(primaryTable.batch(requests)).thenReturn(primaryFutures);
    when(secondaryTable.batch(secondaryRequests)).thenReturn(secondaryFutures);

    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
    primaryFutures.get(0).complete(null);
    primaryFutures.get(1).complete(get1Result);
    secondaryFutures.get(0).complete(null);
    secondaryFutures.get(1).complete(get1Result);
    List<Object> results = waitForAll(resultFutures);
    assertThat(results.size()).isEqualTo(2);

    verify(primaryTable, times(1)).batch(requests);
    verify(secondaryTable, times(1)).batch(eq(secondaryRequests));

    verify(mismatchDetector, times(1))
        .batch(Arrays.asList(get1), new Result[] {get1Result}, new Result[] {get1Result});
    verify(secondaryWriteErrorConsumer, never())
        .consume(eq(HBaseOperation.BATCH), (List<? extends Row>) any());
  }

  @Test
  public void testBatchGetAndPut() throws IOException, InterruptedException, ExecutionException {
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

    List<CompletableFuture<Object>> resultFutures = mirroringTable.batch(requests);
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
    secondaryFutures.get(3).complete(get3Result); // get3 - ok

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
            eq(Arrays.asList(get3)), eq(new Result[] {get3Result}), eq(new Result[] {get3Result}));
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(HBaseOperation.BATCH, Arrays.asList(put1));
  }

  @Test
  public void testBatchGetsPrimaryFailsSecondaryOk()
      throws IOException, InterruptedException, ExecutionException {
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
    List<CompletableFuture<Object>> secondaryFutures = Arrays.asList(new CompletableFuture<>());

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
        .batch(Arrays.asList(get2), new Result[] {get2Result}, new Result[] {get2Result});

    // no read errors reported
    verify(mismatchDetector, never())
        .batch(ArgumentMatchers.<Get>anyList(), any(IOException.class));
  }

  @Test
  public void testDelete() throws IOException, InterruptedException, ExecutionException {
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
  public void testMutateRow() throws IOException, ExecutionException, InterruptedException {
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

  @Test
  public void testIncrement() throws IOException, ExecutionException, InterruptedException {
    Increment increment = new Increment("r1".getBytes());
    Result incrementResult =
        Result.create(
            new Cell[] {
              CellUtil.createCell(
                  "r1".getBytes(),
                  "f1".getBytes(),
                  "q1".getBytes(),
                  12,
                  KeyValue.Type.Put.getCode(),
                  Longs.toByteArray(142))
            });

    when(primaryTable.increment(any(Increment.class)))
        .thenReturn(CompletableFuture.completedFuture(incrementResult));
    mirroringTable.increment(increment).get();
    mirroringTable
        .incrementColumnValue("r1".getBytes(), "f1".getBytes(), "q1".getBytes(), 3L)
        .get();
    mirroringTable
        .incrementColumnValue(
            "r1".getBytes(), "f1".getBytes(), "q1".getBytes(), 3L, Durability.SYNC_WAL)
        .get();

    ArgumentCaptor<Increment> argument = ArgumentCaptor.forClass(Increment.class);
    verify(secondaryTable, times(3)).increment(argument.capture());
    assertThat(argument.getAllValues().get(0)).isEqualTo(increment);
  }

  @Test
  public void testAppend() throws IOException, ExecutionException, InterruptedException {
    Append append = new Append("r1".getBytes());
    Result appendResult =
        Result.create(
            new Cell[] {
              CellUtil.createCell(
                  "r1".getBytes(),
                  "f1".getBytes(),
                  "q1".getBytes(),
                  12,
                  KeyValue.Type.Put.getCode(),
                  Longs.toByteArray(142))
            });
    when(primaryTable.append(any(Append.class)))
        .thenReturn(CompletableFuture.completedFuture(appendResult));
    mirroringTable.append(append).get();

    verify(secondaryTable, times(1)).append(append);
  }

  @Test
  public void TestExceptionalFlowControllerAndWriteInBatch()
      throws ExecutionException, InterruptedException {
    setupFlowControllerToRejectRequests(flowController);
    Put put1 = createPut("test1", "f1", "q1", "v1");
    Put put2 = createPut("test2", "f2", "q2", "v2");
    List<Put> requests = Arrays.asList(new Put[] {put1, put2});

    CompletableFuture<Void> exceptionalFuture = new CompletableFuture<>();
    exceptionalFuture.completeExceptionally(new IOException("expected"));

    List<CompletableFuture<Void>> primaryResults =
        Arrays.asList(exceptionalFuture, CompletableFuture.completedFuture(null));

    when(primaryTable.batch(requests)).thenReturn(primaryResults);

    List<CompletableFuture<Void>> resultFutures = mirroringTable.batch(requests);
    assertThat(resultFutures.size()).isEqualTo(2);
    assertThat(resultFutures.get(0).isCompletedExceptionally());
    assertThat(resultFutures.get(1).get()).isEqualTo(null);

    verify(secondaryTable, never()).batch((List<Put>) any());
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(HBaseOperation.BATCH, Arrays.asList(put2));
  }
}
