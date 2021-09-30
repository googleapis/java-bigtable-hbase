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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

  private void mockFlowController() {
    FlowController.ResourceReservation resourceReservationMock =
        mock(FlowController.ResourceReservation.class);

    SettableFuture<FlowController.ResourceReservation> resourceReservationFuture =
        SettableFuture.create();
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

  @Test
  public void testMismatchDetectorIsCalledOnGetSingle()
      throws IOException, ExecutionException, InterruptedException {
    mockFlowController();
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
    mockFlowController();
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
    mockFlowController();
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
  public void testSecondaryReadExceptionCallsVerificationErrorHandlerOnExists()
      throws IOException, ExecutionException, InterruptedException {
    mockFlowController();
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

  private Put createPut(String row, String family, String qualifier, String value) {
    Put put = new Put(row.getBytes());
    put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
    return put;
  }

  @Test
  public void testPutIsMirrored() throws IOException, InterruptedException, ExecutionException {
    mockFlowController();
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
    mockFlowController();
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
    mockFlowController();
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

    ArgumentCaptor<List<Row>> argument = ArgumentCaptor.forClass(List.class);
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.PUT), argument.capture());
    assertThat(argument.getValue().size()).isEqualTo(1);
    assertThat(argument.getValue().get(0)).isEqualTo(put);
  }

  @Test
  public void testDelete() throws IOException, InterruptedException, ExecutionException {
    mockFlowController();
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
    mockFlowController();
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
    mockFlowController();
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
    mockFlowController();
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
}
