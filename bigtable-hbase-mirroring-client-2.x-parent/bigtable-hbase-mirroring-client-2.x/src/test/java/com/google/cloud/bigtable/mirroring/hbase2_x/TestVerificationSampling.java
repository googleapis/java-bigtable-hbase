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
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestVerificationSampling {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock AsyncTable<ScanResultConsumerBase> primaryTable;
  @Mock AsyncTable<ScanResultConsumerBase> secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  @Mock ReadSampler readSampler;
  @Mock ListenableReferenceCounter referenceCounter;

  MirroringAsyncTable<ScanResultConsumerBase> mirroringTable;

  Get get = createGet("test");
  List<Get> gets = ImmutableList.of(get);

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
                readSampler,
                referenceCounter,
                executorServiceRule.executorService));
  }

  public <T, R> T mockWithCompleteFuture(T table, R result) {
    return doReturn(CompletableFuture.completedFuture(result)).when(table);
  }

  public <T, R> T mockWithCompleteFutureList(T table, int len, R result) {
    List<CompletableFuture<R>> results = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      results.add(CompletableFuture.completedFuture(result));
    }
    return doReturn(results).when(table);
  }

  public <T> CompletableFuture<Void> allOfList(List<CompletableFuture<T>> list) {
    return CompletableFuture.allOf(list.toArray(new CompletableFuture[0]));
  }

  @Test
  public void isGetSampled() throws ExecutionException, InterruptedException {
    mockWithCompleteFuture(primaryTable, new Result()).get(get);
    mockWithCompleteFuture(secondaryTable, new Result()).get(get);

    withSamplingEnabled(false);
    mirroringTable.get(get).get();
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).get(get);
    verify(secondaryTable, never()).get(get);

    withSamplingEnabled(true);
    mirroringTable.get(get).get();
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).get(get);
    verify(secondaryTable, times(1)).get(get);
  }

  @Test
  public void isGetListSampled() throws ExecutionException, InterruptedException, TimeoutException {
    mockWithCompleteFutureList(primaryTable, gets.size(), new Result()).get(gets);
    mockWithCompleteFutureList(secondaryTable, gets.size(), new Result()).get(gets);

    withSamplingEnabled(false);
    allOfList(mirroringTable.get(gets)).get(1, TimeUnit.SECONDS);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).get(gets);
    verify(secondaryTable, never()).get(gets);

    withSamplingEnabled(true);
    allOfList(mirroringTable.get(gets)).get(1, TimeUnit.SECONDS);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).get(gets);
    verify(secondaryTable, times(1)).get(gets);
  }

  @Test
  public void isExistsSampled() throws ExecutionException, InterruptedException, TimeoutException {
    mockWithCompleteFuture(primaryTable, Boolean.TRUE).exists(get);
    mockWithCompleteFuture(secondaryTable, Boolean.TRUE).exists(get);

    withSamplingEnabled(false);
    mirroringTable.exists(get).get(1, TimeUnit.SECONDS);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).exists(get);
    verify(secondaryTable, never()).exists(get);

    withSamplingEnabled(true);
    mirroringTable.exists(get).get(1, TimeUnit.SECONDS);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).exists(get);
    verify(secondaryTable, times(1)).exists(get);
  }

  @Test
  public void isExistsAllSampled()
      throws ExecutionException, InterruptedException, TimeoutException {
    mockWithCompleteFutureList(primaryTable, gets.size(), Boolean.TRUE).exists(gets);
    mockWithCompleteFutureList(secondaryTable, gets.size(), Boolean.TRUE).exists(gets);

    withSamplingEnabled(false);
    allOfList(mirroringTable.exists(gets)).get(1, TimeUnit.SECONDS);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).exists(gets);
    verify(secondaryTable, never()).exists(gets);

    withSamplingEnabled(true);
    allOfList(mirroringTable.exists(gets)).get(1, TimeUnit.SECONDS);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).exists(gets);
    verify(secondaryTable, times(1)).exists(gets);
  }

  @Test
  public void isBatchSampled() throws InterruptedException, ExecutionException, TimeoutException {
    Put put = createPut("test", "test", "test", "test");
    List<? extends Row> ops = ImmutableList.of(get, put);
    mockWithCompleteFutureList(primaryTable, 2, new Result()).batch(ops);
    mockWithCompleteFutureList(secondaryTable, 2, new Result()).batch(ops);

    withSamplingEnabled(false);
    allOfList(mirroringTable.batch(ops)).get(1, TimeUnit.SECONDS);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).batch(ops);

    withSamplingEnabled(true);
    allOfList(mirroringTable.batch(ops)).get(1, TimeUnit.SECONDS);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).batch(ops);
    verify(secondaryTable, times(1)).batch(ops);
    verify(secondaryTable, times(1)).batch(ImmutableList.of(put));
  }

  @Test
  public void isResultScannerSampled() {
    mirroringTable.getScanner(new Scan());
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
  }

  @Test
  public void testResultScannerWithSampling() throws IOException {
    ResultScanner primaryScanner = mock(ResultScanner.class);
    ResultScanner secondaryScanner = mock(ResultScanner.class);
    doReturn(primaryScanner).when(primaryTable).getScanner(any(Scan.class));
    doReturn(secondaryScanner).when(secondaryTable).getScanner(any(Scan.class));

    withSamplingEnabled(true);

    ResultScanner s = mirroringTable.getScanner(new Scan());
    s.next();
    executorServiceRule.waitForExecutor();
    verify(primaryScanner, times(1)).next();
    verify(secondaryScanner, times(1)).next();
  }

  @Test
  public void testResultScannerWithoutSampling() throws IOException {
    ResultScanner primaryScanner = mock(ResultScanner.class);
    ResultScanner secondaryScanner = mock(ResultScanner.class);
    doReturn(primaryScanner).when(primaryTable).getScanner(any(Scan.class));
    doReturn(secondaryScanner).when(secondaryTable).getScanner(any(Scan.class));

    withSamplingEnabled(false);

    ResultScanner s = mirroringTable.getScanner(new Scan());
    s.next();
    executorServiceRule.waitForExecutor();
    verify(primaryScanner, times(1)).next();
    verify(secondaryScanner, never()).next();
  }

  private void withSamplingEnabled(boolean b) {
    doReturn(b).when(readSampler).shouldNextReadOperationBeSampled();
  }
}
