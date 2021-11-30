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
package com.google.cloud.bigtable.mirroring.core;

import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestVerificationSampling {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  Timestamper timestamper = new NoopTimestamper();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  @Mock ReadSampler readSampler;

  MirroringTable mirroringTable;

  Get get = createGet("test");
  List<Get> gets = ImmutableList.of(get);

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                readSampler,
                timestamper,
                false,
                false,
                new MirroringTracer(),
                mock(ReferenceCounter.class),
                10));
  }

  @Test
  public void isGetSampled() throws IOException {
    withSamplingEnabled(false);
    mirroringTable.get(get);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).get(get);
    verify(secondaryTable, never()).get(get);

    withSamplingEnabled(true);
    mirroringTable.get(get);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).get(get);
    verify(secondaryTable, times(1)).get(get);
  }

  @Test
  public void isGetListSampled() throws IOException {
    doReturn(new Result[] {createResult("k")})
        .when(primaryTable)
        .get(ArgumentMatchers.<Get>anyList());

    withSamplingEnabled(false);
    mirroringTable.get(gets);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).get(gets);
    verify(secondaryTable, never()).get(gets);

    withSamplingEnabled(true);
    mirroringTable.get(gets);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).get(gets);
    verify(secondaryTable, times(1)).get(gets);
  }

  @Test
  public void isExistsSampled() throws IOException {
    withSamplingEnabled(false);
    mirroringTable.exists(get);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).exists(get);
    verify(secondaryTable, never()).exists(get);

    withSamplingEnabled(true);
    mirroringTable.exists(get);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).exists(get);
    verify(secondaryTable, times(1)).exists(get);
  }

  @Test
  public void isExistsAllSampled() throws IOException {
    doReturn(new boolean[] {true}).when(primaryTable).existsAll(ArgumentMatchers.<Get>anyList());

    withSamplingEnabled(false);
    mirroringTable.existsAll(gets);
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).existsAll(gets);
    verify(secondaryTable, never()).existsAll(gets);

    withSamplingEnabled(true);
    mirroringTable.existsAll(gets);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(2)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(2)).existsAll(gets);
    verify(secondaryTable, times(1)).existsAll(gets);
  }

  @Test
  public void isBatchSampledWithSamplingEnabled() throws IOException, InterruptedException {
    Put put = createPut("test", "test", "test", "test");
    List<? extends Row> ops = ImmutableList.of(get, put);

    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];
                result[0] = Result.create(new Cell[0]);
                result[1] = Result.create(new Cell[0]);
                return null;
              }
            })
        .when(primaryTable)
        .batch(eq(ops), any(Object[].class));

    withSamplingEnabled(true);
    mirroringTable.batch(ops);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).batch(eq(ops), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(ops), any(Object[].class));
  }

  @Test
  public void isBatchSampledWithSamplingDisabled() throws IOException, InterruptedException {
    Put put = createPut("test", "test", "test", "test");
    List<? extends Row> ops = ImmutableList.of(get, put);

    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];
                result[0] = Result.create(new Cell[0]);
                result[1] = Result.create(new Cell[0]);
                return null;
              }
            })
        .when(primaryTable)
        .batch(eq(ops), any(Object[].class));

    withSamplingEnabled(false);
    mirroringTable.batch(ops);
    executorServiceRule.waitForExecutor();
    verify(readSampler, times(1)).shouldNextReadOperationBeSampled();
    verify(primaryTable, times(1)).batch(eq(ops), any(Object[].class));
    verify(secondaryTable, times(1)).batch(eq(ImmutableList.of(put)), any(Object[].class));
  }

  @Test
  public void isResultScannerSampled() throws IOException {
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
