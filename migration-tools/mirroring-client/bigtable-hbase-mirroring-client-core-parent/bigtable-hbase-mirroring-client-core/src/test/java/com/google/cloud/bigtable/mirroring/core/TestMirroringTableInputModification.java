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

import static com.google.cloud.bigtable.mirroring.core.TestHelpers.blockMethodCall;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createGets;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
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
public class TestMirroringTableInputModification {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  Timestamper timestamper = new NoopTimestamper();

  MirroringTable mirroringTable;
  SettableFuture<Void> secondaryOperationBlockingFuture;

  @Before
  public void setUp() throws IOException, InterruptedException {
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
                new ReadSampler(100),
                timestamper,
                false,
                false,
                new MirroringTracer(),
                mock(ReferenceCounter.class),
                10));

    this.secondaryOperationBlockingFuture = SettableFuture.create();

    mockExistsAll(this.primaryTable);
    mockGet(this.primaryTable);
    mockBatch(this.primaryTable);

    secondaryOperationBlockingFuture = SettableFuture.create();

    blockMethodCall(secondaryTable, secondaryOperationBlockingFuture)
        .existsAll(ArgumentMatchers.<Get>anyList());
    blockMethodCall(this.secondaryTable, secondaryOperationBlockingFuture)
        .batch(ArgumentMatchers.<Put>anyList(), (Object[]) any());
    blockMethodCall(this.secondaryTable, secondaryOperationBlockingFuture)
        .get(ArgumentMatchers.<Get>anyList());
  }

  @Test
  public void testExistsAll() throws IOException {
    mockExistsAll(this.primaryTable);
    blockMethodCall(secondaryTable, secondaryOperationBlockingFuture)
        .existsAll(ArgumentMatchers.<Get>anyList());

    List<Get> gets = createGets("k1", "k2", "k3");
    List<Get> inputList = new ArrayList<>(gets);

    this.mirroringTable.existsAll(inputList);
    verify(this.primaryTable, times(1)).existsAll(inputList);
    inputList.clear(); // User modifies the list

    secondaryOperationBlockingFuture.set(null);
    executorServiceRule.waitForExecutor();

    verify(this.secondaryTable, times(1)).existsAll(gets);
  }

  @Test
  public void testGet() throws IOException {
    mockGet(this.primaryTable);
    blockMethodCall(this.secondaryTable, secondaryOperationBlockingFuture)
        .get(ArgumentMatchers.<Get>anyList());

    List<Get> gets = createGets("k1", "k2", "k3");
    List<Get> inputList = new ArrayList<>(gets);

    this.mirroringTable.get(inputList);
    verify(this.primaryTable, times(1)).get(inputList);
    inputList.clear(); // User modifies the list

    secondaryOperationBlockingFuture.set(null);
    executorServiceRule.waitForExecutor();

    verify(this.secondaryTable, times(1)).get(gets);
  }

  @Test
  public void testPut() throws IOException, InterruptedException {
    mockBatch(this.primaryTable);
    blockMethodCall(this.secondaryTable, secondaryOperationBlockingFuture)
        .batch(ArgumentMatchers.<Put>anyList(), (Object[]) any());

    List<Put> puts = Collections.singletonList(createPut("r", "f", "q", "v"));
    List<Put> inputList = new ArrayList<>(puts);

    this.mirroringTable.put(inputList);
    verify(this.primaryTable, times(1)).batch(eq(inputList), (Object[]) any());
    inputList.clear(); // User modifies the list

    secondaryOperationBlockingFuture.set(null);
    executorServiceRule.waitForExecutor();

    verify(this.secondaryTable, times(1)).batch(eq(puts), (Object[]) any());
  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    mockBatch(this.primaryTable);
    blockMethodCall(this.secondaryTable, secondaryOperationBlockingFuture)
        .batch(ArgumentMatchers.<Put>anyList(), (Object[]) any());

    List<Delete> puts = Collections.singletonList(new Delete("r".getBytes()));
    List<Delete> inputList = new ArrayList<>(puts);

    this.mirroringTable.delete(inputList); // inputList is modified by the call
    verify(this.primaryTable, times(1)).batch(eq(puts), (Object[]) any());

    secondaryOperationBlockingFuture.set(null);
    executorServiceRule.waitForExecutor();

    verify(this.secondaryTable, times(1)).batch(eq(puts), (Object[]) any());
  }

  @Test
  public void testBatch() throws IOException, InterruptedException {
    mockBatch(this.primaryTable);
    blockMethodCall(this.secondaryTable, secondaryOperationBlockingFuture)
        .batch(ArgumentMatchers.<Put>anyList(), (Object[]) any());

    List<? extends Row> ops = Arrays.asList(new Delete("r".getBytes()), createGet("k"));
    List<? extends Row> inputList = new ArrayList<>(ops);

    this.mirroringTable.batch(inputList);
    verify(this.primaryTable, times(1)).batch(eq(ops), (Object[]) any());
    inputList.clear(); // User modifies the list

    secondaryOperationBlockingFuture.set(null);
    executorServiceRule.waitForExecutor();

    verify(this.secondaryTable, times(1)).batch(eq(ops), (Object[]) any());
  }

  private void mockGet(Table table) throws IOException {
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                List<Get> gets = invocationOnMock.getArgument(0);
                Result[] results = new Result[gets.size()];
                for (int i = 0; i < gets.size(); i++) {
                  results[i] = createResult(gets.get(i).getRow(), gets.get(i).getRow());
                }
                return results;
              }
            })
        .when(table)
        .get(ArgumentMatchers.<Get>anyList());
  }

  private void mockExistsAll(Table table) throws IOException {
    doAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                List<Get> gets = invocationOnMock.getArgument(0);
                return new boolean[gets.size()];
              }
            })
        .when(table)
        .existsAll(ArgumentMatchers.<Get>anyList());
  }

  private void mockBatch(Table table) throws IOException, InterruptedException {
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                List<? extends Row> operations = (List<? extends Row>) args[0];
                Object[] result = (Object[]) args[1];

                for (int i = 0; i < operations.size(); i++) {
                  Row operation = operations.get(i);
                  if (operation instanceof Get) {
                    Get get = (Get) operation;
                    result[i] = createResult(get.getRow(), get.getRow());
                  } else {
                    result[i] = Result.create(new Cell[0]);
                  }
                }
                return null;
              }
            })
        .when(table)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }
}
