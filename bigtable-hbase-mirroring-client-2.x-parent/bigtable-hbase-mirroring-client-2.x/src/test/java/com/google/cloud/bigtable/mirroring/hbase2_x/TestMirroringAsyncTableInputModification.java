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

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.blockMethodCall;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGets;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringAsyncTableInputModification {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock AsyncTable<ScanResultConsumerBase> primaryTable;
  @Mock AsyncTable<ScanResultConsumerBase> secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  @Mock ListenableReferenceCounter referenceCounter;

  MirroringAsyncTable<ScanResultConsumerBase> mirroringTable;
  CompletableFuture<Void> letPrimaryThroughFuture;
  SettableFuture<Void> secondaryOperationAllowedFuture;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.letPrimaryThroughFuture = new CompletableFuture<>();
    this.mirroringTable =
        spy(
            new MirroringAsyncTable<>(
                primaryTable,
                secondaryTable,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new MirroringTracer(),
                referenceCounter));

    secondaryOperationAllowedFuture = SettableFuture.create();
    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).exists(anyList());
    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).get(anyList());
    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).put(anyList());
    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).delete(anyList());
    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).batch(anyList());

    lenient().doAnswer(this::answerWithSuccessfulNulls).when(primaryTable).exists(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(primaryTable).get(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(primaryTable).put(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(primaryTable).delete(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(primaryTable).batch(anyList());

    lenient().doAnswer(this::answerWithSuccessfulNulls).when(secondaryTable).exists(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(secondaryTable).get(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(secondaryTable).put(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(secondaryTable).delete(anyList());
    lenient().doAnswer(this::answerWithSuccessfulNulls).when(secondaryTable).batch(anyList());
  }

  private <T> List<CompletableFuture<T>> answerWithSuccessfulNulls(
      InvocationOnMock invocationOnMock) {
    List<?> operations = (List<?>) invocationOnMock.getArguments()[0];
    return operations.stream()
        .map(ignored -> (CompletableFuture<T>) letPrimaryThroughFuture)
        .collect(Collectors.toList());
  }

  @Test
  public void testExists() throws InterruptedException, ExecutionException, TimeoutException {
    List<Get> gets = createGets("k1", "k2", "k3");
    List<Get> inputList = new ArrayList<>(gets);

    CompletableFuture<Boolean> letExistsThroughFuture = new CompletableFuture<>();
    List<CompletableFuture<Boolean>> expectedResult =
        Collections.nCopies(gets.size(), letExistsThroughFuture);

    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).exists(anyList());
    doAnswer(ignored -> expectedResult).when(primaryTable).exists(anyList());
    doAnswer(ignored -> expectedResult).when(secondaryTable).exists(anyList());

    List<CompletableFuture<Boolean>> results = this.mirroringTable.exists(inputList);

    inputList.clear();
    letExistsThroughFuture.complete(true);

    results.get(0).get(3, TimeUnit.SECONDS);
    verify(this.primaryTable, times(1)).exists(gets);

    secondaryOperationAllowedFuture.set(null);
    verify(this.secondaryTable, times(1)).exists(gets);
  }

  @Test
  public void testGet() throws InterruptedException, TimeoutException, ExecutionException {
    List<Get> gets = createGets("k1", "k2", "k3");
    List<Get> inputList = new ArrayList<>(gets);

    List<CompletableFuture<Result>> results = this.mirroringTable.get(inputList);

    inputList.clear();
    letPrimaryThroughFuture.complete(null);

    results.get(0).get(3, TimeUnit.SECONDS);
    verify(this.primaryTable, times(1)).get(gets);

    secondaryOperationAllowedFuture.set(null);
    verify(this.secondaryTable, times(1)).get(gets);
  }

  @Test
  public void testPut() throws InterruptedException, TimeoutException, ExecutionException {
    List<Put> puts = Collections.singletonList(createPut("r", "f", "q", "v"));
    List<Put> inputList = new ArrayList<>(puts);

    List<CompletableFuture<Void>> results = this.mirroringTable.put(inputList);

    inputList.clear();
    letPrimaryThroughFuture.complete(null);

    results.get(0).get(3, TimeUnit.SECONDS);
    verify(this.primaryTable, times(1)).put(puts);

    secondaryOperationAllowedFuture.set(null);
    verify(this.secondaryTable, times(1)).put(puts);
  }

  @Test
  public void testDelete() throws InterruptedException, TimeoutException, ExecutionException {
    List<Delete> puts = Collections.singletonList(new Delete("r".getBytes()));
    List<Delete> inputList = new ArrayList<>(puts);

    List<CompletableFuture<Void>> results = this.mirroringTable.delete(inputList);

    inputList.clear();
    letPrimaryThroughFuture.complete(null);

    results.get(0).get(3, TimeUnit.SECONDS);
    verify(this.primaryTable, times(1)).delete(puts);

    secondaryOperationAllowedFuture.set(null);
    verify(this.primaryTable, times(1)).delete(puts);
  }

  @Test
  public void testBatch() throws InterruptedException, TimeoutException, ExecutionException {
    List<? extends Row> ops = Arrays.asList(new Delete("r".getBytes()), createGet("k"));
    List<? extends Row> inputList = new ArrayList<>(ops);

    List<CompletableFuture<Void>> results = this.mirroringTable.batch(inputList);

    inputList.clear();
    letPrimaryThroughFuture.complete(null);

    results.get(0).get(3, TimeUnit.SECONDS);
    verify(this.primaryTable, times(1)).batch(ops);

    secondaryOperationAllowedFuture.set(null);
    verify(this.secondaryTable, times(1)).batch(ops);
  }
}
