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
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import org.apache.hadoop.hbase.Cell;
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
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestMirroringAsyncTableInputModification {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock AsyncTable<ScanResultConsumerBase> primaryTable;
  @Mock AsyncTable<ScanResultConsumerBase> secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;

  MirroringAsyncTable<ScanResultConsumerBase> mirroringTable;
  SettableFuture<Void> secondaryOperationAllowedFuture;

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
                new MirroringTracer()));

    secondaryOperationAllowedFuture = SettableFuture.create();
    blockMethodCall(secondaryTable, secondaryOperationAllowedFuture).batch(anyList());

    mockBatch(this.primaryTable);
    mockBatch(this.secondaryTable);
  }

  @Test
  public void testExists() throws InterruptedException, ExecutionException, TimeoutException {
    List<Get> gets = createGets("k1", "k2", "k3");
    List<Get> inputList = new ArrayList<>(gets);

    List<CompletableFuture<Boolean>> results = this.mirroringTable.exists(inputList);

    verifyWithInputModification(secondaryOperationAllowedFuture, gets, inputList, results);
  }

  @Test
  public void testGet() throws InterruptedException, TimeoutException, ExecutionException {
    List<Get> gets = createGets("k1", "k2", "k3");
    List<Get> inputList = new ArrayList<>(gets);

    List<CompletableFuture<Result>> results = this.mirroringTable.get(inputList);

    verifyWithInputModification(secondaryOperationAllowedFuture, gets, inputList, results);
  }

  @Test
  public void testPut() throws InterruptedException, TimeoutException, ExecutionException {
    List<Put> puts = Collections.singletonList(createPut("r", "f", "q", "v"));
    List<Put> inputList = new ArrayList<>(puts);

    List<CompletableFuture<Void>> results = this.mirroringTable.put(inputList);

    verifyWithInputModification(secondaryOperationAllowedFuture, puts, inputList, results);
  }

  @Test
  public void testDelete() throws InterruptedException, TimeoutException, ExecutionException {
    List<Delete> puts = Collections.singletonList(new Delete("r".getBytes()));
    List<Delete> inputList = new ArrayList<>(puts);

    List<CompletableFuture<Void>> results = this.mirroringTable.delete(inputList);

    verifyWithInputModification(secondaryOperationAllowedFuture, puts, inputList, results);
  }

  @Test
  public void testBatch() throws InterruptedException, TimeoutException, ExecutionException {
    List<? extends Row> ops = Arrays.asList(new Delete("r".getBytes()), createGet("k"));
    List<? extends Row> inputList = new ArrayList<>(ops);

    List<CompletableFuture<Void>> results = this.mirroringTable.batch(inputList);

    verifyWithInputModification(secondaryOperationAllowedFuture, ops, inputList, results);
  }

  private <T> void verifyWithInputModification(
      SettableFuture<Void> secondaryOperationAllowedFuture,
      List<? extends Row> ops,
      List<? extends Row> inputList,
      List<CompletableFuture<T>> results)
      throws InterruptedException, ExecutionException, TimeoutException {

    results.get(0).get(3, TimeUnit.SECONDS);
    verify(this.primaryTable, times(1)).batch(ops);
    inputList.clear(); // User modifies the list

    secondaryOperationAllowedFuture.set(null);
    verify(this.secondaryTable, times(1)).batch(ops);
  }

  void mockBatch(AsyncTable<?> table) {
    doAnswer(
            (Answer<List<CompletableFuture<Result>>>)
                invocationOnMock -> {
                  Object[] args = invocationOnMock.getArguments();
                  List<? extends Row> operations = (List<? extends Row>) args[0];
                  List<CompletableFuture<Result>> results = new ArrayList<>();

                  for (Row operation : operations) {
                    if (operation instanceof Get) {
                      Get get = (Get) operation;
                      CompletableFuture<Result> result = new CompletableFuture<>();
                      result.complete(createResult(get.getRow(), get.getRow()));
                      results.add(result);
                    } else {
                      CompletableFuture<Result> result = new CompletableFuture<>();
                      result.complete(Result.create(new Cell[0]));
                      results.add(result);
                    }
                  }
                  return results;
                })
        .when(table)
        .batch(ArgumentMatchers.anyList());
  }
}
