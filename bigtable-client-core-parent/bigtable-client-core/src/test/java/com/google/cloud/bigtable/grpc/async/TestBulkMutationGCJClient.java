/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestBulkMutationGCJClient {

  @Rule
  public ExpectedException expect = ExpectedException.none();

  @Mock private UnaryCallable<RowMutation, Void> callable;
  private BulkMutationGCJClient bulkMutationClient;
  private SettableApiFuture<Void> future;
  private RowMutation rowMutation = RowMutation.create("fake-table", "fake-key");
  private final BulkMutationBatcher batcher = new BulkMutationBatcher(new BatcherUnaryCallable());

  @Before
  public void setUp() {
    BulkMutationBatcher bulkMutationBatcher = new BulkMutationBatcher(callable);
    bulkMutationClient = new BulkMutationGCJClient(bulkMutationBatcher);
    future = SettableApiFuture.create();
  }

  @Test
  public void testAdd() {
    when(callable.futureCall(rowMutation)).thenReturn(future);
    ApiFuture<Void> result = bulkMutationClient.add(rowMutation);
    Assert.assertFalse(result.isDone());
    future.set(null);
    Assert.assertTrue(result.isDone());
    verify(callable).futureCall(rowMutation);
  }

  @Test(expected = ExecutionException.class)
  public void testAddFailure() throws Exception{
    String message = "can not perform mutation";
    when(callable.futureCall(rowMutation)).thenReturn(future);
    future.setException(new RuntimeException(message));
    ApiFuture<Void> result = bulkMutationClient.add(rowMutation);
    Assert.assertTrue(result.isDone());
    result.get();
    expect.expectMessage(message);
    verify(callable).futureCall(rowMutation);
  }

  @Test
  public void testCallableTooFewStatuses() {
    RowMutation rowMutationOther = RowMutation.create("table", "key");
    SettableApiFuture<Void> futureOther = SettableApiFuture.create();
    when(callable.futureCall(rowMutation)).thenReturn(future);
    when(callable.futureCall(rowMutationOther)).thenReturn(futureOther);

    ApiFuture<Void> result = bulkMutationClient.add(rowMutation);
    ApiFuture<Void> resultOther = bulkMutationClient.add(rowMutationOther);
    // Only resolving one response
    future.set(null);

    Assert.assertTrue(result.isDone());
    Assert.assertFalse(resultOther.isDone());

    Assert.assertFalse(bulkMutationClient.isFlushed());
    futureOther.set(null);
    Assert.assertTrue(bulkMutationClient.isFlushed());

    verify(callable).futureCall(rowMutation);
    verify(callable).futureCall(rowMutationOther);
  }

  @Test
  public void testConcurrentBatches() throws Exception {
    final List<ApiFuture<Void>> futures =
        Collections.synchronizedList(new ArrayList<ApiFuture<Void>>());
    final int batchCount = 10;
    final int concurrentBulkMutationCount = 50;

    Runnable r = new Runnable() {
      @Override
      public void run() {
        IBulkMutation bulkMutation = new BulkMutationGCJClient(batcher);
        for (int i = 0; i < batchCount * 10; i++) {
          futures.add(bulkMutation.add(RowMutation.create("fake-table", "fake-key")));
        }
        bulkMutation.sendUnsent();
      }
    };
    ExecutorService pool = Executors.newFixedThreadPool(100);
    for (int i = 0; i < concurrentBulkMutationCount; i++) {
      pool.execute(r);
    }
    pool.shutdown();
    pool.awaitTermination(100, TimeUnit.SECONDS);

    for (ApiFuture<Void> future : futures) {
      Assert.assertTrue(future.isDone());
    }
    pool.shutdownNow();
  }

  class BatcherUnaryCallable extends UnaryCallable<RowMutation, Void> {
    @Override
    public ApiFuture<Void> futureCall(RowMutation rowMutation, ApiCallContext apiCallContext) {
      return ApiFutures.immediateFuture(null);
    }
  }

  @Test
  public void testIsFlush() {
    when(callable.futureCall(rowMutation)).thenReturn(future);
    bulkMutationClient.add(rowMutation);
    Assert.assertFalse("BulkMutation should have one pending element",
        bulkMutationClient.isFlushed());
    future.set(null);
    Assert.assertTrue("BulkMutation should not have any pending element",
        bulkMutationClient.isFlushed());
    verify(callable).futureCall(rowMutation);
  }

  @Test
  public void testFlush() {
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    final SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(callable.futureCall(rowMutation)).thenReturn(future).thenReturn(future2);
    ApiFuture<Void> result1 = bulkMutationClient.add(rowMutation);
    ApiFuture<Void> result2 = bulkMutationClient.add(rowMutation);

    Assert.assertFalse(result1.isDone());
    Assert.assertFalse(result2.isDone());
    scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        future.set(null);
        future2.set(null);
      }
    }, 50, TimeUnit.MILLISECONDS);

    bulkMutationClient.sendUnsent();
    Assert.assertTrue(result1.isDone());
    Assert.assertTrue(result2.isDone());

    verify(callable, times(2)).futureCall(rowMutation);
  }
}
