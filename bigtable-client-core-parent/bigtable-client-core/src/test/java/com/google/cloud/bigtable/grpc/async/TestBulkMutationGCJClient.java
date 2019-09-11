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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBulkMutationGCJClient {

  @Rule public ExpectedException expect = ExpectedException.none();

  @Mock private UnaryCallable<RowMutation, Void> callable;

  private BulkMutationGCJClient bulkMutationClient;
  private SettableApiFuture<Void> future;
  private RowMutation rowMutation = RowMutation.create("fake-table", "fake-key");

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
    assertFalse(result.isDone());
    future.set(null);
    assertTrue(result.isDone());
    verify(callable).futureCall(rowMutation);
  }

  @Test(expected = ExecutionException.class)
  public void testAddFailure() throws Exception {
    when(callable.futureCall(rowMutation)).thenReturn(future);
    future.setException(new RuntimeException("can not perform mutation"));
    ApiFuture<Void> result = bulkMutationClient.add(rowMutation);
    assertTrue(result.isDone());
    verify(callable).futureCall(rowMutation);
    result.get();
  }

  @Test
  public void testFlush() throws Exception {
    final SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(callable.futureCall(rowMutation)).thenReturn(future).thenReturn(future2);
    ApiFuture<Void> result1 = bulkMutationClient.add(rowMutation);
    ApiFuture<Void> result2 = bulkMutationClient.add(rowMutation);

    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    try {
      executor.schedule(
          new Runnable() {
            @Override
            public void run() {
              future.set(null);
              future2.set(null);
            }
          },
          50,
          TimeUnit.MILLISECONDS);

      // flush should block until the responses are resolved.
      bulkMutationClient.flush();

      assertTrue(result1.isDone());
      assertTrue(result2.isDone());
    } finally {
      executor.shutdown();
      executor.awaitTermination(100, TimeUnit.MILLISECONDS);
    }
    verify(callable, times(2)).futureCall(rowMutation);
  }

  @Test
  public void testIsClosed() throws IOException {
    bulkMutationClient.close();
    Exception actualEx = null;
    try {
      bulkMutationClient.add(rowMutation);
    } catch (Exception e) {
      actualEx = e;
    }
    assertTrue(actualEx instanceof IllegalStateException);
  }
}
