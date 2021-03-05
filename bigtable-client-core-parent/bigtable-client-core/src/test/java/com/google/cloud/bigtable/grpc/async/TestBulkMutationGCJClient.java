/*
 * Copyright 2019 Google LLC
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatcherImpl;
import com.google.api.gax.batching.BatchingDescriptor;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestBulkMutationGCJClient {

  @Rule public ExpectedException expect = ExpectedException.none();

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private Batcher<RowMutationEntry, Void> batcher;

  @InjectMocks private BulkMutationGCJClient bulkMutationClient;

  private RowMutationEntry rowMutation = RowMutationEntry.create("fake-key");

  @Test
  public void testAdd() {
    SettableApiFuture<Void> future = SettableApiFuture.create();
    when(batcher.add(rowMutation)).thenReturn(future);
    ApiFuture<Void> result = bulkMutationClient.add(rowMutation);
    assertFalse(result.isDone());
    future.set(null);
    assertTrue(result.isDone());
    verify(batcher).add(rowMutation);
  }

  @Test(expected = ExecutionException.class)
  public void testAddFailure() throws Exception {
    SettableApiFuture<Void> future = SettableApiFuture.create();
    when(batcher.add(rowMutation)).thenReturn(future);
    future.setException(new RuntimeException("can not perform mutation"));
    ApiFuture<Void> result = bulkMutationClient.add(rowMutation);
    assertTrue(result.isDone());
    verify(batcher).add(rowMutation);
    result.get();
  }

  @Test
  public void testFlush() throws Exception {
    final SettableApiFuture<Void> future = SettableApiFuture.create();
    final SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(batcher.add(rowMutation)).thenReturn(future).thenReturn(future2);
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) {
                future.set(null);
                future2.set(null);
                return null;
              }
            })
        .when(batcher)
        .flush();
    ApiFuture<Void> result1 = bulkMutationClient.add(rowMutation);
    ApiFuture<Void> result2 = bulkMutationClient.add(rowMutation);
    // flush should block until the responses are resolved.
    bulkMutationClient.flush();

    assertTrue(result1.isDone());
    assertTrue(result2.isDone());
    verify(batcher, times(2)).add(rowMutation);
    verify(batcher).flush();
  }

  @Test
  public void testIsClosed() throws IOException {
    BatchingSettings batchingSettings = mock(BatchingSettings.class);
    FlowControlSettings flowControlSettings = mock(FlowControlSettings.class);
    when(flowControlSettings.getLimitExceededBehavior()).thenReturn(LimitExceededBehavior.Ignore);
    when(batchingSettings.getFlowControlSettings()).thenReturn(flowControlSettings);

    @SuppressWarnings("unchecked")
    Batcher<RowMutationEntry, Void> actualBatcher =
        new BatcherImpl(
            mock(BatchingDescriptor.class),
            mock(UnaryCallable.class),
            new Object(),
            batchingSettings,
            mock(ScheduledExecutorService.class));
    IBulkMutation underTest = new BulkMutationGCJClient(actualBatcher);
    underTest.close();

    Exception actualEx = null;
    try {
      underTest.add(rowMutation);
    } catch (Exception e) {
      actualEx = e;
    }
    assertTrue(actualEx instanceof IllegalStateException);
  }
}
