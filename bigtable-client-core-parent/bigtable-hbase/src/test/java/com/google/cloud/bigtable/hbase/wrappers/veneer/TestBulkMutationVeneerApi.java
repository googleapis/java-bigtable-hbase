/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchResource;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatcherImpl;
import com.google.api.gax.batching.BatchingDescriptor;
import com.google.api.gax.batching.BatchingRequestBuilder;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestBulkMutationVeneerApi {

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private Batcher<RowMutationEntry, Void> batcher;

  private BulkMutationVeneerApi bulkMutationWrapper;

  private RowMutationEntry rowMutation = RowMutationEntry.create("fake-key");

  @Before
  public void setup() {
    bulkMutationWrapper = new BulkMutationVeneerApi(batcher, 0);
  }

  @Test
  public void testAdd() {
    SettableApiFuture<Void> future = SettableApiFuture.create();
    when(batcher.add(rowMutation)).thenReturn(future);
    ApiFuture<Void> result = bulkMutationWrapper.add(rowMutation);
    assertFalse(result.isDone());
    future.set(null);
    assertTrue(result.isDone());
    verify(batcher).add(rowMutation);
  }

  @Test
  public void testAddFailure() {
    RuntimeException exception = new RuntimeException("can not perform mutation");
    SettableApiFuture<Void> future = SettableApiFuture.create();
    when(batcher.add(rowMutation)).thenReturn(future);
    future.setException(exception);
    try {
      bulkMutationWrapper.add(rowMutation).get();
      fail("should throw an exception");
    } catch (Exception actualException) {
      assertEquals(exception, actualException.getCause());
    }
    verify(batcher).add(rowMutation);
  }

  @Test
  public void testFlush() throws Exception {
    final SettableApiFuture<Void> future1 = SettableApiFuture.create();
    final SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(batcher.add(rowMutation)).thenReturn(future1).thenReturn(future2);
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) {
                future1.set(null);
                future2.set(null);
                return null;
              }
            })
        .when(batcher)
        .flush();
    ApiFuture<Void> result1 = bulkMutationWrapper.add(rowMutation);
    ApiFuture<Void> result2 = bulkMutationWrapper.add(rowMutation);
    // flush should block until the responses are resolved.
    bulkMutationWrapper.flush();

    assertTrue(result1.isDone());
    assertTrue(result2.isDone());
    verify(batcher, times(2)).add(rowMutation);
    verify(batcher).flush();
  }

  @Test
  public void testWhenBatcherIsClosed() throws IOException {
    BatchingSettings batchingSettings = mock(BatchingSettings.class);
    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setLimitExceededBehavior(LimitExceededBehavior.Ignore)
            .build();
    when(batchingSettings.getFlowControlSettings()).thenReturn(flowControlSettings);

    BatchingDescriptor mockBatchingDescriptor = mock(BatchingDescriptor.class);

    when(mockBatchingDescriptor.createEmptyResource()).thenReturn(new FakeResource());
    when(mockBatchingDescriptor.newRequestBuilder(any()))
        .thenReturn(mock(BatchingRequestBuilder.class));

    UnaryCallable unaryCallable = mock(UnaryCallable.class);
    when(unaryCallable.futureCall(any(), any()))
        .thenReturn(ApiFutures.immediateFuture(new Object()));

    @SuppressWarnings("unchecked")
    Batcher<RowMutationEntry, Void> actualBatcher =
        new BatcherImpl(
            mockBatchingDescriptor,
            unaryCallable,
            new Object(),
            batchingSettings,
            mock(ScheduledExecutorService.class));
    BulkMutationWrapper underTest = new BulkMutationVeneerApi(actualBatcher, 0);
    underTest.close();

    Exception actualEx = null;
    try {
      underTest.add(rowMutation);
      fail("batcher should throw exception");
    } catch (Exception e) {
      actualEx = e;
    }
    assertTrue(actualEx instanceof IllegalStateException);
  }

  private class FakeResource implements BatchResource {

    @Override
    public BatchResource add(BatchResource resource) {
      return new FakeResource();
    }

    @Override
    public long getElementCount() {
      return 1;
    }

    @Override
    public long getByteCount() {
      return 1;
    }

    @Override
    public boolean shouldFlush(long maxElementThreshold, long maxBytesThreshold) {
      return false;
    }
  }
}
