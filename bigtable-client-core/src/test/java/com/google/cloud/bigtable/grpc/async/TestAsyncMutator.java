/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestAsyncMutator {

  @Mock
  private BigtableDataClient client;

  @SuppressWarnings("rawtypes")
  @Mock
  private ListenableFuture future;
  private List<Runnable> futureRunnables = new ArrayList<>();

  private AsyncMutator underTest;

  private ExecutorService heapSizeExecutorService = MoreExecutors.newDirectExecutorService();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    futureRunnables.clear();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        futureRunnables.add((Runnable)invocation.getArguments()[0]);
        return null;
      }
    }).when(future).addListener(any(Runnable.class), same(heapSizeExecutorService));
    underTest = new AsyncMutator(
        client,
        AsyncMutator.MAX_INFLIGHT_RPCS_DEFAULT,
        AsyncMutator.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT,
        heapSizeExecutorService);
  }

  @Test
  public void testNoMutation() throws IOException {
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMutation() throws IOException, InterruptedException {
    when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
    underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCheckAndMutate() throws IOException, InterruptedException {
    when(client.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class))).thenReturn(future);
    underTest.checkAndMutateRowAsync(CheckAndMutateRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testReadWriteModify() throws IOException, InterruptedException {
    when(client.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(future);
    underTest.readModifyWriteRowAsync(ReadModifyWriteRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testInvalidMutation() throws Exception {
    try {
      when(client.mutateRowAsync(any(MutateRowRequest.class))).thenThrow(new RuntimeException());
      underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
    } catch(Exception ignored) {
    }
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  private void completeCall() {
    for (Runnable runnable : futureRunnables) {
      runnable.run();
    }
  }
}
