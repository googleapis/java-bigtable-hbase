/*
 * Copyright 2015 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.GeneratedMessage;

/**
 * Tests for {@link BigtableAsyncExecutor}
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TestBigtableAsyncExecutor {

  private BigtableAsyncExecutor underTest;

  @Mock
  BigtableDataClient client;

  @SuppressWarnings("rawtypes")
  @Mock
  private ListenableFuture future;

  private List<Runnable> runnables = new ArrayList<>();
  @Mock
  Executor executor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    setup();
  }

  private void setup() {
    underTest = new BigtableAsyncExecutor(100, 100000000l, client, executor);
    runnables.clear();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Runnable r = (Runnable) invocation.getArguments()[0];
        runnables.add(r);
        return null;
      }
    }).when(future).addListener(any(Runnable.class), any(Executor.class));
  }

  @Test
  public void testNoMutation() throws IOException {
    Assert.assertFalse(underTest.hasInflightRequests());
    Assert.assertEquals(0l, underTest.sizeManager.getHeapSize());
  }

  @Test
  public void testMutation() throws IOException, InterruptedException, ExecutionException {
    MutateRowRequest request = MutateRowRequest.getDefaultInstance();
    when(client.mutateRowAsync(eq(request))).thenReturn(future);
    when(future.get()).thenReturn(null);
    underTest.mutateRowAsync(request);
    verify(client, times(1)).mutateRowAsync(eq(request));
    verifyGoodCall(request);
  }

  @Test
  public void testCheckAndMutate() throws IOException, InterruptedException, ExecutionException {
    CheckAndMutateRowRequest request = CheckAndMutateRowRequest.getDefaultInstance();
    when(client.checkAndMutateRowAsync(eq(request))).thenReturn(future);
    underTest.checkAndMutateRowAsync(request);
    when(future.get()).thenReturn(null);
    verify(client, times(1)).checkAndMutateRowAsync(eq(request));
    verifyGoodCall(request);
  }

  @Test
  public void testRedModifyWrite() throws IOException, InterruptedException, ExecutionException {
    ReadModifyWriteRowRequest request = ReadModifyWriteRowRequest.getDefaultInstance();
    when(client.readModifyWriteRowAsync(eq(request))).thenReturn(future);
    when(future.get()).thenReturn(null);
    underTest.readModifyWriteRowAsync(request);
    verify(client, times(1)).readModifyWriteRowAsync(eq(request));
    verifyGoodCall(request);
  }

  private void verifyGoodCall(GeneratedMessage request) {
    Assert.assertTrue(underTest.hasInflightRequests());
    Assert.assertEquals(request.getSerializedSize(), underTest.sizeManager.getHeapSize());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
    Assert.assertEquals(0l, underTest.sizeManager.getHeapSize());
  }

  private void completeCall() {
    for (Runnable r : runnables) {
      r.run();
    }
  }

  @Test
  public void testInvalidPut() throws Exception {
    MutateRowRequest request = MutateRowRequest.getDefaultInstance();
    when(client.mutateRowAsync(eq(request))).thenReturn(future);
    underTest.mutateRowAsync(request);
    completeCall();
    // wait until the handling in the heapSizeExecutor kicks in.
    Assert.assertFalse(underTest.hasInflightRequests());
    Assert.assertEquals(0l, underTest.sizeManager.getHeapSize());
  }
}
