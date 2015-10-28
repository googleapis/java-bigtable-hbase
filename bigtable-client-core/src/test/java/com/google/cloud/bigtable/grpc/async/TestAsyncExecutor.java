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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

/**
 * Tests for {@link AsyncExecutor}
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TestAsyncExecutor {

  @Mock
  private BigtableDataClient client;

  @SuppressWarnings("rawtypes")
  @Mock
  private ListenableFuture future;

  private AsyncExecutor underTest;
  private HeapSizeManager heapSizeManager;
  private List<FutureCallback<?>> callbacks;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    callbacks = new ArrayList<>();
    heapSizeManager = new HeapSizeManager(1000, 10){
      @Override
      public <T> FutureCallback<T> addCallback(ListenableFuture<T> future, Long id) {
        FutureCallback<T> callback = super.addCallback(future, id);
        synchronized (callbacks) {
          callbacks.add(callback);
        }
        return callback;
      }
    };

    underTest = new AsyncExecutor(client, heapSizeManager);
  }

  @Test
  public void testNoMutation() throws IOException {
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testMutation() throws IOException, InterruptedException {
    when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
    underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testCheckAndMutate() throws IOException, InterruptedException {
    when(client.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class))).thenReturn(future);
    underTest.checkAndMutateRowAsync(CheckAndMutateRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testReadWriteModify() throws IOException, InterruptedException {
    when(client.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(future);
    underTest.readModifyWriteRowAsync(ReadModifyWriteRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testReadRowsAsync() throws IOException, InterruptedException {
    when(client.readRowsAsync(any(ReadRowsRequest.class))).thenReturn(future);
    underTest.readRowsAsync(ReadRowsRequest.getDefaultInstance());
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

  @Test
  /**
   * Tests to make sure that mutateRowAsync will perform a wait() if there is a bigger count of RPCs
   * than the maximum of the HeapSizeManager.
   */
  public void testRegisterWaitsAfterCountLimit() throws Exception {
    ExecutorService testExecutor = Executors.newCachedThreadPool();
    try {
      when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
      // Fill up the Queue
      for (int i = 0; i < 10; i++) {
        underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
      }
      final AtomicBoolean eleventRpcInvoked = new AtomicBoolean(false);
      testExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
          eleventRpcInvoked.set(true);
          return null;
        }
      });
      Thread.sleep(50);
      Assert.assertFalse(eleventRpcInvoked.get());
      completeCall();
      Thread.sleep(50);
      Assert.assertTrue(eleventRpcInvoked.get());
    } finally {
      testExecutor.shutdownNow();
    }
  }

  @Test
  /**
   * Tests to make sure that mutateRowAsync will perform a wait() if there is a bigger accumulated
   * serialized size of RPCs than the maximum of the HeapSizeManager.
   */
  public void testRegisterWaitsAfterSizeLimit() throws Exception {
    ExecutorService testExecutor = Executors.newCachedThreadPool();
    try {
      when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
      // Send a huge request to block further RPCs.
      underTest.mutateRowAsync(MutateRowRequest.newBuilder()
          .setRowKey(ByteString.copyFrom(new byte[1000])).build());
      final AtomicBoolean newRpcInvoked = new AtomicBoolean(false);
      Future<Void> future = testExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
          newRpcInvoked.set(true);
          return null;
        }
      });
      try {
        future.get(50, TimeUnit.MILLISECONDS);
        Assert.fail("The future.get() call should timeout.");
      } catch(TimeoutException expected) {
        // Expected Exception.
      }
      completeCall();
      future.get(50, TimeUnit.MILLISECONDS);
      Assert.assertTrue(newRpcInvoked.get());
    } finally {
      testExecutor.shutdownNow();
    }
  }

  private void completeCall() {
    synchronized (callbacks) {
      for (FutureCallback<?> fc : callbacks) {
        fc.onSuccess(null);
      }
      callbacks.clear();
    }
  }
}
