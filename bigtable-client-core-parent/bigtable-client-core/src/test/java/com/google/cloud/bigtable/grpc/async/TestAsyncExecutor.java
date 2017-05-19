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

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.ResourceLimiter;
import com.google.common.util.concurrent.SettableFuture;
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
  private SettableFuture future;

  private AsyncExecutor underTest;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    underTest = new AsyncExecutor(client, new ResourceLimiter(1000, 10));
    future = SettableFuture.create();
  }

  @Test
  public void testNoMutation() {
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testMutation() throws InterruptedException {
    when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
    underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testCheckAndMutate() throws InterruptedException {
    when(client.checkAndMutateRowAsync(any(CheckAndMutateRowRequest.class))).thenReturn(future);
    underTest.checkAndMutateRowAsync(CheckAndMutateRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testReadWriteModify() throws InterruptedException {
    when(client.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(future);
    underTest.readModifyWriteRowAsync(ReadModifyWriteRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testReadRowsAsync() throws InterruptedException {
    when(client.readRowsAsync(any(ReadRowsRequest.class))).thenReturn(future);
    underTest.readRowsAsync(ReadRowsRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testInvalidMutation() throws Exception {
    try {
      when(client.mutateRowAsync(any(MutateRowRequest.class))).thenThrow(new RuntimeException());
      underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
    } catch(Exception ignored) {
    }
    future.set(ReadRowsResponse.getDefaultInstance());
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  /**
   * Tests to make sure that mutateRowAsync will perform a wait() if there is a bigger count of RPCs
   * than the maximum of the RpcThrottler.
   */
  public void testRegisterWaitsAfterCountLimit() throws Exception {
    ExecutorService testExecutor = Executors.newCachedThreadPool();
    try {
      when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
      // Fill up the Queue
      for (int i = 0; i < 10; i++) {
        underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
      }
      final AtomicBoolean eleventhRpcInvoked = new AtomicBoolean(false);
      testExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
          eleventhRpcInvoked.set(true);
          return null;
        }
      });
      Thread.sleep(50);
      Assert.assertFalse(eleventhRpcInvoked.get());
      future.set("");
      Thread.sleep(50);
      Assert.assertTrue(eleventhRpcInvoked.get());
    } finally {
      testExecutor.shutdownNow();
    }
  }

  @Test
  /**
   * Tests to make sure that mutateRowAsync will perform a wait() if there is a bigger accumulated
   * serialized size of RPCs than the maximum of the RpcThrottler.
   */
  public void testRegisterWaitsAfterSizeLimit() throws Exception {
    ExecutorService testExecutor = Executors.newCachedThreadPool();
    try {
      when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
      // Send a huge request to block further RPCs.
      underTest.mutateRowAsync(MutateRowRequest.newBuilder()
          .setRowKey(ByteString.copyFrom(new byte[1000])).build());
      final AtomicBoolean newRpcInvoked = new AtomicBoolean(false);
      Future<Void> future1 = testExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.mutateRowAsync(MutateRowRequest.getDefaultInstance());
          newRpcInvoked.set(true);
          return null;
        }
      });
      try {
        future1.get(50, TimeUnit.MILLISECONDS);
        Assert.fail("The future.get() call should timeout.");
      } catch(TimeoutException expected) {
        // Expected Exception.
      }
      future.set("");
      future1.get(50, TimeUnit.MILLISECONDS);
      Assert.assertTrue(newRpcInvoked.get());
    } finally {
      testExecutor.shutdownNow();
    }
  }
}
