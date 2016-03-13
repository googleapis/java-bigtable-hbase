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
package com.google.cloud.bigtable.hbase;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.junit.After;
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

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.MutateRowsRequest;
import com.google.bigtable.v1.MutateRowsResponse;
import com.google.bigtable.v1.MutateRowsResponse.Builder;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.rpc.Status;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  private static final byte[] EMPTY_BYTES = new byte[1];
  private static final Put SIMPLE_PUT = new Put(EMPTY_BYTES).addColumn(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);
  private static final Status OK_STATUS =
      Status.newBuilder().setCode(io.grpc.Status.OK.getCode().value()).build();

  @Mock
  private BigtableDataClient mockClient;

  @SuppressWarnings("rawtypes")
  @Mock
  private ListenableFuture mockFuture;

  @Mock
  private BufferedMutator.ExceptionListener listener;

  private ExecutorService executorService;

  private List<Runnable> callbacks = new ArrayList<>();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        callbacks.add(invocation.getArgumentAt(0, Runnable.class));
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
  }

  @After
  public void tearDown(){
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  private BigtableBufferedMutator createMutator(Configuration configuration) throws IOException {
    HeapSizeManager heapSizeManager =
        new HeapSizeManager(AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT,
            AsyncExecutor.MAX_INFLIGHT_RPCS_DEFAULT);

    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, "project");
    configuration.set(BigtableOptionsFactory.ZONE_KEY, "zone");
    configuration.set(BigtableOptionsFactory.CLUSTER_KEY, "cluster");

    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    HBaseRequestAdapter adapter = new HBaseRequestAdapter(
        options.getClusterName(), TableName.valueOf("TABLE"), configuration);

    executorService = Executors.newCachedThreadPool();
    return new BigtableBufferedMutator(
      mockClient,
      adapter,
      configuration,
      options,
      listener,
      heapSizeManager,
      executorService);
  }

  @Test
  public void testNoMutation() throws IOException {
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMutation() throws IOException, InterruptedException {
    when(mockClient.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(mockFuture);
    try (BigtableBufferedMutator underTest = createMutator(new Configuration(false))) {
      underTest.mutate(SIMPLE_PUT);
      // Leave some time for the async worker to handle the request.
      Thread.sleep(100);
      verify(mockClient, times(1)).mutateRowAsync(any(MutateRowRequest.class));
      Assert.assertTrue(underTest.hasInflightRequests());
      completeCall();
      Assert.assertFalse(underTest.hasInflightRequests());
    }
  }

  @Test
  public void testInvalidPut() throws Exception {
    when(mockClient.mutateRowAsync(any(MutateRowRequest.class))).thenThrow(new RuntimeException());
    try (BigtableBufferedMutator underTest = createMutator(new Configuration(false))) {
      underTest.mutate(SIMPLE_PUT);
      // Leave some time for the async worker to handle the request.
      Thread.sleep(100);
      verify(listener, times(0)).onException(any(RetriesExhaustedWithDetailsException.class),
          same(underTest));
      completeCall();
      underTest.mutate(SIMPLE_PUT);
      verify(listener, times(1)).onException(any(RetriesExhaustedWithDetailsException.class),
          same(underTest));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testZeroWorkers() throws Exception {
    when(mockClient.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(mockFuture);
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, "0");
    try (BigtableBufferedMutator underTest = createMutator(config)) {
      underTest.mutate(SIMPLE_PUT);
      verify(mockClient, times(1)).mutateRowAsync(any(MutateRowRequest.class));
      Assert.assertTrue(underTest.hasInflightRequests());
      completeCall();
      Assert.assertFalse(underTest.hasInflightRequests());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBulkSingleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    SettableFuture<MutateRowsResponse> future = SettableFuture.create();
    when(mockClient.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(future);
    SettableFuture<Empty> retryFuture = SettableFuture.<Empty>create();
    when(mockClient.addMutationRetry(any(ListenableFuture.class), any(MutateRowRequest.class)))
        .thenReturn(retryFuture);
    try (final BigtableBufferedMutator underTest = createMutator(config)) {
      underTest.mutate(SIMPLE_PUT);
      verify(mockClient, times(0)).mutateRowAsync(any(MutateRowRequest.class));
      Future<Void> flushFuture = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.flush();
          verify(mockClient, times(1)).mutateRowsAsync(any(MutateRowsRequest.class));
          return null;
        }
      });
      future.set(MutateRowsResponse.newBuilder().addStatuses(OK_STATUS).build());
      Thread.sleep(100);
      completeCall();
      try {
        flushFuture.get(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        e.printStackTrace();
      }
      verify(mockClient, times(1)).mutateRowsAsync(any(MutateRowsRequest.class));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBulkMultipleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    config.set(BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT, "10");
    config.set(BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES, "1000");
    final List<SettableFuture<MutateRowsResponse>> futures = new ArrayList<>();
    when(mockClient.mutateRowsAsync(any(MutateRowsRequest.class)))
        .thenAnswer(new Answer<SettableFuture<MutateRowsResponse>>() {
          @Override
          public SettableFuture<MutateRowsResponse> answer(InvocationOnMock invocation)
              throws Throwable {
            SettableFuture<MutateRowsResponse> future = SettableFuture.create();
            futures.add(future);
            return future;
          }
        });
    when(mockClient.addMutationRetry(any(ListenableFuture.class), any(MutateRowRequest.class)))
        .thenReturn(SettableFuture.<Empty> create());
    try (final BigtableBufferedMutator underTest = createMutator(config)) {
      Builder firstResponseBuilder = MutateRowsResponse.newBuilder();
      while(futures.size() == 0) {
        underTest.mutate(SIMPLE_PUT);
        firstResponseBuilder.addStatuses(OK_STATUS);
      }
      futures.get(0).set(firstResponseBuilder.build());
      underTest.mutate(SIMPLE_PUT);
      Future<Void> flushFuture = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.flush();
          verify(mockClient, times(2)).mutateRowsAsync(any(MutateRowsRequest.class));
          return null;
        }
      });
      Thread.sleep(100);
      futures.get(1).set(MutateRowsResponse.newBuilder().addStatuses(OK_STATUS).build());
      Thread.sleep(100);
      completeCall();
      try {
        flushFuture.get(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        e.printStackTrace();
      }
      verify(mockClient, times(2)).mutateRowsAsync(any(MutateRowsRequest.class));
    }
  }

  private void completeCall() {
    for (Runnable callback : callbacks) {
      callback.run();
    }
    callbacks.clear();
  }
}
