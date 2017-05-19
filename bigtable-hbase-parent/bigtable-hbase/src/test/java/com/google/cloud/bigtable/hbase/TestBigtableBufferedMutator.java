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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
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
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Builder;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.OperationAccountant;
import com.google.cloud.bigtable.grpc.async.ResourceLimiter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.rpc.Status;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  private static final byte[] EMPTY_BYTES = new byte[1];
  private static final Put SIMPLE_PUT =
      new Put(EMPTY_BYTES).addColumn(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);
  private static final Status OK_STATUS =
      Status.newBuilder().setCode(io.grpc.Status.OK.getCode().value()).build();

  @Mock
  private BigtableSession mockSession;

  @Mock
  private BigtableDataClient mockClient;

  @SuppressWarnings("rawtypes")
  private SettableFuture future = SettableFuture.create();

  @Mock
  private BufferedMutator.ExceptionListener listener;

  private ExecutorService executorService;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockSession.createAsyncExecutor())
        .thenAnswer(
            new Answer<AsyncExecutor>() {
              @Override
              public AsyncExecutor answer(InvocationOnMock invocation) throws Throwable {
                OperationAccountant operationAccountant =
                    new OperationAccountant(new ResourceLimiter(BulkOptions.BIGTABLE_MAX_MEMORY_DEFAULT,
                      BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT));

                return new AsyncExecutor(mockClient, operationAccountant);
              }
            });
    when(mockSession.createBulkMutation(any(BigtableTableName.class), any(AsyncExecutor.class)))
        .thenAnswer(new Answer<BulkMutation>() {

          @Override
          public BulkMutation answer(InvocationOnMock invocation) throws Throwable {
            final BigtableOptions options = mockSession.getOptions();
            return new BulkMutation(
                invocation.getArgumentAt(0, BigtableTableName.class),
                invocation.getArgumentAt(1, AsyncExecutor.class),
                options.getRetryOptions(),
                BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(),
                options.getBulkOptions().getBulkMaxRowKeyCount(),
                options.getBulkOptions().getBulkMaxRequestSize());
          }
        });
  }

  @After
  public void tearDown(){
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  private BigtableBufferedMutator createMutator(Configuration configuration) throws IOException {
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, "project");
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "instance");
    if (configuration.get(BigtableOptionsFactory.BIGTABLE_USE_BULK_API) == null) {
      configuration.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "false");
    }

    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    HBaseRequestAdapter adapter =
        new HBaseRequestAdapter(options, TableName.valueOf("TABLE"), configuration);

    executorService = Executors.newCachedThreadPool();
    when(mockSession.getOptions()).thenReturn(options);
    return new BigtableBufferedMutator(
        adapter, configuration, mockSession, listener, executorService);
  }

  @Test
  public void testNoMutation() throws IOException {
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testMutation() throws IOException, InterruptedException {
    when(mockClient.mutateRowAsync(any(MutateRowRequest.class)))
        .thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(SIMPLE_PUT);
    Assert.assertTrue(underTest.hasInflightRequests());
    // Leave some time for the async worker to handle the request.
    Thread.sleep(100);
    verify(mockClient, times(1)).mutateRowAsync(any(MutateRowRequest.class));
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testInvalidPut() throws Exception {
    when(mockClient.mutateRowAsync(any(MutateRowRequest.class))).thenThrow(new RuntimeException());
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(SIMPLE_PUT);
    // Leave some time for the async worker to handle the request.
    Thread.sleep(100);
    verify(listener, times(0)).onException(any(RetriesExhaustedWithDetailsException.class),
        same(underTest));
    future.set("");
    underTest.mutate(SIMPLE_PUT);
    verify(listener, times(1)).onException(any(RetriesExhaustedWithDetailsException.class),
        same(underTest));
  }

  @Test
  public void testZeroWorkers() throws Exception {
    when(mockClient.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, "0");
    BigtableBufferedMutator underTest = createMutator(config);
    underTest.mutate(SIMPLE_PUT);
    Assert.assertTrue(underTest.hasInflightRequests());
    verify(mockClient, times(1)).mutateRowAsync(any(MutateRowRequest.class));
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testBulkSingleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    when(mockClient.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(future);
    final BigtableBufferedMutator underTest = createMutator(config);
    try {
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
      future.set(Arrays.asList(MutateRowsResponse.newBuilder()
          .addEntries(Entry.newBuilder().setStatus(OK_STATUS)).build()));
      Thread.sleep(100);
      try {
        flushFuture.get(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        Assert.fail("Didn't flush in time");
      }
      verify(mockClient, times(1)).mutateRowsAsync(any(MutateRowsRequest.class));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testBulkMultipleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    config.set(BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT, "10");
    config.set(BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES, "1000");
    final List<SettableFuture<List<MutateRowsResponse>>> futures = new ArrayList<>();
    when(mockClient.mutateRowsAsync(any(MutateRowsRequest.class)))
        .thenAnswer(new Answer<SettableFuture<List<MutateRowsResponse>>>() {
          @Override
          public SettableFuture<List<MutateRowsResponse>> answer(InvocationOnMock invocation)
              throws Throwable {
            SettableFuture<List<MutateRowsResponse>> future = SettableFuture.create();
            futures.add(future);
            return future;
          }
        });
    final BigtableBufferedMutator underTest = createMutator(config);
    try {
      Builder firstResponseBuilder = MutateRowsResponse.newBuilder();
      int index = 0;
      while(futures.size() == 0) {
        underTest.mutate(SIMPLE_PUT);
        firstResponseBuilder.addEntriesBuilder().setStatus(OK_STATUS).setIndex(index++);
      }
      futures.get(0).set(Arrays.asList(firstResponseBuilder.build()));
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
      futures.get(1).set(Arrays.asList(MutateRowsResponse.newBuilder()
          .addEntries(Entry.newBuilder().setStatus(OK_STATUS).setIndex(0)).build()));
      Thread.sleep(100);
      future.set("");
      try {
        flushFuture.get(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        Assert.fail("Didn't flush in time");
      }
      verify(mockClient, times(2)).mutateRowsAsync(any(MutateRowsRequest.class));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
