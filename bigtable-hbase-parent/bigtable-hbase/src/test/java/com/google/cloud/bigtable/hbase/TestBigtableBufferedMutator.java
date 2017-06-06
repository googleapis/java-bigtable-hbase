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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  private static final byte[] EMPTY_BYTES = new byte[1];
  private static final Put SIMPLE_PUT =
      new Put(EMPTY_BYTES).addColumn(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);

  @Mock
  private BigtableSession mockSession;

  @Mock
  private AsyncExecutor mockAsyncExecutor;

  @Mock
  private BulkMutation mockBulkMutation;

  @SuppressWarnings("rawtypes")
  private SettableFuture future = SettableFuture.create();

  @Mock
  private BufferedMutator.ExceptionListener listener;

  private ExecutorService executorService;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockSession.createAsyncExecutor()).thenReturn(mockAsyncExecutor);
    when(mockSession.createBulkMutation(any(BigtableTableName.class), any(AsyncExecutor.class)))
        .thenReturn(mockBulkMutation);
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
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class), anyLong()))
        .thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(new Configuration(false));
    underTest.mutate(SIMPLE_PUT);
    // Leave some time for the async worker to handle the request.
    Thread.sleep(100);
    verify(mockAsyncExecutor, times(1)).mutateRowAsync(any(MutateRowRequest.class), anyLong());
  }

  @Test
  public void testInvalidPut() throws Exception {
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class), anyLong()))
        .thenThrow(new RuntimeException());
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
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class), anyLong())).thenReturn(future);
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, "0");
    BigtableBufferedMutator underTest = createMutator(config);
    underTest.mutate(SIMPLE_PUT);
    verify(mockAsyncExecutor, times(1)).mutateRowAsync(any(MutateRowRequest.class), anyLong());
  }

  @Test
  public void testBulkSingleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    when(mockBulkMutation.add(any(MutateRowsRequest.Entry.class))).thenReturn(future);
    final BigtableBufferedMutator underTest = createMutator(config);
    underTest.mutate(SIMPLE_PUT);
    verify(mockAsyncExecutor, times(0)).mutateRowAsync(any(MutateRowRequest.class), anyLong());
    verify(mockBulkMutation, times(1)).add(any(MutateRowsRequest.Entry.class));
    underTest.flush();
    verify(mockBulkMutation, times(1)).flush();
    verify(mockAsyncExecutor, times(1)).flush();
  }

  @Test
  public void testBulkMultipleRequests() throws IOException, InterruptedException {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    when(mockBulkMutation.add(any(MutateRowsRequest.Entry.class))).thenReturn(future);
    BigtableBufferedMutator underTest = createMutator(config);
    int count = 30;
    for (int i = 0; i < count; i++) {
      underTest.mutate(SIMPLE_PUT);
    }
    verify(mockBulkMutation, times(count)).add(any(MutateRowsRequest.Entry.class));
    underTest.flush();
    verify(mockBulkMutation, times(1)).flush();
    verify(mockAsyncExecutor, times(1)).flush();
  }
}
