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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  private static final byte[] emptyBytes = new byte[1];
  private static final Put SIMPLE_PUT = new Put(emptyBytes).addColumn(emptyBytes, emptyBytes, emptyBytes);

  @Mock
  private BigtableDataClient client;

  @SuppressWarnings("rawtypes")
  @Mock
  private ListenableFuture future;

  @Mock
  private BufferedMutator.ExceptionListener listener;

  private BigtableBufferedMutator underTest;
  private List<FutureCallback<?>> callbacks = new ArrayList<>();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    BigtableClusterName clusterName = new BigtableClusterName("project", "zone", "cluster");
    Configuration configuration = new Configuration();
    HeapSizeManager heapSizeManager =
        new HeapSizeManager(AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT,
            AsyncExecutor.MAX_INFLIGHT_RPCS_DEFAULT) {
          @Override
          public <T> FutureCallback<T> addCallback(ListenableFuture<T> future, Long id) {
            FutureCallback<T> callback = super.addCallback(future, id);
            callbacks.add(callback);
            return callback;
          }
        };

    HBaseRequestAdapter adapter =
        new HBaseRequestAdapter(clusterName, TableName.valueOf("TABLE"), configuration);

    underTest = new BigtableBufferedMutator(
      client,
      adapter,
      configuration,
      null,
      listener,
      heapSizeManager);
  }

  @Test
  public void testNoMutation() {
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMutation() throws IOException {
    when(client.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(future);
    underTest.mutate(SIMPLE_PUT);
    verify(client, times(1)).mutateRowAsync(any(MutateRowRequest.class));
    Assert.assertTrue(underTest.hasInflightRequests());
    completeCall();
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testInvalidPut() throws Exception {
    when(client.mutateRowAsync(any(MutateRowRequest.class))).thenThrow(new RuntimeException());
    underTest.mutate(SIMPLE_PUT);
    verify(listener, times(0)).onException(any(RetriesExhaustedWithDetailsException.class),
        same(underTest));
    completeCall();
    underTest.mutate(SIMPLE_PUT);
    verify(listener, times(1)).onException(any(RetriesExhaustedWithDetailsException.class),
        same(underTest));
  }

  private void completeCall() {
    for (FutureCallback<?> callback : callbacks) {
      callback.onSuccess(null);
    }
  }
}
