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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  private static final byte[] emptyBytes = new byte[1];
  private static final Put SIMPLE_PUT = new Put(emptyBytes).addColumn(emptyBytes, emptyBytes, emptyBytes);

  @Mock
  private BigtableDataClient client;

  @Mock 
  private BufferedMutator.ExceptionListener listener;

  @SuppressWarnings("rawtypes")
  @Mock
  private ListenableFuture future;
  private List<Runnable> futureRunnables = new ArrayList<>();

  private BigtableBufferedMutator underTest;

  private ExecutorService heapSizeExecutorService = MoreExecutors.newDirectExecutorService();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    BigtableClusterName clusterName = new BigtableClusterName("project", "zone", "cluster");
    Configuration configuration = new Configuration();
    futureRunnables.clear();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        futureRunnables.add((Runnable)invocation.getArguments()[0]);
        return null;
      }
    }).when(future).addListener(any(Runnable.class), same(heapSizeExecutorService));
    underTest = new BigtableBufferedMutator(
        client,
        new HBaseRequestAdapter(clusterName,  TableName.valueOf("TABLE"), configuration),
        configuration,
        AbstractBigtableConnection.MAX_INFLIGHT_RPCS_DEFAULT,
        AbstractBigtableConnection.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_DEFAULT,
        null,
        listener,
        heapSizeExecutorService);
  }

  @Test
  public void testNoMutation() throws IOException {
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
    for (Runnable runnable : futureRunnables) {
      runnable.run();
    }
  }
}
