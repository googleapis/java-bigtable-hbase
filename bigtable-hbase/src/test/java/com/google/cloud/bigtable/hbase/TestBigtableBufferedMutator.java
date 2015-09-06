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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  @Mock
  BatchExecutor executor;

  @Mock
  HeapSizeManager heapSizeManager;

  @SuppressWarnings("rawtypes")
  @Mock
  ListenableFuture future;

  List<Runnable> runnables = null;
  private BigtableBufferedMutator underTest;


  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    setup();
  }

  private void setup() {
    BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
      @Override
      public void onException(RetriesExhaustedWithDetailsException exception,
          BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
        throw exception;
      }
    };
    underTest =
        new BigtableBufferedMutator(null, TableName.valueOf("TABLE"), heapSizeManager, null,
            listener, executor);
    runnables = new ArrayList<>();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        runnables.add((Runnable)invocation.getArguments()[0]);
        return null;
      }
    }).when(future).addListener(any(Runnable.class), any(Executor.class));
    completeRequest();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMutation() throws IOException, InterruptedException {
    when(heapSizeManager.registerOperationWithHeapSize(any(Integer.class))).thenReturn(1l);
    when(executor.issueRequest(any(Row.class))).thenReturn(future);
    Put mutation = new Put(new byte[1]);
    underTest.mutate(mutation);
    verify(executor, times(1)).issueRequest(any(Row.class));
    verify(heapSizeManager, times(1)).registerOperationWithHeapSize(eq(mutation.heapSize()));
    completeRequest();
    assertFalse(underTest.hasExceptions.get());
  }

  @Test
  public void testException() {
    underTest.hasExceptions.set(true);
    underTest.addGlobalException(null, new Exception());

    try {
      underTest.handleExceptions();
      Assert.fail("expected RetriesExhaustedWithDetailsException");
    } catch (RetriesExhaustedWithDetailsException expected) {
      // Expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testListenerException() throws Exception {
    when(heapSizeManager.registerOperationWithHeapSize(any(Integer.class))).thenReturn(1l);
    when(executor.issueRequest(any(Row.class))).thenReturn(future);
    when(future.get()).thenThrow(new RuntimeException("some exception"));
    Put mutation = new Put(new byte[1]);
    underTest.mutate(mutation);
    completeRequest();
    assertTrue(underTest.hasExceptions.get());
  }

  private void completeRequest() {
    for (Runnable runnable : runnables) {
      runnable.run();
    }
  }

  @Test
  public void testInvalidPut() throws IOException {
    when(executor.issueRequest((Row) any())).thenThrow(new RuntimeException());
    try {
      underTest.mutate(new Increment(new byte[1]));
    } catch (RuntimeException ignored) {
      // The RuntimeException is expected behavior
    }
    assertTrue(underTest.hasExceptions.get());
  }

}
