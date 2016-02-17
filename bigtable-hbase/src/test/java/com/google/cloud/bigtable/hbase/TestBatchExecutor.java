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
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Empty;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests {@link BatchExecutor}
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBatchExecutor {

  static final byte[] EMPTY_KEY = new byte[1];

  @Mock
  private AsyncExecutor mockAsyncExecutor;
  @Mock
  private ListenableFuture mockFuture;
  @Mock
  private HBaseRequestAdapter mockRequestAdapter;

  private final BigtableOptions options = new BigtableOptions.Builder().build();

  private ListeningExecutorService service;

  private List<Runnable> runnables = null;

  private BatchExecutor underTest;

  @Before
  public void setup() throws InterruptedException {
    service =
        MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService());
    MockitoAnnotations.initMocks(this);
    when(mockAsyncExecutor.readRowsAsync(any(ReadRowsRequest.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class))).thenReturn(mockFuture);
    when(mockAsyncExecutor.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(mockFuture);
    runnables = new ArrayList<>();
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        runnables.add(invocation.getArgumentAt(0, Runnable.class));
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
    underTest = new BatchExecutor(mockAsyncExecutor, options, service, mockRequestAdapter);
  }

  @After
  public void tearDown() {
    service.shutdown();
    service = null;
  }

  @Test
  public void testGet() throws Exception {
    final Row response = Row.getDefaultInstance();
    when(mockFuture.get()).thenReturn(ImmutableList.of(response));
    Result[] results = batch(Arrays.asList(new Get(EMPTY_KEY)));
    Assert.assertTrue(matchesRow(Adapters.ROW_ADAPTER.adaptResponse(response)).matches(results[0]));
  }

  @Test
  public void testPut() throws Exception {
    testMutation(new Put(EMPTY_KEY));
  }

  @Test
  public void testDelete() throws Exception {
    testMutation(new Delete(EMPTY_KEY));
  }

  @Test
  public void testAppend() throws Exception {
    testMutation(new Append(EMPTY_KEY));
  }

  @Test
  public void testIncrement() throws Exception {
    testMutation(new Increment(EMPTY_KEY));
  }

  @Test
  public void testRowMutations() throws Exception {
    testMutation(new RowMutations(EMPTY_KEY));
  }

  @Test
  public void testShutdownService() throws Exception {
    service.shutdown();
    service.awaitTermination(1000, TimeUnit.MILLISECONDS);
    try {
      underTest.batch(Arrays.asList(new Put(EMPTY_KEY)));
    } catch (RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertEquals(IOException.class, e.getCause(0).getClass());
      Assert.assertTrue(e.getCause(0).getMessage().toLowerCase().contains("closed"));
    }
  }

  @Test
  public void testAsyncException() throws Exception {
    String message = "Something bad happened";
    when(mockAsyncExecutor.mutateRowAsync(any(MutateRowRequest.class)))
        .thenThrow(new RuntimeException(message));
    try {
      underTest.batch(Arrays.asList(new Put(EMPTY_KEY)));
    } catch (RetriesExhaustedWithDetailsException e) {
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertEquals(IOException.class, e.getCause(0).getClass());
      Assert.assertEquals(RuntimeException.class, e.getCause(0).getCause().getClass());
      Assert.assertEquals(message, e.getCause(0).getCause().getMessage());
    }
  }

  private void testMutation(org.apache.hadoop.hbase.client.Row mutation)
      throws InterruptedException, ExecutionException, TimeoutException {
    when(mockFuture.get()).thenReturn(Empty.getDefaultInstance());
    Result[] results = batch(Arrays.asList(mutation));
    Assert.assertTrue(matchesRow(Result.EMPTY_RESULT).matches(results[0]));
  }

  @Test
  public void testGetCallback() throws Exception {
    Row response = Row.getDefaultInstance();
    when(mockFuture.get()).thenReturn(ImmutableList.of(response));
    final Callback<Result> callback = Mockito.mock(Callback.class);
    runBatch(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        List<Get> gets = Arrays.asList(new Get(EMPTY_KEY));
        underTest.batchCallback(gets, new Result[1], callback);
        return null;
      }
    });

    verify(callback, times(1)).update(same(BatchExecutor.NO_REGION), same(EMPTY_KEY),
      argThat(matchesRow(Adapters.ROW_ADAPTER.adaptResponse(response))));
  }


  private Result[] batch(final List<? extends org.apache.hadoop.hbase.client.Row> actions)
      throws InterruptedException, ExecutionException, TimeoutException {
    return runBatch(new Callable<Result[]>() {
      @Override
      public Result[] call() throws Exception {
        return underTest.batch(actions);
      }
    });
  }

  private <T> T runBatch(Callable<T> callable)
      throws InterruptedException, ExecutionException, TimeoutException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      Future<T> callableFuture = executorService.submit(callable);
      Thread.sleep(100L);
      for (Runnable runnable : runnables) {
        runnable.run();
      }
      return callableFuture.get(100L, TimeUnit.MILLISECONDS);
    } finally {
      executorService.shutdownNow();
    }
  }

  private Matcher<Result> matchesRow(final Result expected) {
    return new BaseMatcher<Result>() {

      @Override
      public void describeTo(Description description) {
      }

      @Override
      public boolean matches(Object item) {
        try {
          Result.compareResults((Result) item, expected);
          return true;
        } catch (Exception e) {
          return false;
        }
      }

      @Override
      public void describeMismatch(Object item, Description mismatchDescription) {
      }
    };
  }
}
