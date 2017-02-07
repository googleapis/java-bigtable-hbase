/*
 * Copyright 2015 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.Clock;
import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkMutation.RequestManager;
import com.google.cloud.bigtable.grpc.async.RpcThrottler;
import com.google.cloud.bigtable.grpc.async.RpcThrottler.RetryHandler;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import io.grpc.StatusRuntimeException;

/**
 * Tests for {@link BulkMutation}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@RunWith(JUnit4.class)
public class TestBulkMutation {
  private final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName("project", "instance").toTableName("table");
  private final static ByteString QUALIFIER = ByteString.copyFrom("qual".getBytes());
  private static AtomicLong retryIdGenerator = new AtomicLong();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  AsyncExecutor asyncExecutor;

  @Mock
  ScheduledExecutorService retryExecutorService;

  @Mock
  RpcThrottler rpcThrottler;

  @Mock
  ListenableFuture mockFuture;

  @Mock NanoClock nanoClock;

  RetryOptions retryOptions;

  private BulkMutation underTest;

  @Before
  public void setup() throws InterruptedException {
    MockitoAnnotations.initMocks(this);
    retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);

    when(asyncExecutor.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(mockFuture);
    when(asyncExecutor.getRpcThrottler()).thenReturn(rpcThrottler);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
    doReturn(true).when(mockFuture).isDone();

    when(rpcThrottler.registerRetry(any(RetryHandler.class))).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return retryIdGenerator.incrementAndGet();
      }
    });

    underTest = new BulkMutation(TABLE_NAME, asyncExecutor, retryOptions,
        retryExecutorService, 1000, 1000000L);
  }

  @After
  public void turnDown() {
    BulkMutation.clock = Clock.SYSTEM;
  }

  @Test
  public void testAdd() {
    MutateRowRequest mutateRowRequest = createRequest();
    BulkMutation.RequestManager requestManager = createTestRequestManager();
    requestManager.add(null, BulkMutation.convert(mutateRowRequest));
    Entry entry = Entry.newBuilder()
        .setRowKey(mutateRowRequest.getRowKey())
        .addMutations(mutateRowRequest.getMutations(0))
        .build();
    MutateRowsRequest expected = MutateRowsRequest.newBuilder()
        .setTableName(TABLE_NAME.toString())
        .addEntries(entry)
        .build();
    Assert.assertEquals(expected, requestManager.build());
  }

  private RequestManager createTestRequestManager() {
    return new BulkMutation.RequestManager(
        TABLE_NAME.toString(), BigtableClientMetrics.meter(MetricLevel.Trace, "test.bulk"));
  }

  protected static MutateRowRequest createRequest() {
    SetCell setCell = SetCell.newBuilder()
        .setFamilyName("cf1")
        .setColumnQualifier(QUALIFIER)
        .build();
    ByteString rowKey = ByteString.copyFrom("SomeKey".getBytes());
    return MutateRowRequest.newBuilder()
        .setRowKey(rowKey)
        .addMutations(Mutation.newBuilder()
          .setSetCell(setCell))
        .build();
  }

  @Test
  public void testCallableSuccess()
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    long preRunId = retryIdGenerator.get();
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testCallableNotRetriedStatus()
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    ListenableFuture<List<MutateRowsResponse>> rowsFuture = SettableFuture.<List<MutateRowsResponse>>create();
    when(nanoClock.nanoTime()).thenReturn(1l);
    underTest.currentBatch.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture.isDone());
    long preRunId = retryIdGenerator.get();
    setResponse(io.grpc.Status.NOT_FOUND);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    try {
      rowFuture.get(100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.assertEquals(StatusRuntimeException.class, e.getCause().getClass());
      verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
    }
  }

  @Test
  public void testRetriedStatus() throws InterruptedException, ExecutionException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    Assert.assertFalse(rowFuture.isDone());

    // Send Deadline exceeded,
    long preRunId = retryIdGenerator.get();
    setResponse(io.grpc.Status.DEADLINE_EXCEEDED);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    when(nanoClock.nanoTime()).thenReturn(1l);

    // Make sure that the request is scheduled
    Assert.assertFalse(rowFuture.isDone());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    verify(rpcThrottler, times(0)).onRetryCompletion(eq(retryIdGenerator.get()));

    // Make sure that a second try works.
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    Assert.assertTrue(rowFuture.isDone());
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testCallableTooFewStatuses() throws InterruptedException, ExecutionException {
    ListenableFuture<MutateRowResponse> rowFuture1 = underTest.add(createRequest());
    ListenableFuture<MutateRowResponse> rowFuture2 = underTest.add(createRequest());
    SettableFuture<List<MutateRowsResponse>> rowsFuture = SettableFuture.<List<MutateRowsResponse>> create();
    underTest.currentBatch.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());

    Assert.assertEquals(2, underTest.currentBatch.getRequestCount());
    // Send only one response - this is poor server behavior.
    long preRunId = retryIdGenerator.get();
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    when(nanoClock.nanoTime()).thenReturn(0l);
    Assert.assertEquals(1, underTest.currentBatch.getRequestCount());

    // Make sure that the first request completes, but the second does not.
    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), rowFuture1.get());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    verify(rpcThrottler, times(0)).onRetryCompletion(eq(retryIdGenerator.get()));

    // Make sure that only the second request was sent.
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    Assert.assertEquals(0, underTest.currentBatch.getRequestCount());
    Assert.assertTrue(rowFuture2.isDone());
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testRunOutOfTime() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());

    AtomicLong waitedNanos = new AtomicLong();
    setupScheduler(waitedNanos);
    long preRunId = retryIdGenerator.get();
    setResponse(io.grpc.Status.DEADLINE_EXCEEDED);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    try {
      rowFuture.get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(io.grpc.Status.DEADLINE_EXCEEDED.getCode(), io.grpc.Status.fromThrowable(e).getCode());
    }
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
    Assert.assertTrue(
        waitedNanos.get()
            > TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElaspedBackoffMillis()));
  }

  @Test
  public void testCallableStale()
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    long preRunId = retryIdGenerator.get();
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
  }
  @Test
  public void testRequestTimer() {
    RequestManager requestManager = createTestRequestManager();
    Assert.assertTrue(requestManager.isStale());
    final AtomicLong currentTime = new AtomicLong(500);
    BulkMutation.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return currentTime.get();
      }
    };
    requestManager.lastRpcSentTime = currentTime.get();
    Assert.assertFalse(requestManager.isStale());
    currentTime.addAndGet(BulkMutation.MAX_RPC_WAIT_TIME - 1);
    Assert.assertFalse(requestManager.isStale());
    currentTime.addAndGet(2);
    Assert.assertTrue(requestManager.isStale());
  }

  @Test
  public void testAutoflushDisabled() throws ExecutionException, InterruptedException {
    // buffer a request, with a mocked success
    MutateRowRequest mutateRowRequest = createRequest();
    underTest.add(mutateRowRequest);

    verify(retryExecutorService, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void testAutoflush() throws InterruptedException, ExecutionException {
    // Setup a BulkMutation with autoflush enabled: the scheduled flusher will get captured by the scheduled executor mock
    underTest = new BulkMutation(TABLE_NAME, asyncExecutor, retryOptions,
        retryExecutorService, 1000, 1000000L, 1000L);

    ArgumentCaptor<Runnable> autoflusher = ArgumentCaptor.forClass(Runnable.class);
    ScheduledFuture f = Mockito.mock(ScheduledFuture.class);
    doReturn(f)
        .when(retryExecutorService).schedule(autoflusher.capture(), anyLong(), any(TimeUnit.class));

    // buffer a request, with a mocked success (for never it gets invoked)
    MutateRowRequest mutateRowRequest = createRequest();
    underTest.add(mutateRowRequest);

    when(mockFuture.get()).thenAnswer(new Answer<ImmutableList<MutateRowsResponse>>() {
      @Override
      public ImmutableList<MutateRowsResponse> answer(InvocationOnMock invocation) throws Throwable {
        MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
        responseBuilder.addEntriesBuilder()
            .setIndex(0)
            .getStatusBuilder()
            .setCode(io.grpc.Status.OK.getCode().value());
        return ImmutableList.of(responseBuilder.build());
      }
    });

    // Verify that the autoflusher was scheduled
    verify(retryExecutorService, times(1))
        .schedule(autoflusher.capture(), anyLong(), any(TimeUnit.class));

    // Verify that the request wasn't sent
    verify(asyncExecutor, never()).mutateRowsAsync(any(MutateRowsRequest.class));

    // Fake the triggering of the autoflusher
    autoflusher.getValue().run();

    // Verify that the request was sent
    verify(asyncExecutor, times(1)).mutateRowsAsync(any(MutateRowsRequest.class));
  }

  private void setupScheduler(final AtomicLong waitedNanos) {
    final long start = System.nanoTime();
    doAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return start + waitedNanos.get();
      }
    }).when(nanoClock).nanoTime();

    doAnswer(new Answer<ScheduledFuture<?>>() {
      @Override
      public ScheduledFuture<?> answer(InvocationOnMock invocation) throws Throwable {
        waitedNanos
            .addAndGet(TimeUnit.MILLISECONDS.toNanos(invocation.getArgumentAt(1, Long.class)));
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(retryExecutorService).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
  }

  private void setResponse(final io.grpc.Status code)
      throws InterruptedException, ExecutionException {
    when(mockFuture.isDone()).thenReturn(true);
    when(mockFuture.get()).thenAnswer(new Answer<ImmutableList<MutateRowsResponse>>() {
      @Override
      public ImmutableList<MutateRowsResponse> answer(InvocationOnMock invocation) throws Throwable {
        MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
        responseBuilder.addEntriesBuilder()
            .setIndex(0)
            .getStatusBuilder()
                .setCode(code.getCode().value());
        return ImmutableList.of(responseBuilder.build());
      }
    });
    underTest.currentBatch.run();
  }
}
