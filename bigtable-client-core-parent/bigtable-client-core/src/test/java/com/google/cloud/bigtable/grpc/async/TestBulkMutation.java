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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.RpcThrottler;
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
  private final static String TABLE_NAME = "table";
  private final static ByteString QUALIFIER = ByteString.copyFrom("qual".getBytes());

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

  private AtomicLong retryIdGenerator = new AtomicLong();
  private BulkMutation.Batch underTest;

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

    when(rpcThrottler.registerRetry()).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return retryIdGenerator.incrementAndGet();
      }
    });

    underTest = new BulkMutation.Batch(TABLE_NAME, asyncExecutor, retryOptions, retryExecutorService,
      1000, 1000000L);
  }

  @Test
  public void testAdd() {
    MutateRowRequest mutateRowRequest = createRequest();
    BulkMutation.RequestManager requestManager = new BulkMutation.RequestManager(TABLE_NAME,
        BigtableClientMetrics.meter(MetricLevel.Trace, "test.bulk"));
    requestManager.add(null, BulkMutation.convert(mutateRowRequest));
    MutateRowsRequest expected = MutateRowsRequest.newBuilder()
        .setTableName(TABLE_NAME)
        .addEntries(Entry.newBuilder().addMutations(mutateRowRequest.getMutations(0)).build())
        .build();
    Assert.assertEquals(expected, requestManager.build());
  }

  protected static MutateRowRequest createRequest() {
    return MutateRowRequest.newBuilder().addMutations(
      Mutation.newBuilder()
        .setSetCell(
            SetCell.newBuilder()
              .setFamilyName("cf1")
              .setColumnQualifier(QUALIFIER))
            .build())
        .build();
  }

  @Test
  public void testCallableSuccess()
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    setResponse(io.grpc.Status.OK);
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
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture.isDone());
    setResponse(io.grpc.Status.NOT_FOUND);

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
    setResponse(io.grpc.Status.DEADLINE_EXCEEDED);
    when(nanoClock.nanoTime()).thenReturn(1l);

    // Make sure that the request is scheduled
    Assert.assertFalse(rowFuture.isDone());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    verify(rpcThrottler, times(0)).onRetryCompletion(eq(retryIdGenerator.get()));

    // Make sure that a second try works.
    setResponse(io.grpc.Status.OK);
    Assert.assertTrue(rowFuture.isDone());
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testCallableTooFewStatuses() throws InterruptedException, ExecutionException {
    ListenableFuture<MutateRowResponse> rowFuture1 = underTest.add(createRequest());
    ListenableFuture<MutateRowResponse> rowFuture2 = underTest.add(createRequest());
    SettableFuture<List<MutateRowsResponse>> rowsFuture = SettableFuture.<List<MutateRowsResponse>> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());

    Assert.assertEquals(2, underTest.getRequestCount());
    // Send only one response - this is poor server behavior.
    setResponse(io.grpc.Status.OK);
    when(nanoClock.nanoTime()).thenReturn(0l);
    Assert.assertEquals(1, underTest.getRequestCount());

    // Make sure that the first request completes, but the second does not.
    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), rowFuture1.get());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    verify(rpcThrottler, times(0)).onRetryCompletion(eq(retryIdGenerator.get()));

    // Make sure that only the second request was sent.
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(0, underTest.getRequestCount());
    Assert.assertTrue(rowFuture2.isDone());
    verify(rpcThrottler, times(1)).onRetryCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testRunOutOfTime() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());

    AtomicLong waitedNanos = new AtomicLong();
    setupScheduler(waitedNanos);
    setResponse(io.grpc.Status.DEADLINE_EXCEEDED);
    try {
      rowFuture.get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(io.grpc.Status.DEADLINE_EXCEEDED.getCode(), io.grpc.Status.fromThrowable(e).getCode());
    }
    Assert.assertTrue(
        waitedNanos.get()
            > TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElaspedBackoffMillis()));
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
    MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
    responseBuilder.addEntriesBuilder()
        .setIndex(0)
        .getStatusBuilder().setCode(code.getCode().value());
    when(mockFuture.get()).thenReturn(ImmutableList.of(responseBuilder.build()));
    underTest.run();
  }
}
