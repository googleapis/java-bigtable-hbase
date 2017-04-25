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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.google.cloud.bigtable.grpc.async.BulkMutation.Batch;
import com.google.cloud.bigtable.grpc.async.BulkMutation.RequestManager;
import com.google.cloud.bigtable.grpc.async.OperationAccountant.ComplexOperationStalenessHandler;
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
  private static int OK = io.grpc.Status.OK.getCode().value();
  private static int MAX_ROW_COUNT = 10;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  AsyncExecutor asyncExecutor;

  @Mock
  ScheduledExecutorService retryExecutorService;

  @Mock
  OperationAccountant operationAccountant;

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
    when(asyncExecutor.getOperationAccountant()).thenReturn(operationAccountant);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
    doReturn(true).when(mockFuture).isDone();

    when(operationAccountant.registerComplexOperation(any(ComplexOperationStalenessHandler.class)))
        .then(new Answer<Long>() {
          @Override
          public Long answer(InvocationOnMock invocation) throws Throwable {
            return retryIdGenerator.incrementAndGet();
          }
        });

    underTest = createBulkMutation();
  }

  protected BulkMutation createBulkMutation() {
    return new BulkMutation(TABLE_NAME, asyncExecutor, retryOptions,
        retryExecutorService, MAX_ROW_COUNT, 1000000L);
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
    long preRunId = retryIdGenerator.get();
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    verify(operationAccountant, times(1)).onComplexOperationCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testCallableNotRetriedStatus()
      throws InterruptedException, ExecutionException, TimeoutException {
    long preRunId = retryIdGenerator.get();
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    ListenableFuture<List<MutateRowsResponse>> rowsFuture = SettableFuture.<List<MutateRowsResponse>>create();
    when(nanoClock.nanoTime()).thenReturn(1l);
    underTest.currentBatch.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture.isDone());
    setResponse(io.grpc.Status.NOT_FOUND);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    try {
      rowFuture.get(100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.assertEquals(StatusRuntimeException.class, e.getCause().getClass());
      verify(operationAccountant, times(1)).onComplexOperationCompletion(eq(retryIdGenerator.get()));
    }
  }

  @Test
  public void testRetriedStatus() throws InterruptedException, ExecutionException {
    long preRunId = retryIdGenerator.get();
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    Assert.assertFalse(rowFuture.isDone());

    Batch batch = underTest.currentBatch;
    // Send Deadline exceeded,
    setFuture(io.grpc.Status.DEADLINE_EXCEEDED);
    underTest.flush();
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    when(nanoClock.nanoTime()).thenReturn(1l);

    // Make sure that the request is scheduled
    Assert.assertFalse(rowFuture.isDone());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    verify(operationAccountant, times(0)).onComplexOperationCompletion(eq(retryIdGenerator.get()));

    // Make sure that a second try works.
    setFuture(io.grpc.Status.OK);
    batch.run();
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    Assert.assertTrue(rowFuture.isDone());
    verify(operationAccountant, times(1)).onComplexOperationCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testCallableTooFewStatuses() throws InterruptedException, ExecutionException {
    long preRunId = retryIdGenerator.get();
    ListenableFuture<MutateRowResponse> rowFuture1 = underTest.add(createRequest());
    ListenableFuture<MutateRowResponse> rowFuture2 = underTest.add(createRequest());
    SettableFuture<List<MutateRowsResponse>> rowsFuture = SettableFuture.<List<MutateRowsResponse>> create();
    Batch batch = underTest.currentBatch;
    batch.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());

    Assert.assertEquals(2, batch.getRequestCount());
    // Send only one response - this is poor server behavior.
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    when(nanoClock.nanoTime()).thenReturn(0l);
    Assert.assertEquals(1, batch.getRequestCount());

    // Make sure that the first request completes, but the second does not.
    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), rowFuture1.get());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    verify(operationAccountant, times(0)).onComplexOperationCompletion(eq(retryIdGenerator.get()));

    // Make sure that only the second request was sent.
    batch.run();
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    Assert.assertNull(underTest.currentBatch);
    Assert.assertTrue(rowFuture2.isDone());
    verify(operationAccountant, times(1)).onComplexOperationCompletion(eq(retryIdGenerator.get()));
  }

  @Test
  public void testRunOutOfTime() throws InterruptedException, ExecutionException, TimeoutException {
    long preRunId = retryIdGenerator.get();
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());

    AtomicLong waitedNanos = new AtomicLong();
    setupScheduler(waitedNanos);
    setResponse(io.grpc.Status.DEADLINE_EXCEEDED);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());
    try {
      rowFuture.get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(io.grpc.Status.DEADLINE_EXCEEDED.getCode(), io.grpc.Status.fromThrowable(e).getCode());
    }
    verify(operationAccountant, times(1)).onComplexOperationCompletion(eq(retryIdGenerator.get()));
    Assert.assertTrue(
        waitedNanos.get()
            > TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElaspedBackoffMillis()));
  }

  @Test
  public void testCallableStale()
      throws InterruptedException, ExecutionException, TimeoutException {
    long preRunId = retryIdGenerator.get();
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    setResponse(io.grpc.Status.OK);
    Assert.assertEquals(preRunId + 1, retryIdGenerator.get());

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    verify(operationAccountant, times(1)).onComplexOperationCompletion(eq(retryIdGenerator.get()));
  }
  @Test
  public void testRequestTimer() {
    RequestManager requestManager = createTestRequestManager();
    Assert.assertFalse(requestManager.wasSent());
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
  public void testConcurrentBatches()
      throws InterruptedException, ExecutionException, TimeoutException {
    final List<ListenableFuture<MutateRowResponse>> futures =
        Collections.synchronizedList(new ArrayList<ListenableFuture<MutateRowResponse>>());
    final MutateRowRequest mutateRowRequest = createRequest();
    final int batchCount = 100;
    final int concurrentBulkMutationCount = 200;

    MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
    for (int i = 0; i < MAX_ROW_COUNT; i++) {
      responseBuilder.addEntriesBuilder().setIndex(i).getStatusBuilder().setCode(OK);
    }
    final MutateRowsResponse response = responseBuilder.build();

    when(mockFuture.get()).thenAnswer(new Answer<ImmutableList<MutateRowsResponse>>() {
      @Override
      public ImmutableList<MutateRowsResponse> answer(InvocationOnMock invocation)
          throws Throwable {
        return ImmutableList.of(response);
      }
    });

    Runnable r = new Runnable() {

      @Override
      public void run() {
        BulkMutation bulkMutation = createBulkMutation();
        for (int i = 0; i < batchCount * MAX_ROW_COUNT; i++) {
          futures.add(bulkMutation.add(mutateRowRequest));
        }
        bulkMutation.flush();
      }
    };
    ExecutorService pool = Executors.newFixedThreadPool(100);

    for (int i = 0; i < concurrentBulkMutationCount; i++) {
      pool.execute(r);
    }
    pool.shutdown();
    pool.awaitTermination(100, TimeUnit.SECONDS);
    when(mockFuture.isDone()).thenReturn(true);
    for (ListenableFuture<MutateRowResponse> future : futures) {
      MutateRowResponse result = future.get(100, TimeUnit.MILLISECONDS);
      Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
      Assert.assertTrue(future.isDone());
    }
    pool.shutdownNow();

    verify(operationAccountant, times(batchCount * concurrentBulkMutationCount))
        .onComplexOperationCompletion(anyLong());
  }

  @Test
  public void testAutoflushDisabled() {
    // buffer a request, with a mocked success
    MutateRowRequest mutateRowRequest = createRequest();
    underTest.add(mutateRowRequest);

    verify(retryExecutorService, never())
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
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
    setFuture(code);
    underTest.flush();
  }

  protected void setFuture(final io.grpc.Status code)
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
  }
}
