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
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
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
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation.Batch;
import com.google.cloud.bigtable.grpc.async.BulkMutation.RequestManager;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import io.grpc.Status;

/**
 * Tests for {@link BulkMutation}
 */
@SuppressWarnings("rawtypes")
@RunWith(JUnit4.class)
public class TestBulkMutation {
  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName("project", "instance").toTableName("table");
  private final static ByteString QUALIFIER = ByteString.copyFrom("qual".getBytes());
  private final static int MAX_ROW_COUNT = 10;

  @Mock private BigtableDataClient client;
  @Mock private ScheduledExecutorService retryExecutorService;
  @Mock private NanoClock nanoClock;

  private AsyncExecutor asyncExecutor;
  private SettableFuture<List<MutateRowsResponse>> future;
  private RetryOptions retryOptions;
  private BulkMutation underTest;
  private OperationAccountant operationAccountant;

  @Before
  public void setup() throws InterruptedException {
    MockitoAnnotations.initMocks(this);
    retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);

    future = SettableFuture.create();
    when(client.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(future);
    operationAccountant = new OperationAccountant(nanoClock, OperationAccountant.DEFAULT_FINISH_WAIT_MILLIS);
    asyncExecutor = new AsyncExecutor(client, new ResourceLimiter(1000, 10), operationAccountant);
    underTest = createBulkMutation();
  }

  @Test
  public void testAdd() {
    BulkMutationsStats.reset();
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
    Assert.assertEquals(0, BulkMutationsStats.getInstance().getMutationTimer().getCount());
    Assert.assertEquals(0, BulkMutationsStats.getInstance().getMutationMeter().getCount());
    Assert.assertEquals(0, BulkMutationsStats.getInstance().getThrottlingTimer().getCount());
  }

  private RequestManager createTestRequestManager() {
    return new BulkMutation.RequestManager(TABLE_NAME.toString(),
        BigtableClientMetrics.meter(MetricLevel.Trace, "test.bulk"), underTest.clock);
  }

  public static MutateRowRequest createRequest() {
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
    setResponse(Status.OK);

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    Assert.assertFalse(asyncExecutor.hasInflightRequests());
  }

  @Test
  public void testCallableNotRetriedStatus()
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    when(nanoClock.nanoTime()).thenReturn(1l);
    Assert.assertFalse(rowFuture.isDone());

    setResponse(Status.NOT_FOUND);

    try {
      rowFuture.get(100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.assertEquals(Status.NOT_FOUND.getCode(), Status.fromThrowable(e).getCode());
      Assert.assertFalse(asyncExecutor.hasInflightRequests());
    }
  }

  @Test
  public void testRetriedStatus() throws InterruptedException, ExecutionException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    Assert.assertFalse(rowFuture.isDone());
    setRpcFailure(Status.DEADLINE_EXCEEDED);

    // Send Deadline exceeded,
    when(nanoClock.nanoTime()).thenReturn(1l);

    // Make sure that the request is scheduled
    Assert.assertFalse(rowFuture.isDone());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    Assert.assertTrue(asyncExecutor.hasInflightRequests());

    // Make sure that a second try works.
    future.set(createResponse(Status.OK));
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertFalse(asyncExecutor.hasInflightRequests());
  }

  @Test
  public void testCallableTooFewStatuses() throws InterruptedException, ExecutionException {
    ListenableFuture<MutateRowResponse> rowFuture1 = underTest.add(createRequest());
    ListenableFuture<MutateRowResponse> rowFuture2 = underTest.add(createRequest());
    Batch batch = underTest.currentBatch;
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Assert.assertEquals(2, batch.getRequestCount());
    setResponse(Status.OK);
    // Send only one response - this is poor server behavior.

    when(nanoClock.nanoTime()).thenReturn(0l);
    Assert.assertEquals(1, batch.getRequestCount());

    // Make sure that the first request completes, but the second does not.
    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), rowFuture1.get());
    verify(retryExecutorService, times(1)).schedule(any(Runnable.class), anyLong(),
      same(TimeUnit.MILLISECONDS));
    Assert.assertTrue(asyncExecutor.hasInflightRequests());

    // Make sure that only the second request was sent.
    batch.run();
    Assert.assertNull(underTest.currentBatch);
    Assert.assertTrue(rowFuture2.isDone());
    Assert.assertFalse(asyncExecutor.hasInflightRequests());
  }

  @Test
  public void testRunOutOfTime() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    AtomicLong waitedNanos = setupScheduler();
    setResponse(Status.DEADLINE_EXCEEDED);
    try {
      rowFuture.get(3, TimeUnit.SECONDS);
      Assert.fail("Expected exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(Status.DEADLINE_EXCEEDED.getCode(),
        Status.fromThrowable(e).getCode());
    }
    Assert.assertFalse(asyncExecutor.hasInflightRequests());
    Assert.assertTrue(
        waitedNanos.get()
            > TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElaspedBackoffMillis()));
  }

  @Test
  public void testCallableStale()
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequest());
    setResponse(Status.OK);

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    Assert.assertFalse(asyncExecutor.hasInflightRequests());
  }

  @Test
  public void testRequestTimer() {
    final AtomicLong currentTime = new AtomicLong(System.nanoTime());
    underTest.clock = new NanoClock() {
      @Override
      public long nanoTime() {
        return currentTime.get();
      }
    };
    RequestManager requestManager = createTestRequestManager();
    Assert.assertFalse(requestManager.wasSent());
    requestManager.lastRpcSentTimeNanos = currentTime.get();
    Assert.assertFalse(requestManager.isStale());
    currentTime.addAndGet(BulkMutation.MAX_RPC_WAIT_TIME_NANOS - 1);
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
    final int batchCount = 10;
    final int concurrentBulkMutationCount = 50;

    MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
    for (int i = 0; i < MAX_ROW_COUNT; i++) {
      responseBuilder.addEntriesBuilder().setIndex(i).getStatusBuilder()
          .setCode(Status.Code.OK.value());
    }
    future.set(Arrays.asList(responseBuilder.build()));
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
    for (ListenableFuture<MutateRowResponse> future : futures) {
      Assert.assertTrue(future.isDone());
    }
    pool.shutdownNow();

    Assert.assertFalse(asyncExecutor.hasInflightRequests());
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
        retryExecutorService, new BulkOptions.Builder().setAutoflushMs(1000L).build());

    ArgumentCaptor<Runnable> autoflusher = ArgumentCaptor.forClass(Runnable.class);
    ScheduledFuture f = Mockito.mock(ScheduledFuture.class);
    doReturn(f)
        .when(retryExecutorService).schedule(autoflusher.capture(), anyLong(), any(TimeUnit.class));

    // buffer a request, with a mocked success (for never it gets invoked)
    MutateRowRequest mutateRowRequest = createRequest();
    underTest.add(mutateRowRequest);

    // Verify that the autoflusher was scheduled
    verify(retryExecutorService, times(1))
        .schedule(autoflusher.capture(), anyLong(), any(TimeUnit.class));

    // Verify that the request wasn't sent
    verify(client, never()).mutateRowsAsync(any(MutateRowsRequest.class));

    // Fake the triggering of the autoflusher
    autoflusher.getValue().run();

    // Verify that the request was sent
    verify(client, times(1)).mutateRowsAsync(any(MutateRowsRequest.class));
  }

  @Test
  public void testMissingResponse() throws Exception {
    underTest.clock = nanoClock;
    ListenableFuture<MutateRowResponse> addFuture = underTest.add(createRequest());

    // TODO(igorbernstein2): this should either block & throw or return a failing future
    // force the batch to be sent
    underTest.flush();

    // since we don't mock the response from the client, this rpc will just hang

    // Fast forward time
    doAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return BulkMutation.MAX_RPC_WAIT_TIME_NANOS + 1;
      }
    }).when(nanoClock).nanoTime();

    // TODO(igorbernstein2): Should this throw as well?
    // force the executor to checking for stale requests
    asyncExecutor.flush();

    try {
      addFuture.get();
      Assert.fail("Expected an exception");
    } catch(ExecutionException executionException) {
      // Unwrap the exception
      if (!(executionException.getCause() instanceof StatusRuntimeException)) {
        throw executionException;
      }
      StatusRuntimeException e = (StatusRuntimeException) executionException.getCause();

      // Make sure that we caught a Stale request exception
      if (!(e.getStatus().getCode() == Code.UNKNOWN && e.getMessage().contains("Stale"))) {
        throw e;
      }
    }
  }

  private BulkMutation createBulkMutation() {
    BulkOptions options = new BulkOptions.Builder().setBulkMaxRequestSize(1000000L)
        .setBulkMaxRowKeyCount(MAX_ROW_COUNT).build();
    return new BulkMutation(TABLE_NAME, asyncExecutor, retryOptions, retryExecutorService, options);
  }

  private AtomicLong setupScheduler() {
    final AtomicLong waitedNanos = new AtomicLong();
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
    return waitedNanos;
  }

  private void setResponse(Status code)
      throws InterruptedException, ExecutionException {
    underTest.flush();
    future.set(createResponse(code));
  }

  private void setRpcFailure(Status status) {
    Batch batch = underTest.currentBatch;
    underTest.flush();
    batch.performFullRetry(new AtomicReference<Long>(), status);
  }

  private List<MutateRowsResponse> createResponse(Status code) {
    MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
    responseBuilder.addEntriesBuilder()
        .setIndex(0)
        .getStatusBuilder()
            .setCode(code.getCode().value());
    return Arrays.asList(responseBuilder.build());
  }
}
