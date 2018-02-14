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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation.Batch;
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
  private final static BulkOptions BULK_OPTIONS = new BulkOptions.Builder()
      .setBulkMaxRequestSize(1000000L).setBulkMaxRowKeyCount(MAX_ROW_COUNT).build();

  static MutateRowsRequest.Entry createEntry() {
    SetCell setCell = SetCell.newBuilder()
        .setFamilyName("cf1")
        .setColumnQualifier(QUALIFIER)
        .build();
    return MutateRowsRequest.Entry.newBuilder()
        .setRowKey(ByteString.copyFrom("SomeKey".getBytes()))
        .addMutations(Mutation.newBuilder().setSetCell(setCell))
        .build();
  }

  @Mock private BigtableDataClient client;
  @Mock private ScheduledExecutorService retryExecutorService;
  @Mock private ScheduledFuture mockScheduledFuture;

  private AtomicLong time;
  private AtomicInteger timeIncrementCount = new AtomicInteger();
  private SettableFuture<List<MutateRowsResponse>> future;
  private BulkMutation underTest;
  private OperationAccountant operationAccountant;

  @Before
  public void setup() {
    time = new AtomicLong(System.nanoTime());
    NanoClock clock = new NanoClock() {
      @Override
      public long nanoTime() {
        timeIncrementCount.incrementAndGet();
        return time.get();
      }
    };
    MockitoAnnotations.initMocks(this);

    future = SettableFuture.create();
    when(client.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(future);
    operationAccountant =
        new OperationAccountant(clock, OperationAccountant.DEFAULT_FINISH_WAIT_MILLIS);
    underTest = createBulkMutation();
    underTest.clock = clock;
  }

  @Test
  public void testIsStale() {
    underTest.add(createEntry());
    underTest.currentBatch.lastRpcSentTimeNanos = time.get();
    Assert.assertFalse(underTest.currentBatch.isStale());
    time.addAndGet(BulkMutation.MAX_RPC_WAIT_TIME_NANOS);
    Assert.assertTrue(underTest.currentBatch.isStale());
  }

  @Test
  public void testAdd() {
    MutateRowsRequest.Entry entry = createRequestEntry();
    underTest.add(entry);
    underTest.sendUnsent();
    MutateRowsRequest expected = MutateRowsRequest.newBuilder()
        .setTableName(TABLE_NAME.toString())
        .addEntries(entry)
        .build();

    verify(client, times(1)).mutateRowsAsync(eq(expected));
  }

  public static MutateRowsRequest.Entry createRequestEntry() {
    SetCell setCell = SetCell.newBuilder()
        .setFamilyName("cf1")
        .setColumnQualifier(QUALIFIER)
        .build();
    ByteString rowKey = ByteString.copyFrom("SomeKey".getBytes());
    return MutateRowsRequest.Entry.newBuilder()
        .setRowKey(rowKey)
        .addMutations(Mutation.newBuilder()
          .setSetCell(setCell))
        .build();
  }

  @Test
  public void testCallableSuccess() throws Exception {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequestEntry());
    setResponse(Status.OK);

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    Assert.assertFalse(operationAccountant.hasInflightOperations());
  }

  @Test
  public void testCallableFail() throws Exception {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequestEntry());
    Assert.assertFalse(rowFuture.isDone());
    setResponse(Status.NOT_FOUND);
    Assert.assertTrue(rowFuture.isDone());

    try {
      rowFuture.get();
    } catch (ExecutionException e) {
      Assert.assertEquals(Status.NOT_FOUND.getCode(), Status.fromThrowable(e).getCode());
      Assert.assertFalse(operationAccountant.hasInflightOperations());
    }
  }

  public void testCallableTooFewStatuses() throws Exception {
    ListenableFuture<MutateRowResponse> rowFuture1 = underTest.add(createRequestEntry());
    ListenableFuture<MutateRowResponse> rowFuture2 = underTest.add(createRequestEntry());
    Batch batch = underTest.currentBatch;
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Assert.assertEquals(2, batch.getRequestCount());

    // Send only one response - this is poor server behavior.
    setResponse(Status.OK);

    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertTrue(rowFuture2.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), rowFuture1.get());

    // This should not throw an exception
    rowFuture1.get();

    try {
      rowFuture2.get();
      Assert.fail("Expected exception");
    } catch (Exception e) {
      Assert.assertEquals(Status.Code.INTERNAL, Status.fromThrowable(e).getCode());
    }
  }

  public void testRunOutOfTime() throws Exception {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequestEntry());
    setResponse(Status.DEADLINE_EXCEEDED);
    Assert.assertTrue(rowFuture.isDone());
    try {
      rowFuture.get();
      Assert.fail("Expected exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(Status.DEADLINE_EXCEEDED.getCode(),
        Status.fromThrowable(e).getCode());
    }

    Assert.assertFalse(operationAccountant.hasInflightOperations());
  }

  @Test
  public void testCallableStale() throws Exception {
    ListenableFuture<MutateRowResponse> rowFuture = underTest.add(createRequestEntry());
    setResponse(Status.OK);

    MutateRowResponse result = rowFuture.get(10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(MutateRowResponse.getDefaultInstance(), result);
    Assert.assertFalse(operationAccountant.hasInflightOperations());
  }

  @Test
  public void testRequestTimer() {
    underTest.add(createEntry());
    Assert.assertFalse(underTest.currentBatch.wasSent());
    underTest.currentBatch.lastRpcSentTimeNanos = time.get();
    Assert.assertFalse(underTest.currentBatch.isStale());
    time.addAndGet(BulkMutation.MAX_RPC_WAIT_TIME_NANOS - 1);
    Assert.assertFalse(underTest.currentBatch.isStale());
    time.addAndGet(1);
    Assert.assertTrue(underTest.currentBatch.isStale());
  }

  @Test
  public void testConcurrentBatches() throws Exception {
    final List<ListenableFuture<MutateRowResponse>> futures =
        Collections.synchronizedList(new ArrayList<ListenableFuture<MutateRowResponse>>());
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
          futures.add(bulkMutation.add(createRequestEntry()));
        }
        bulkMutation.sendUnsent();
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

    Assert.assertFalse(operationAccountant.hasInflightOperations());
  }

  @Test
  public void testAutoflushDisabled() {
    // buffer a request, with a mocked success
    underTest.add(createRequestEntry());
    verify(retryExecutorService, never())
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAutoflush() throws Exception {
    // Setup a BulkMutation with autoflush enabled: the scheduled flusher will get captured by the
    // scheduled executor mock
    underTest = new BulkMutation(TABLE_NAME, client, operationAccountant,
        retryExecutorService, new BulkOptions.Builder().setAutoflushMs(1000L).build());
    ArgumentCaptor<Runnable> autoflusher = ArgumentCaptor.forClass(Runnable.class);
    when(retryExecutorService.schedule(autoflusher.capture(), anyLong(), any(TimeUnit.class)))
        .thenReturn(mockScheduledFuture);

    // buffer a request, with a mocked success (for never it gets invoked)
    underTest.add(createRequestEntry());

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
    setupScheduler(true);

    ListenableFuture<MutateRowResponse> addFuture = underTest.add(createRequestEntry());

    // TODO(igorbernstein2): this should either block & throw or return a failing future
    // force the batch to be sent
    underTest.flush();

    // TODO(igorbernstein2): Should this throw as well?
    // force the executor to checking for stale requests
    operationAccountant.awaitCompletion();

    Assert.assertTrue(addFuture.isDone());
    try {
      addFuture.get();
      Assert.fail("Expected an exception");
    } catch (Exception e) {
      Status fromThrowable = Status.fromThrowable(e);
      Assert.assertEquals(Status.Code.INTERNAL, fromThrowable.getCode());
      Assert.assertTrue(fromThrowable.getDescription().toLowerCase().contains("stale"));
    }
  }

  @Test
  public void testLotsOfMutations() throws Exception {
    MutateRowsRequest.Entry smallRequest = createRequestEntry();
    MutateRowsRequest.Entry.Builder bigRequest = createRequestEntry().toBuilder();
    bigRequest.addAllMutations(
        Collections.nCopies(20_000, bigRequest.getMutations(0))
    );
    MutateRowsRequest.Entry lastRequest = bigRequest.clone().setRowKey(
        ByteString.copyFrom("SomeOtherKey".getBytes())
    ).build();

    underTest.add(smallRequest);
    underTest.add(bigRequest.build());
    underTest.add(bigRequest.build());
    underTest.add(lastRequest);

    Assert.assertTrue(underTest.currentBatch.builder.getEntriesList().contains(smallRequest));

    underTest.add(lastRequest);
    Assert.assertFalse(underTest.currentBatch.builder.getEntriesList().contains(smallRequest));
    Assert.assertTrue(underTest.currentBatch.builder.getEntriesList().contains(lastRequest));
  }

  private BulkMutation createBulkMutation() {
    return new BulkMutation(TABLE_NAME, client, operationAccountant, retryExecutorService,
        BULK_OPTIONS);
  }

  private void setupScheduler(final boolean inNewThread) {
    when(retryExecutorService.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .then(new Answer<ScheduledFuture>() {
          @Override
          public ScheduledFuture<?> answer(InvocationOnMock invocation) throws Throwable {
            TimeUnit timeUnit = invocation.getArgumentAt(2, TimeUnit.class);
            long nanos = timeUnit.toNanos(invocation.getArgumentAt(1, Long.class));
            time.addAndGet(nanos);
            Runnable runnable = invocation.getArgumentAt(0, Runnable.class);
            if (inNewThread) {
              new Thread(runnable).start();
            } else {
              runnable.run();
            }
            return null;
          }
        });
  }

  private void setResponse(Status code) {
    MutateRowsResponse.Builder responseBuilder = MutateRowsResponse.newBuilder();
    responseBuilder.addEntriesBuilder()
        .setIndex(0)
        .getStatusBuilder()
            .setCode(code.getCode().value());
    future.set(Arrays.asList(responseBuilder.build()));
    underTest.sendUnsent();
  }
}
