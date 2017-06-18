/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.rpc.Status;

import io.netty.util.concurrent.ScheduledFuture;

/**
 * Tests for {@link BulkMutation} that ensure that RPC failures and highly asynchronous calls work
 * correctly.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("rawtypes")
public class TestBulkMutationAwaitCompletion {

  private static final int OPERATIONS_PER_MUTATOR = 103;
  private static final int MUTATIONS_PER_RPC = 10;
  private static final int PER_BULK_MUTATION_OPERATIONS =
      (int) (Math.ceil(OPERATIONS_PER_MUTATOR / (double) MUTATIONS_PER_RPC));
  private static final Status OK_STATUS =
      Status.newBuilder().setCode(io.grpc.Status.Code.OK.value()).build();

  @Mock
  private BigtableDataClient mockClient;

  @Mock
  private ScheduledExecutorService mockScheduler;

  @Mock
  private ScheduledFuture mockScheduledFuture;

  @Mock
  private Logger mockLogger;

  private AtomicLong currentTime = new AtomicLong(500);
  private NanoClock clock = new NanoClock() {
    @Override
    public long nanoTime() {
      return currentTime.get();
    }
  };

  private List<Runnable> opCompletionRunnables;
  private List<Runnable> timeoutRunnables;
  private ScheduledExecutorService testExecutor;
  private List<OperationAccountant> accountants;
  private List<ListenableFuture<MutateRowResponse>> singleMutationFutures;
  private Logger originalBulkMutatorLog;
  private Logger originalOperationAccountantLog;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    testExecutor = Executors.newScheduledThreadPool(100);
    opCompletionRunnables = Collections.synchronizedList(new LinkedList<Runnable>());
    timeoutRunnables = Collections.synchronizedList(new ArrayList<Runnable>());
    accountants = Collections.synchronizedList(new ArrayList<OperationAccountant>());
    singleMutationFutures =
        Collections.synchronizedList(new ArrayList<ListenableFuture<MutateRowResponse>>());

    // Keep track of methods of completing mutateRowsAsync calls.  This method will add a Runnable
    // that can set the correct value of the MutateRowsResponse future.
    when(mockClient.mutateRowsAsync(any(MutateRowsRequest.class)))
        .thenAnswer(new Answer<ListenableFuture<List<MutateRowsResponse>>>() {
          @Override
          public ListenableFuture<List<MutateRowsResponse>> answer(final InvocationOnMock invocation)
              throws Throwable {
            final int responseCount =
                invocation.getArgumentAt(0, MutateRowsRequest.class).getEntriesCount();
            final SettableFuture<List<MutateRowsResponse>> future =
                SettableFuture.<List<MutateRowsResponse>> create();
            opCompletionRunnables.add(new Runnable() {
              @Override
              public void run() {
                MutateRowsResponse.Builder responses = MutateRowsResponse.newBuilder();
                for (int i = 0; i < responseCount; i++) {
                  responses.addEntries(Entry.newBuilder().setIndex(i).setStatus(OK_STATUS));
                }
                future.set(Arrays.asList(responses.build()));
              }
            });
            return future;
          }
        });
    when(mockScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .then(new Answer<ScheduledFuture>() {
          @Override
          public ScheduledFuture answer(final InvocationOnMock invocation) throws Throwable {
            timeoutRunnables.add(new Runnable() {
              @Override
              public void run() {
                long duration = invocation.getArgumentAt(1, Long.class);
                TimeUnit timeUnit = invocation.getArgumentAt(2, TimeUnit.class);
                currentTime.addAndGet(timeUnit.toNanos(duration));
                invocation.getArgumentAt(0, Runnable.class).run();
              }
            });
            return mockScheduledFuture;
          }
        });
    originalBulkMutatorLog = BulkMutation.LOG;
    originalOperationAccountantLog = OperationAccountant.LOG;
    BulkMutation.LOG = mockLogger;
    OperationAccountant.LOG = mockLogger;
    Runnable doTimout = new Runnable() {
      @Override
      public void run() {
        try {
          performTimeout();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    testExecutor.scheduleAtFixedRate(doTimout, 10, 10, TimeUnit.MILLISECONDS);
  }

  @After
  public void teardown(){
    testExecutor.shutdownNow();
    BulkMutation.LOG = originalBulkMutatorLog;
    OperationAccountant.LOG = originalOperationAccountantLog;
  }

  /**
   * Test to make sure that in the event of catastrophe, where there are absolutely no RPC
   * completions, that {@link BulkMutation} will clean up after some amount of time.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Test
  public void testBulkMutationNoCompletions() throws InterruptedException, ExecutionException {
    int count = 100;

    for (int i = 0; i < count; i++) {
      runOneBulkMutation();
    }

    verify(mockClient, times(count * PER_BULK_MUTATION_OPERATIONS))
        .mutateRowsAsync(any(MutateRowsRequest.class));

    performTimeout();
    confirmCompletion();
  }

  /**
   * This test performs a asynchronous bulk mutations similar to the way that Dataflow does. It
   * create multiple sets of bulk mutations asynchronously, and randomly complete RPCs until most of
   * them are complete. This test should also clean up RPCs that have not been completed using the
   * {@link BulkMutation} stale batch logic.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  @Test
  public void testBulkMutationSlowCompletions()
      throws InterruptedException, ExecutionException, TimeoutException {
    int count = 50;
    final AtomicInteger bulkMutationsOutstanding = new AtomicInteger(count);

    // Use {mutatorThreads} threads to create BulkMutations and submit some mutations per
    // bulkMutation.
    for (int i = 0; i < count; i++) {
      testExecutor.submit(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < 10; i++) {
            runOneBulkMutation();
          }
          bulkMutationsOutstanding.decrementAndGet();
        }
      });
    }

    // Randomly complete mutateRowAsync responses.  Leave some stragglers to test performTimeout();
    Future<?> completionFuture = testExecutor.submit(new Runnable() {
      int stragglers = (int) (Math.random() * 20);

      @Override
      public void run() {
        while (!done()) {
          // Sleep for a little bit to allow bulk mutations to submit requests.
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          // Complete some RPCs
          int rpcsToComplete = Math.min(100, opCompletionRunnables.size() - stragglers);
          for (int i = 0; i < rpcsToComplete; i++) {
            int mutationToComplete = (int) Math.floor(Math.random() * opCompletionRunnables.size());
            opCompletionRunnables.remove(mutationToComplete).run();
          }
        }
      }

      protected boolean done() {
        return bulkMutationsOutstanding.get() == 0 && opCompletionRunnables.size() <= stragglers;
      }
    });

    // Wait until all operations are complete. The had work should take no more than a couple of
    // seconds.
    completionFuture.get(10, TimeUnit.SECONDS);

    performTimeout();

    // Make sure that all operations completed.
    confirmCompletion();
  }

  /**
   * Creates a single {@link BulkMutation}, and adds {@link #OPERATIONS_PER_MUTATOR} Mutations to
   * it.
   */
  private void runOneBulkMutation() {
    MutateRowRequest request = TestBulkMutation.createRequest();
    OperationAccountant accountant = createOperationAccountant();
    BulkMutation bulkMutation = createBulkMutation(accountant);
    for (int i = 0; i < OPERATIONS_PER_MUTATOR; i++) {
      singleMutationFutures.add(bulkMutation.add(request));
    }
    bulkMutation.flush();
    accountants.add(accountant);
  }

  private OperationAccountant createOperationAccountant() {
    return new OperationAccountant(clock, OperationAccountant.DEFAULT_FINISH_WAIT_MILLIS);
  }

  protected BulkMutation createBulkMutation(OperationAccountant operationAccountant) {
    BulkOptions options = new BulkOptions.Builder()
        .setBulkMaxRowKeyCount(MUTATIONS_PER_RPC)
        .setBulkMaxRequestSize(1000000000)
        .build();
    BulkMutation bulkMutation =
        new BulkMutation(
            TestBulkMutation.TABLE_NAME,
            mockClient,
            operationAccountant,
            mockScheduler,
            options);
    bulkMutation.clock = clock;
    return bulkMutation;
  }

  // //////////////////////////  Helper methods for finishing up the BulkMutation process.

  /**
   * Ensures that all operations are timed out successfully. This method pings each
   * {@link OperationAccountant} every "minute" and runs the BulkMutation staleness logic until
   * either 100 "minutes" pass or all of the BulkMutations cleaned up their stale operations.
   *
   * @throws InterruptedException
   */
  protected void performTimeout() throws InterruptedException {
    ArrayList<Runnable> copy = new ArrayList<>(timeoutRunnables);
    for (Runnable runnable : copy) {
      runnable.run();
    }
    timeoutRunnables.removeAll(copy);
  }

  /**
   * Checks to make sure that for all accountants that
   * !{@link AsyncExecutor#hasInflightRequests()} and that for all futures that
   * {@link Future#isDone()}.
   */
  protected void confirmCompletion() {
    for (OperationAccountant accountant : accountants) {
      Assert.assertFalse(accountant.hasInflightOperations());
    }
    for (ListenableFuture<MutateRowResponse> future : singleMutationFutures) {
      Assert.assertTrue(future.isDone());
    }
  }
}
