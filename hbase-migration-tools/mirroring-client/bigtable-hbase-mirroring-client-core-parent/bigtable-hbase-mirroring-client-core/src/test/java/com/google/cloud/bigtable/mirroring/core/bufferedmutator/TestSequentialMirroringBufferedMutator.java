/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.bufferedmutator;

import static com.google.cloud.bigtable.mirroring.core.TestHelpers.blockMethodCall;
import static com.google.cloud.bigtable.mirroring.core.bufferedmutator.MirroringBufferedMutatorCommon.makeConfigurationWithFlushThreshold;
import static com.google.cloud.bigtable.mirroring.core.bufferedmutator.MirroringBufferedMutatorCommon.mutateWithErrors;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestSequentialMirroringBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  Timestamper timestamper = new NoopTimestamper();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.spyedCachedPoolExecutor();

  public final MirroringBufferedMutatorCommon common = new MirroringBufferedMutatorCommon();

  private final List<Mutation> singletonMutation1 = Collections.singletonList(common.mutation1);
  private ListenableReferenceCounter referenceCounter = new ListenableReferenceCounter();

  @Test
  public void testBufferedWritesWithoutErrors() throws IOException, InterruptedException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(1)).mutate(singletonMutation1);
    verify(common.secondaryBufferedMutator, never()).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(2)).mutate(singletonMutation1);
    verify(common.secondaryBufferedMutator, never()).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(3)).mutate(singletonMutation1);
    verify(common.secondaryBufferedMutator, never()).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    bm.mutate(common.mutation1);
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(4)).mutate(singletonMutation1);
    verify(common.secondaryBufferedMutator, times(1))
        .mutate(
            Arrays.asList(common.mutation1, common.mutation1, common.mutation1, common.mutation1));
    verify(common.secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    // Flush is called, and only called once, because we've reached the threshold exactly once.
    // The threshold is set to 3.5 times mutation size when BufferedMutator is constructed above.
    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(4)).release();
  }

  @Test
  public void testBufferedMutatorFlush() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.flush();
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(3)).mutate(singletonMutation1);
    verify(common.secondaryBufferedMutator, times(1))
        .mutate(Arrays.asList(common.mutation1, common.mutation1, common.mutation1));
    verify(common.secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseFlushesWrites()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.close();
    // close() flushes primary BM and waits until the flush finishes. Secondary flush is scheduled
    // asynchronously.
    verify(common.primaryBufferedMutator, times(3)).mutate(singletonMutation1);
    verify(common.primaryBufferedMutator, times(1)).flush();
    // decrement initial reference value - now only asynchronous flush should hold a reference in
    // reference counter.
    referenceCounter.decrementReferenceCount();
    // wait until secondary flush is finished
    referenceCounter.getOnLastReferenceClosed().get(3, TimeUnit.SECONDS);
    verify(common.secondaryBufferedMutator, times(1))
        .mutate(Arrays.asList(common.mutation1, common.mutation1, common.mutation1));
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseIsIdempotent()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.close();
    bm.close();
    verify(common.primaryBufferedMutator, times(1)).flush();

    // wait until secondary flush is finished
    referenceCounter.decrementReferenceCount();
    referenceCounter.getOnLastReferenceClosed().get(3, TimeUnit.SECONDS);

    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testFlushesCanBeScheduledSimultaneouslyAndAreExecutedInOrder() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 0.5));

    final SettableFuture<Void> startFlush = SettableFuture.create();
    blockMethodCall(common.primaryBufferedMutator, startFlush).flush();

    InOrder inOrder1 = Mockito.inOrder(common.primaryBufferedMutator);
    InOrder inOrder2 = Mockito.inOrder(common.secondaryBufferedMutator);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation2);
    bm.mutate(common.mutation3);
    bm.mutate(common.mutation4);

    startFlush.set(null);
    executorServiceRule.waitForExecutor();

    inOrder1.verify(common.primaryBufferedMutator).mutate(Arrays.asList(common.mutation1));
    inOrder1.verify(common.primaryBufferedMutator).mutate(Arrays.asList(common.mutation2));
    inOrder1.verify(common.primaryBufferedMutator).mutate(Arrays.asList(common.mutation3));
    inOrder1.verify(common.primaryBufferedMutator).mutate(Arrays.asList(common.mutation4));

    inOrder2.verify(common.secondaryBufferedMutator).mutate(Arrays.asList(common.mutation1));
    inOrder2.verify(common.secondaryBufferedMutator).mutate(Arrays.asList(common.mutation2));
    inOrder2.verify(common.secondaryBufferedMutator).mutate(Arrays.asList(common.mutation3));
    inOrder2.verify(common.secondaryBufferedMutator).mutate(Arrays.asList(common.mutation4));

    verify(common.primaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.resourceReservation, times(4)).release();
  }

  @Test
  public void testErrorsReportedByPrimaryAreNotUsedBySecondary() throws IOException {
    doAnswer(
            mutateWithErrors(
                this.common.primaryBufferedMutatorParamsCaptor,
                common.primaryBufferedMutator,
                common.mutation1,
                common.mutation3))
        .when(common.primaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    try {
      bm.mutate(common.mutation1);
    } catch (IOException ignored) {

    }
    bm.mutate(common.mutation2);
    try {
      bm.mutate(common.mutation3);
    } catch (IOException ignored) {

    }
    bm.mutate(common.mutation4);
    executorServiceRule.waitForExecutor();
    verify(common.secondaryBufferedMutator, times(1))
        .mutate(Arrays.asList(common.mutation2, common.mutation4));
  }

  @Test
  public void testPrimaryAsyncFlushExceptionIsReportedOnNextMutateCall()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Mutation[] mutations =
        new Mutation[] {
          new Delete(Longs.toByteArray(0)),
          new Delete(Longs.toByteArray(1)),
          new Delete(Longs.toByteArray(2))
        };

    final SettableFuture<Void> flushesStarted = SettableFuture.create();
    final SettableFuture<Void> performFlush = SettableFuture.create();
    final AtomicInteger runningFlushes = new AtomicInteger(3);

    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                flushesStarted.set(null);
                performFlush.get();
                int value = runningFlushes.decrementAndGet();

                long id = Longs.fromByteArray(mutations[value].getRow());
                RetriesExhaustedWithDetailsException e =
                    new RetriesExhaustedWithDetailsException(
                        Arrays.asList((Throwable) new IOException(String.valueOf(id))),
                        Arrays.asList((Row) mutations[value]),
                        Arrays.asList("localhost:" + value));
                common
                    .primaryBufferedMutatorParamsCaptor
                    .getValue()
                    .getListener()
                    .onException(e, common.primaryBufferedMutator);
                return null;
              }
            })
        .when(common.primaryBufferedMutator)
        .flush();

    final BufferedMutator bm = getBufferedMutator(1);

    // Wait until flush is started to ensure that flushes are scheduled in the same order
    // as mutations.
    bm.mutate(mutations[2]);
    bm.mutate(mutations[1]);
    bm.mutate(mutations[0]);
    flushesStarted.get(1, TimeUnit.SECONDS);
    performFlush.set(null);

    // It waits for ExecutorService by shutting it down synchronously.
    // After this call we can't send new tasks to it.
    executorServiceRule.waitForExecutor();
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return SettableFuture.create();
              }
            })
        .when(executorServiceRule.executorService)
        .submit(any(Callable.class));

    verify(common.secondaryBufferedMutator, never()).flush();
    verify(common.resourceReservation, times(3)).release();

    // We previously used mutate() and thus scheduled asynchronous mutations.
    // In this scenario asynchronous flush() on primary threw an exception after mutate() returned.
    // Because of that we throw an exception the next time mutate() is called.
    try {
      bm.mutate(mutations[0]);
      verify(executorServiceRule.executorService, times(1)).submit(any(Callable.class));
      fail("Should have thrown");
    } catch (RetriesExhaustedWithDetailsException e) {
      assertThat(e.getNumExceptions()).isEqualTo(3);
      assertThat(Arrays.asList(e.getRow(0), e.getRow(1), e.getRow(2)))
          .containsExactly(mutations[0], mutations[1], mutations[2]);
      for (int i = 0; i < 3; i++) {
        Row r = e.getRow(i);
        long id = Longs.fromByteArray(r.getRow());
        assertThat(e.getCause(i).getMessage()).isEqualTo(String.valueOf(id));
        assertThat(e.getHostnamePort(i)).isEqualTo("localhost:" + id);
      }
    }

    verify(common.secondaryBufferedMutator, never()).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseWaitsForOngoingFlushesOnPrimaryMutatorOnly()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final List<? extends Mutation> mutations =
        Arrays.asList(
            new Delete(Longs.toByteArray(0)),
            new Delete(Longs.toByteArray(1)),
            new Delete(Longs.toByteArray(2)));

    long mutationSize = mutations.get(0).heapSize();

    final SettableFuture<Void> closeStarted = SettableFuture.create();
    final SettableFuture<Void> closeEnded = SettableFuture.create();
    final SettableFuture<Void> unlockPrimaryFlush = SettableFuture.create();

    final BufferedMutator bm = getBufferedMutator((long) 4 * mutationSize);

    blockMethodCall(common.primaryBufferedMutator, unlockPrimaryFlush).flush();

    bm.mutate(mutations);

    Thread t =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  closeStarted.set(null);
                  bm.close(); // calls flush
                  closeEnded.set(null);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            });
    t.start();
    closeStarted.get(1, TimeUnit.SECONDS);

    // best effort - we give the closing thread some time to run.
    try {
      closeEnded.get(1, TimeUnit.SECONDS);
      fail("Should have thrown.");
    } catch (TimeoutException ignored) {
    }

    // primary flushes have completed
    // only the flushed that was blocked.
    verify(common.primaryBufferedMutator, times(1)).flush();
    // and secondary is not yet called
    verify(common.secondaryBufferedMutator, times(0)).flush();
    assertThat(t.isAlive()).isTrue();

    unlockPrimaryFlush.set(null);
    closeEnded.get(3, TimeUnit.SECONDS);
    t.join(1000);
    assertThat(t.isAlive()).isFalse();
  }

  private BufferedMutator getBufferedMutator(long flushThreshold) throws IOException {
    return new SequentialMirroringBufferedMutator(
        common.primaryConnection,
        common.secondaryConnection,
        common.bufferedMutatorParams,
        makeConfigurationWithFlushThreshold(flushThreshold),
        common.flowController,
        executorServiceRule.executorService,
        common.secondaryWriteErrorConsumerWithMetrics,
        this.referenceCounter,
        timestamper,
        new MirroringTracer());
  }
}
