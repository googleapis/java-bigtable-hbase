/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestMirroringBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();

  @Mock BufferedMutator primaryBufferedMutator;
  @Mock BufferedMutator secondaryBufferedMutator;

  @Mock Connection primaryConnection;
  @Mock Connection secondaryConnection;
  @Mock FlowController flowController;
  @Mock ResourceReservation resourceReservation;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumerWithMetrics;

  BufferedMutatorParams bufferedMutatorParams =
      new BufferedMutatorParams(TableName.valueOf("test1"));

  ArgumentCaptor<BufferedMutatorParams> primaryBufferedMutatorParamsCaptor;
  ArgumentCaptor<BufferedMutatorParams> secondaryBufferedMutatorParamsCaptor;

  @Before
  public void setUp() throws IOException {
    this.primaryBufferedMutatorParamsCaptor = ArgumentCaptor.forClass(BufferedMutatorParams.class);
    doReturn(primaryBufferedMutator)
        .when(primaryConnection)
        .getBufferedMutator(primaryBufferedMutatorParamsCaptor.capture());

    this.secondaryBufferedMutatorParamsCaptor =
        ArgumentCaptor.forClass(BufferedMutatorParams.class);
    doReturn(secondaryBufferedMutator)
        .when(secondaryConnection)
        .getBufferedMutator(secondaryBufferedMutatorParamsCaptor.capture());

    resourceReservation = setupFlowControllerMock(flowController);
  }

  @Test
  public void testBufferedWritesWithoutErrors() throws IOException, InterruptedException {
    Mutation mutation = new Delete("key1".getBytes());
    long mutationSize = mutation.heapSize();

    BufferedMutator bm = getBufferedMutator((long) (mutationSize * 3.5));

    bm.mutate(mutation);
    verify(primaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    bm.mutate(mutation);
    verify(primaryBufferedMutator, times(2)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    bm.mutate(mutation);
    verify(primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    bm.mutate(mutation);
    Thread.sleep(300);
    executorServiceRule.waitForExecutor();
    verify(primaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    verify(secondaryBufferedMutator, times(1)).flush();
    verify(resourceReservation, times(4)).release();
  }

  @Test
  public void testBufferedMutatorFlush() throws IOException {
    Mutation mutation = new Delete("key1".getBytes());
    long mutationSize = mutation.heapSize();

    BufferedMutator bm = getBufferedMutator((long) (mutationSize * 3.5));

    bm.mutate(mutation);
    bm.mutate(mutation);
    bm.mutate(mutation);
    bm.flush();
    executorServiceRule.waitForExecutor();
    verify(primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, never()).mutate(any(Mutation.class));
    verify(secondaryBufferedMutator, times(1)).flush();
    verify(resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseFlushesWrites() throws IOException {
    Mutation mutation = new Delete("key1".getBytes());
    long mutationSize = mutation.heapSize();

    BufferedMutator bm = getBufferedMutator((long) (mutationSize * 3.5));

    bm.mutate(mutation);
    bm.mutate(mutation);
    bm.mutate(mutation);
    bm.close();
    verify(primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(secondaryBufferedMutator, times(1)).flush();
    verify(resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseIsIdempotent() throws IOException {
    Mutation mutation = new Delete("key1".getBytes());
    long mutationSize = mutation.heapSize();

    BufferedMutator bm = getBufferedMutator((long) (mutationSize * 3.5));

    bm.mutate(mutation);
    bm.mutate(mutation);
    bm.mutate(mutation);
    bm.close();
    bm.close();
    verify(secondaryBufferedMutator, times(1)).flush();
    verify(resourceReservation, times(3)).release();
  }

  @Test
  public void testFlushesCanBeScheduledSimultaneously()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    Mutation mutation = new Delete("key1".getBytes());
    long mutationSize = mutation.heapSize();

    final AtomicInteger ongoingFlushes = new AtomicInteger(0);
    final SettableFuture<Void> allFlushesStarted = SettableFuture.create();
    final SettableFuture<Void> endFlush = SettableFuture.create();

    doAnswer(blockedFlushes(ongoingFlushes, allFlushesStarted, endFlush, 4))
        .when(primaryBufferedMutator)
        .flush();

    BufferedMutator bm = getBufferedMutator((long) (mutationSize * 1.5));

    bm.mutate(mutation);
    bm.mutate(mutation);

    bm.mutate(mutation);
    bm.mutate(mutation);

    bm.mutate(mutation);
    bm.mutate(mutation);

    bm.mutate(mutation);
    bm.mutate(mutation);

    allFlushesStarted.get(3, TimeUnit.SECONDS);
    assertThat(ongoingFlushes.get()).isEqualTo(4);
    endFlush.set(null);
    executorServiceRule.waitForExecutor();
    verify(secondaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(resourceReservation, times(8)).release();
  }

  @Test
  public void testErrorsReportedByPrimaryAreNotUsedBySecondary() throws IOException {
    final Mutation mutation1 = new Delete("key1".getBytes());
    final Mutation mutation2 = new Delete("key2".getBytes());
    final Mutation mutation3 = new Delete("key3".getBytes());
    final Mutation mutation4 = new Delete("key4".getBytes());

    long mutationSize = mutation1.heapSize();

    doAnswer(
            mutateWithErrors(
                this.primaryBufferedMutatorParamsCaptor,
                primaryBufferedMutator,
                mutation1,
                mutation3))
        .when(primaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    BufferedMutator bm = getBufferedMutator((long) (mutationSize * 3.5));

    bm.mutate(mutation1);
    bm.mutate(mutation2);
    bm.mutate(mutation3);
    bm.mutate(mutation4);
    executorServiceRule.waitForExecutor();
    verify(secondaryBufferedMutator, times(1)).mutate(Arrays.asList(mutation2, mutation4));
  }

  @Test
  public void testErrorsReportedBySecondaryAreReportedAsWriteErrors() throws IOException {
    final Mutation mutation1 = new Delete("key1".getBytes());
    final Mutation mutation2 = new Delete("key2".getBytes());
    final Mutation mutation3 = new Delete("key3".getBytes());
    final Mutation mutation4 = new Delete("key4".getBytes());

    long mutationSize = mutation1.heapSize();

    doAnswer(
            mutateWithErrors(
                this.secondaryBufferedMutatorParamsCaptor,
                secondaryBufferedMutator,
                mutation1,
                mutation3))
        .when(secondaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    MirroringBufferedMutator bm = getBufferedMutator((long) (mutationSize * 3.5));

    bm.mutate(Arrays.asList(mutation1, mutation2, mutation3, mutation4));
    executorServiceRule.waitForExecutor();
    verify(secondaryBufferedMutator, times(1))
        .mutate(Arrays.asList(mutation1, mutation2, mutation3, mutation4));

    verify(secondaryWriteErrorConsumerWithMetrics, atLeastOnce())
        .consume(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, Arrays.asList(mutation1));
    verify(secondaryWriteErrorConsumerWithMetrics, atLeastOnce())
        .consume(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, Arrays.asList(mutation3));
  }

  @Test
  public void testSecondaryErrorsDuringSimultaneousFlushes()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Mutation mutation1 = new Delete("key1".getBytes());
    final Mutation mutation2 = new Delete("key2".getBytes());
    final Mutation mutation3 = new Delete("key3".getBytes());
    final Mutation mutation4 = new Delete("key4".getBytes());

    long mutationSize = mutation1.heapSize();

    final AtomicInteger ongoingFlushes = new AtomicInteger(0);
    final SettableFuture<Void> allFlushesStarted = SettableFuture.create();
    final SettableFuture<Void> endFlush = SettableFuture.create();

    doAnswer(blockedFlushes(ongoingFlushes, allFlushesStarted, endFlush, 2))
        .when(primaryBufferedMutator)
        .flush();

    doAnswer(
            mutateWithErrors(
                this.secondaryBufferedMutatorParamsCaptor,
                secondaryBufferedMutator,
                mutation1,
                mutation3))
        .when(secondaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    MirroringBufferedMutator bm = getBufferedMutator((long) (mutationSize * 1.5));

    bm.mutate(Arrays.asList(mutation1, mutation2));
    bm.mutate(Arrays.asList(mutation3, mutation4));
    allFlushesStarted.get(3, TimeUnit.SECONDS);

    endFlush.set(null);

    executorServiceRule.waitForExecutor();
    verify(secondaryBufferedMutator, atLeastOnce()).mutate(Arrays.asList(mutation1, mutation2));
    verify(secondaryBufferedMutator, atLeastOnce()).mutate(Arrays.asList(mutation3, mutation4));

    verify(secondaryWriteErrorConsumerWithMetrics, atLeastOnce())
        .consume(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, Arrays.asList(mutation1));
    verify(secondaryWriteErrorConsumerWithMetrics, atLeastOnce())
        .consume(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, Arrays.asList(mutation3));
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
                int value = runningFlushes.decrementAndGet();
                if (value == 0) {
                  flushesStarted.set(null);
                }
                performFlush.get();

                long id = Longs.fromByteArray(mutations[value].getRow());
                RetriesExhaustedWithDetailsException e =
                    new RetriesExhaustedWithDetailsException(
                        Arrays.asList((Throwable) new IOException(String.valueOf(id))),
                        Arrays.asList((Row) mutations[value]),
                        Arrays.asList("localhost:" + value));
                primaryBufferedMutatorParamsCaptor
                    .getValue()
                    .getListener()
                    .onException(e, primaryBufferedMutator);
                return null;
              }
            })
        .when(primaryBufferedMutator)
        .flush();

    final BufferedMutator bm = getBufferedMutator(1);

    bm.mutate(mutations[2]);
    // Wait until flush is started to ensure to ensure that flushes are scheduled in the same order
    // as mutations.
    while (runningFlushes.get() == 3) {
      Thread.sleep(100);
    }
    bm.mutate(mutations[1]);
    while (runningFlushes.get() == 2) {
      Thread.sleep(100);
    }
    bm.mutate(mutations[0]);
    while (runningFlushes.get() == 1) {
      Thread.sleep(100);
    }
    flushesStarted.get(1, TimeUnit.SECONDS);
    performFlush.set(null);

    executorServiceRule.waitForExecutor();

    verify(secondaryBufferedMutator, never()).flush();
    verify(resourceReservation, times(3)).release();

    // We have killed the executor, mock next submits.
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return SettableFuture.create();
              }
            })
        .when(executorServiceRule.executorService)
        .submit(any(Callable.class));

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

    verify(secondaryBufferedMutator, never()).flush();
    verify(resourceReservation, times(3)).release();
  }

  private Answer<Void> blockedFlushes(
      final AtomicInteger ongoingFlushes,
      final SettableFuture<Void> allFlushesStarted,
      final SettableFuture<Void> endFlush,
      final int expectedNumberOfFlushes) {
    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (ongoingFlushes.incrementAndGet() == expectedNumberOfFlushes) {
          allFlushesStarted.set(null);
        }
        endFlush.get();
        return null;
      }
    };
  }

  private Answer<Void> mutateWithErrors(
      final ArgumentCaptor<BufferedMutatorParams> argumentCaptor,
      final BufferedMutator bufferedMutator,
      final Mutation... failingMutations) {
    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<Mutation> failingMutationsList = Arrays.asList(failingMutations);
        List<? extends Mutation> argument = invocationOnMock.getArgument(0);
        for (Mutation m : argument) {
          if (failingMutationsList.contains(m)) {
            argumentCaptor
                .getValue()
                .getListener()
                .onException(
                    new RetriesExhaustedWithDetailsException(
                        Arrays.asList(new Throwable[] {new IOException()}),
                        Arrays.asList(new Row[] {m}),
                        Arrays.asList("invalid.example:1234")),
                    bufferedMutator);
          }
        }
        return null;
      }
    };
  }

  private MirroringBufferedMutator getBufferedMutator(long flushThreshold) throws IOException {
    return new MirroringBufferedMutator(
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        makeConfigurationWithFlushThreshold(flushThreshold),
        flowController,
        executorServiceRule.executorService,
        secondaryWriteErrorConsumerWithMetrics,
        new MirroringTracer());
  }

  private MirroringConfiguration makeConfigurationWithFlushThreshold(long flushThreshold) {
    Configuration mirroringConfig = new Configuration();
    mirroringConfig.set(MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH, String.valueOf(flushThreshold));

    return new MirroringConfiguration(new Configuration(), new Configuration(), mirroringConfig);
  }
}
