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
package com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator;

import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.blockedFlushes;
import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.flushWithErrors;
import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.makeConfigurationWithFlushThreshold;
import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.mutateWithErrors;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException.DatabaseIdentifier;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestConcurrentMirroringBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();

  public final MirroringBufferedMutatorCommon common = new MirroringBufferedMutatorCommon();

  @Test
  public void testBufferedWritesWithoutErrors() throws IOException, InterruptedException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(2)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(2)).mutate(ArgumentMatchers.<Mutation>anyList());
    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    bm.mutate(common.mutation1);
    Thread.sleep(300);
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.flowController, never())
        .asyncRequestResource(any(RequestResourcesDescription.class));
    verify(common.resourceReservation, never()).release();
  }

  @Test
  public void testBufferedMutatorFlush() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.flush();
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.flowController, never())
        .asyncRequestResource(any(RequestResourcesDescription.class));
    verify(common.resourceReservation, never()).release();
  }

  @Test
  public void testCloseFlushesWrites() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.close();
    verify(common.primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.flowController, never())
        .asyncRequestResource(any(RequestResourcesDescription.class));
    verify(common.resourceReservation, never()).release();
  }

  @Test
  public void testCloseIsIdempotent() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.close();
    bm.close();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.flowController, never())
        .asyncRequestResource(any(RequestResourcesDescription.class));
    verify(common.resourceReservation, never()).release();
  }

  @Test
  public void testFlushesCanBeScheduledSimultaneously()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    final AtomicInteger ongoingFlushes = new AtomicInteger(0);
    final SettableFuture<Void> allFlushesStarted = SettableFuture.create();
    final SettableFuture<Void> endFlush = SettableFuture.create();

    doAnswer(blockedFlushes(ongoingFlushes, allFlushesStarted, endFlush, 4))
        .when(common.primaryBufferedMutator)
        .flush();

    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 1.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    allFlushesStarted.get(3, TimeUnit.SECONDS);
    assertThat(ongoingFlushes.get()).isEqualTo(4);
    endFlush.set(null);
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(8)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(8)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(4)).flush();
    verify(common.flowController, never())
        .asyncRequestResource(any(RequestResourcesDescription.class));
    verify(common.resourceReservation, never()).release();
  }

  @Test
  public void testErrorsReportedByPrimaryDoNotPreventSecondaryWrites() throws IOException {
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
    try {
      bm.mutate(common.mutation2);
    } catch (IOException ignored) {
    }
    try {
      bm.mutate(common.mutation3);
    } catch (IOException ignored) {
    }
    try {
      bm.mutate(common.mutation4);
    } catch (IOException ignored) {
    }
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation1));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation1));
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation2));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation2));
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation3));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation3));
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation4));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation4));
  }

  @Test
  public void testErrorsReportedBySecondaryAreReportedAsWriteErrors() throws IOException {
    doAnswer(
            mutateWithErrors(
                this.common.secondaryBufferedMutatorParamsCaptor,
                common.secondaryBufferedMutator,
                common.mutation1,
                common.mutation3))
        .when(common.secondaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(
        Arrays.asList(common.mutation1, common.mutation2, common.mutation3, common.mutation4));
    executorServiceRule.waitForExecutor();
    verify(common.secondaryBufferedMutator, times(1))
        .mutate(
            Arrays.asList(common.mutation1, common.mutation2, common.mutation3, common.mutation4));

    verify(common.secondaryWriteErrorConsumerWithMetrics, never())
        .consume(any(HBaseOperation.class), any(Row.class), any(Throwable.class));
    verify(common.secondaryWriteErrorConsumerWithMetrics, never())
        .consume(any(HBaseOperation.class), ArgumentMatchers.<Row>anyList(), any(Throwable.class));
  }

  @Test
  public void testErrorsInBothPrimaryAndSecondary() throws IOException {
    //    | primary | secondary |
    // m1 | x       | v         |
    // m2 | v       | v         |
    // m3 | x       | x         |
    // m4 | v       | x         |

    BufferedMutator bm = getBufferedMutator(common.mutationSize * 10);

    doAnswer(
            flushWithErrors(
                this.common.primaryBufferedMutatorParamsCaptor,
                common.primaryBufferedMutator,
                common.mutation1,
                common.mutation3))
        .when(common.primaryBufferedMutator)
        .flush();
    doAnswer(
            flushWithErrors(
                this.common.secondaryBufferedMutatorParamsCaptor,
                common.secondaryBufferedMutator,
                common.mutation3,
                common.mutation4))
        .when(common.secondaryBufferedMutator)
        .flush();

    List<Mutation> mutations =
        Arrays.asList(common.mutation1, common.mutation2, common.mutation3, common.mutation4);
    bm.mutate(mutations);

    // flush not called
    verify(common.primaryBufferedMutator, never()).flush();
    verify(common.secondaryBufferedMutator, never()).flush();

    try {
      bm.flush();
    } catch (RetriesExhaustedWithDetailsException e) {
      assertThat(e.getNumExceptions()).isEqualTo(3);
      assertThat(e.getRow(0)).isEqualTo(common.mutation1);
      assertThat(MirroringOperationException.extractRootCause(e.getCause(0))).isNotNull();
      assertThat(MirroringOperationException.extractRootCause(e.getCause(0)).databaseIdentifier)
          .isEqualTo(DatabaseIdentifier.Primary);

      assertThat(e.getRow(1)).isEqualTo(common.mutation3);
      assertThat(MirroringOperationException.extractRootCause(e.getCause(1))).isNotNull();
      assertThat(MirroringOperationException.extractRootCause(e.getCause(1)).databaseIdentifier)
          .isEqualTo(DatabaseIdentifier.Both);
      assertThat(MirroringOperationException.extractRootCause(e.getCause(1)).secondaryException)
          .isNotNull();

      assertThat(e.getRow(2)).isEqualTo(common.mutation4);
      assertThat(MirroringOperationException.extractRootCause(e.getCause(2))).isNotNull();
      assertThat(MirroringOperationException.extractRootCause(e.getCause(2)).databaseIdentifier)
          .isEqualTo(DatabaseIdentifier.Secondary);
    }

    executorServiceRule.waitForExecutor();
    verify(common.secondaryBufferedMutator, times(1)).mutate(mutations);
    verify(common.secondaryBufferedMutator, times(1)).mutate(mutations);

    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();

    verify(common.secondaryWriteErrorConsumerWithMetrics, never())
        .consume(any(HBaseOperation.class), any(Row.class), any(Throwable.class));

    verify(common.secondaryWriteErrorConsumerWithMetrics, never())
        .consume(any(HBaseOperation.class), ArgumentMatchers.<Row>anyList(), any(Throwable.class));
  }

  private BufferedMutator getBufferedMutator(long flushThreshold) throws IOException {
    return new ConcurrentMirroringBufferedMutator(
        common.primaryConnection,
        common.secondaryConnection,
        common.bufferedMutatorParams,
        makeConfigurationWithFlushThreshold(flushThreshold),
        executorServiceRule.executorService,
        new MirroringTracer());
  }
}
