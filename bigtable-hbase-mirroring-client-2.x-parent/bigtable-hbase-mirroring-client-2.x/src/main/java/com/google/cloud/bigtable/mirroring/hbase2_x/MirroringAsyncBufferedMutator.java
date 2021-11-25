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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

@InternalApi
public class MirroringAsyncBufferedMutator implements AsyncBufferedMutator {

  private final AsyncBufferedMutator primary;
  private final AsyncBufferedMutator secondary;
  private final FlowController flowController;
  private final ListenableReferenceCounter referenceCounter;
  private final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public MirroringAsyncBufferedMutator(
      AsyncBufferedMutator primary,
      AsyncBufferedMutator secondary,
      FlowController flowController,
      SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer) {
    this.primary = primary;
    this.secondary = secondary;
    this.flowController = flowController;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.referenceCounter = new ListenableReferenceCounter();
  }

  @Override
  public TableName getName() {
    return primary.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return primary.getConfiguration();
  }

  @Override
  public CompletableFuture<Void> mutate(Mutation mutation) {
    referenceCounter.incrementReferenceCount();
    CompletableFuture<Void> primaryCompleted = primary.mutate(mutation);
    CompletableFuture<Void> resultFuture = new CompletableFuture<>();

    primaryCompleted
        .thenRun(
            () -> {
              CompletableFuture<FlowController.ResourceReservation> resourceRequested =
                  FutureConverter.toCompletable(
                      flowController.asyncRequestResource(
                          new RequestResourcesDescription(mutation)));

              resourceRequested
                  .thenRun(
                      () -> {
                        resultFuture.complete(null);
                        secondary
                            .mutate(mutation)
                            .thenRun(referenceCounter::decrementReferenceCount)
                            .exceptionally(
                                secondaryError -> {
                                  this.secondaryWriteErrorConsumer.consume(
                                      MirroringSpanConstants.HBaseOperation.BUFFERED_MUTATOR_MUTATE,
                                      mutation,
                                      secondaryError);
                                  referenceCounter.decrementReferenceCount();
                                  return null;
                                });
                      })
                  .exceptionally(
                      resourceReservationError -> {
                        referenceCounter.decrementReferenceCount();
                        resultFuture.complete(null);
                        this.secondaryWriteErrorConsumer.consume(
                            MirroringSpanConstants.HBaseOperation.BUFFERED_MUTATOR_MUTATE,
                            mutation,
                            resourceReservationError);
                        return null;
                      });
            })
        .exceptionally(
            primaryError -> {
              referenceCounter.decrementReferenceCount();
              resultFuture.completeExceptionally(primaryError);
              return null;
            });

    return resultFuture;
  }

  @Override
  public List<CompletableFuture<Void>> mutate(List<? extends Mutation> list) {
    ArrayList<CompletableFuture<Void>> results = new ArrayList<>(list.size());
    for (Mutation mutation : list) {
      results.add(mutate(mutation));
    }
    return results;
  }

  @Override
  public void flush() {
    primary.flush();
    secondary.flush();
  }

  @Override
  public synchronized void close() {
    if (this.closed.get()) {
      return;
    }
    this.closed.set(true);
    closeMirroringBufferedMutatorAndWaitForAsyncOperations();

    this.primary.close();
    this.secondary.close();
  }

  @Override
  public long getWriteBufferSize() {
    return primary.getWriteBufferSize();
  }

  @Override
  public long getPeriodicalFlushTimeout(TimeUnit unit) {
    return primary.getPeriodicalFlushTimeout(unit);
  }

  private void closeMirroringBufferedMutatorAndWaitForAsyncOperations() {
    this.referenceCounter.decrementReferenceCount();
    try {
      this.referenceCounter.getOnLastReferenceClosed().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
