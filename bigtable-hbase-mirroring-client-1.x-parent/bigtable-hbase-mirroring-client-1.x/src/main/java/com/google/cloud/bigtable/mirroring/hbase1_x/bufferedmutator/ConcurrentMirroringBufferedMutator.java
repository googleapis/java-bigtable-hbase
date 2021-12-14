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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException.ExceptionDetails;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * {@link MirroringBufferedMutator} implementation that performs writes to primary and secondary
 * database concurrently.
 *
 * <p>Similarly to {@link SequentialMirroringBufferedMutator}, after at least {@link
 * com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper#MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH}
 * bytes of mutations issued, a asynchronous flush is performed. This implementation performs
 * flushes on both primary and secondary databases. Errors reported by underlying buffered mutators
 * are reported to the user after those flushes complete and are annotated with {@link
 * MirroringOperationException} to denote which database have failed.
 */
public class ConcurrentMirroringBufferedMutator
    extends MirroringBufferedMutator<List<? extends Mutation>> {
  // These maps use WeakReferences as keys, however we store mutations that might end up in this map
  // in a buffer before they are scheduled on secondary buffered mutator, and we remove them after
  // the secondary was flushed. Elements are inserted into this map between these steps.
  // These maps are safe to be used concurrently.
  private final Map<Row, ExceptionDetails> failedPrimaryOperations =
      (new MapMaker()).weakKeys().makeMap();
  private final Map<Row, ExceptionDetails> failedSecondaryOperations =
      (new MapMaker()).weakKeys().makeMap();

  private final LinkedList<Throwable> flushExceptions = new LinkedList<>();

  private final RetriesExhaustedExceptionBuilder retriesExhaustedExceptionBuilder =
      new RetriesExhaustedExceptionBuilder();

  public ConcurrentMirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      ExecutorService executorService,
      MirroringTracer mirroringTracer)
      throws IOException {
    super(
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        configuration,
        executorService,
        mirroringTracer);
  }

  @Override
  protected void mutateScoped(final List<? extends Mutation> mutations) throws IOException {
    final MirroringExceptionBuilder<IOException> mirroringExceptionBuilder =
        new MirroringExceptionBuilder<>();

    RequestResourcesDescription resourcesDescription = new RequestResourcesDescription(mutations);
    primaryMutate(mutations, mirroringExceptionBuilder);
    secondaryMutate(mutations, mirroringExceptionBuilder);
    storeResourcesAndFlushIfNeeded(mutations, resourcesDescription);
    mirroringExceptionBuilder.throwCombinedExceptionIfPresent();
    throwExceptionIfAvailable();
  }

  private void primaryMutate(
      final List<? extends Mutation> mutations,
      MirroringExceptionBuilder<IOException> mirroringExceptionBuilder) {
    try {
      this.mirroringTracer.spanFactory.wrapPrimaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              primaryBufferedMutator.mutate(mutations);
              return null;
            }
          },
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);
    } catch (RetriesExhaustedWithDetailsException e) {
      // Ignore this error, it was already handled by error handler and we will rethrow it after
      // flush.
    } catch (IOException e) {
      mirroringExceptionBuilder.setPrimaryException(e);
    }
  }

  private void secondaryMutate(
      final List<? extends Mutation> mutations,
      MirroringExceptionBuilder<IOException> mirroringExceptionBuilder) {
    try {
      this.mirroringTracer.spanFactory.wrapSecondaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              secondaryBufferedMutator.mutate(mutations);
              return null;
            }
          },
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);
    } catch (RetriesExhaustedWithDetailsException e) {
      // Ignore this error, it was already handled by error handler and we will rethrow it after
      // flush.
    } catch (IOException e) {
      mirroringExceptionBuilder.setSecondaryException(e);
    }
  }

  @Override
  protected void handlePrimaryException(RetriesExhaustedWithDetailsException e) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      failedPrimaryOperations.put(
          e.getRow(i), new ExceptionDetails(e.getCause(i), e.getHostnamePort(i)));
    }
  }

  @Override
  protected void handleSecondaryException(RetriesExhaustedWithDetailsException e) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      failedSecondaryOperations.put(
          e.getRow(i), new ExceptionDetails(e.getCause(i), e.getHostnamePort(i)));
    }
  }

  @Override
  protected void flushBufferedMutatorBeforeClosing()
      throws ExecutionException, InterruptedException {
    scheduleFlushAll().bothFlushesFinished.get();
  }

  @Override
  protected FlushFutures scheduleFlushScoped(
      final List<List<? extends Mutation>> dataToFlush, FlushFutures previousFlushFutures) {
    final SettableFuture<Void> bothFlushesFinished = SettableFuture.create();

    ListenableFuture<Void> primaryFlushFinished =
        schedulePrimaryFlush(previousFlushFutures.primaryFlushFinished);
    ListenableFuture<Void> secondaryFlushFinished =
        scheduleSecondaryFlush(previousFlushFutures.secondaryFlushFinished);

    // This object will aggregate `IOExceptions` and `RuntimeExceptions` thrown by flush()
    // operations which were not handled by registered handlers (that is
    // `RetriesExhaustedWithDetailsException`s). Those exceptions will be tagged with
    // MirroringOperationException and rethrown to the user.
    final MirroringExceptionBuilder<Throwable> mirroringExceptionBuilder =
        new MirroringExceptionBuilder<>();

    final AtomicBoolean firstFinished = new AtomicBoolean(false);
    final Runnable flushFinished =
        new Runnable() {
          @Override
          public void run() {
            if (firstFinished.getAndSet(true)) {
              bothFlushesFinishedCallback(dataToFlush);
              Throwable flushException = mirroringExceptionBuilder.buildCombinedException();
              if (flushException != null) {
                flushExceptions.add(flushException);
              }
              bothFlushesFinished.set(null);
            }
          }
        };

    Futures.addCallback(
        primaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                flushFinished.run();
              }

              @Override
              public void onFailure(Throwable throwable) {
                // RetriesExhaustedWithDetailsException is ignored, it was reported to the handler
                // and stored in failedPrimaryOperations buffer.
                if (!(throwable instanceof RetriesExhaustedWithDetailsException)) {
                  mirroringExceptionBuilder.setPrimaryException(throwable);
                }
                flushFinished.run();
              }
            }),
        MoreExecutors.directExecutor());

    Futures.addCallback(
        secondaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                flushFinished.run();
              }

              @Override
              public void onFailure(Throwable throwable) {
                // RetriesExhaustedWithDetailsException is ignored, it was reported to the handler
                // and stored in failedSecondaryOperations buffer.
                if (!(throwable instanceof RetriesExhaustedWithDetailsException)) {
                  mirroringExceptionBuilder.setSecondaryException(throwable);
                }
                flushFinished.run();
              }
            }),
        MoreExecutors.directExecutor());

    return new FlushFutures(
        primaryFlushFinished,
        secondaryFlushFinished,
        bothFlushesFinished,
        // Flush operation can be unblocked when both flushes have finished.
        bothFlushesFinished);
  }

  private ListenableFuture<Void> scheduleSecondaryFlush(
      final ListenableFuture<?> previousFlushCompletedFuture) {
    return this.executorService.submit(
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                mirroringTracer.spanFactory.wrapSecondaryOperation(
                    createFlushTask(secondaryBufferedMutator, previousFlushCompletedFuture),
                    HBaseOperation.BUFFERED_MUTATOR_FLUSH);
                return null;
              }
            }));
  }

  private void bothFlushesFinishedCallback(List<List<? extends Mutation>> dataToFlush) {
    Iterable<Mutation> mutations = Iterables.concat(dataToFlush);
    for (Mutation mutation : mutations) {
      ExceptionDetails primaryCause = failedPrimaryOperations.remove(mutation);
      ExceptionDetails secondaryCause = failedSecondaryOperations.remove(mutation);
      boolean primaryFailed = primaryCause != null;
      boolean secondaryFailed = secondaryCause != null;

      if (primaryFailed || secondaryFailed) {
        Throwable exception;
        String hostnamePort;
        if (primaryFailed && secondaryFailed) {
          exception =
              MirroringOperationException.markedAsBothException(
                  primaryCause.exception, secondaryCause, mutation);
          hostnamePort = primaryCause.hostnameAndPort;
        } else if (primaryFailed) {
          exception =
              MirroringOperationException.markedAsPrimaryException(
                  primaryCause.exception, mutation);
          hostnamePort = primaryCause.hostnameAndPort;
        } else {
          exception =
              MirroringOperationException.markedAsSecondaryException(
                  secondaryCause.exception, mutation);
          hostnamePort = secondaryCause.hostnameAndPort;
        }
        retriesExhaustedExceptionBuilder.addException(mutation, exception, hostnamePort);
      }
    }
  }

  /**
   * Throws exceptions that were reported by last flush operation or RetiresExhaustedException for
   * rows that were reported as failed.
   */
  @Override
  protected void throwExceptionIfAvailable() throws IOException {
    throwPrimaryFlushExceptionIfAvailable();
    RetriesExhaustedWithDetailsException e = retriesExhaustedExceptionBuilder.clearAndBuild();
    if (e != null) {
      this.userListener.onException(e, this);
    }
  }

  private void throwPrimaryFlushExceptionIfAvailable() throws IOException {
    if (this.flushExceptions.isEmpty()) {
      return;
    }
    Throwable error = this.flushExceptions.pollFirst();
    if (error instanceof IOException) {
      throw (IOException) error;
    } else if (error instanceof RuntimeException) {
      throw (RuntimeException) error;
    } else {
      throw new RuntimeException(error);
    }
  }

  private static class RetriesExhaustedExceptionBuilder {
    private List<Row> mutations = new ArrayList<>();
    private List<Throwable> exceptions = new ArrayList<>();
    private List<String> hostnamePorts = new ArrayList<>();

    public synchronized void addException(
        Mutation mutation, Throwable exception, String hostnamePort) {
      this.mutations.add(mutation);
      this.hostnamePorts.add(hostnamePort);
      this.exceptions.add(exception);
    }

    public synchronized RetriesExhaustedWithDetailsException clearAndBuild() {
      if (this.mutations.isEmpty()) {
        return null;
      }

      List<Row> mutations = this.mutations;
      List<Throwable> exceptions = this.exceptions;
      List<String> hostnamePorts = this.hostnamePorts;
      this.mutations = new ArrayList<>();
      this.exceptions = new ArrayList<>();
      this.hostnamePorts = new ArrayList<>();
      return new RetriesExhaustedWithDetailsException(exceptions, mutations, hostnamePorts);
    }
  }

  private static class MirroringExceptionBuilder<E extends Throwable> {
    private E primaryException;
    private E secondaryException;

    public void setPrimaryException(E primaryException) {
      this.primaryException = primaryException;
    }

    public void setSecondaryException(E secondaryException) {
      this.secondaryException = secondaryException;
    }

    public E buildCombinedException() {
      if (this.primaryException != null && this.secondaryException != null) {
        return MirroringOperationException.markedAsBothException(
            this.primaryException, new ExceptionDetails(this.secondaryException), null);
      } else if (this.primaryException != null) {
        return MirroringOperationException.markedAsPrimaryException(this.primaryException, null);
      } else if (this.secondaryException != null) {
        return MirroringOperationException.markedAsSecondaryException(
            this.secondaryException, null);
      } else {
        return null;
      }
    }

    public void throwCombinedExceptionIfPresent() throws E {
      E exception = this.buildCombinedException();
      if (exception != null) {
        throw exception;
      }
    }
  }
}
