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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.SequentialMirroringBufferedMutator.Entry;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * {@link MirroringBufferedMutator} implementation that performs mutations on secondary database
 * only if we are certain that they were successfully applied on primary database.
 *
 * <p>The HBase 1.x API doesn't give its user any indication when asynchronous writes were
 * performed, only performing a synchronous {@link BufferedMutator#flush()} ensures that all
 * previously buffered mutations are done. To achieve our goal we store a copy of all mutations sent
 * to primary BufferedMutator in a internal buffer. When size of the buffer reaches a threshold of
 * {@link
 * com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper#MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH}
 * bytes, we perform a flush in a worker thread. After flush we pass collected mutations to
 * secondary BufferedMutator and flush it. Writes that have failed on primary are not forwarded to
 * secondary, writes that have failed on secondary are forwarded to {@link
 * SecondaryWriteErrorConsumer#consume(HBaseOperation, Row, Throwable)} handler.
 *
 * <p>Moreover, we perform our custom flow control to prevent unbounded growth of memory - calls to
 * mutate() might block if secondary database lags behind. We account size of all operations that
 * were placed in primary BufferedMutator but weren't yet executed and confirmed on secondary
 * BufferedMutator (or until we are informed that they have failed on primary).
 */
@InternalApi("For internal usage only")
public class SequentialMirroringBufferedMutator extends MirroringBufferedMutator<Entry> {
  /**
   * Set of {@link Row}s that were passed to primary BufferedMutator but failed. We create a entry
   * in this collection every time our error handler is called by primary BufferedMutator. Those
   * entries are consulted before we perform mutations on secondary BufferedMutator, if a {@link
   * Row} instance scheduled for insertion is in this collection, then it is omitted and
   * corresponding entry is removed from the set.
   *
   * <p>Those maps use {@code WeakReferences<Row>} as keys and compares their content using {@code
   * ==} instead of {@code equals}. This is faster than comparing Rows using {@code equals} and is
   * safe, because we always check is a specific Row object have failed.
   */
  private final Set<Row> failedPrimaryOperations =
      Collections.newSetFromMap((new MapMaker()).weakKeys().<Row, Boolean>makeMap());

  /**
   * Exceptions caught when performing asynchronous flush() on primary BufferedMutator that should
   * be rethrown to inform the user about failed writes.
   */
  private List<RetriesExhaustedWithDetailsException> exceptionsToBeThrown = new ArrayList<>();

  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;

  private final FlowController flowController;

  public SequentialMirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer)
      throws IOException {
    super(
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        configuration,
        executorService,
        mirroringTracer);
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.flowController = flowController;
  }

  @Override
  protected void mutateScoped(final List<? extends Mutation> list) throws IOException {
    IOException primaryException = null;
    try {
      this.mirroringTracer.spanFactory.wrapPrimaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              primaryBufferedMutator.mutate(list);
              return null;
            }
          },
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);
    } catch (IOException e) {
      primaryException = e;
    } finally {
      // This call might block - we have confirmed that mutate() calls on BufferedMutator from
      // HBase client library might also block.
      addSecondaryMutation(list, primaryException);
    }
    // Throw exceptions that were thrown by ExceptionListener on primary BufferedMutator which we
    // have caught when calling flush.
    throwExceptionIfAvailable();
  }

  /**
   * This method is called from within {@code finally} block. Currently processed exception is
   * passed in primaryException, if any. This method shouldn't throw any exception if
   * primaryException != null.
   */
  private void addSecondaryMutation(
      List<? extends Mutation> mutations, IOException primaryException) throws IOException {
    RequestResourcesDescription resourcesDescription = new RequestResourcesDescription(mutations);
    ListenableFuture<ResourceReservation> reservationFuture =
        flowController.asyncRequestResource(resourcesDescription);

    ResourceReservation reservation;
    try {
      try (Scope scope = this.mirroringTracer.spanFactory.flowControlScope()) {
        reservation = reservationFuture.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      // We won't write those mutations to secondary database, they should be reported to
      // secondaryWriteErrorConsumer.
      reportWriteErrors(mutations, e);

      setInterruptedFlagInInterruptedException(e);
      if (primaryException != null) {
        // We are currently in a finally block handling an exception, we shouldn't throw anything.
        primaryException.addSuppressed(e);
        return;
      } else {
        throw new IOException(e);
      }
    }

    storeResourcesAndFlushIfNeeded(new Entry(mutations, reservation), resourcesDescription);
  }

  @Override
  protected void scopedFlush() throws IOException {
    try {
      scheduleFlush().primaryFlushFinished.get();
    } catch (InterruptedException | ExecutionException e) {
      setInterruptedFlagInInterruptedException(e);
      throw new IOException(e);
    }
    throwExceptionIfAvailable();
  }

  @Override
  protected void handlePrimaryException(RetriesExhaustedWithDetailsException e)
      throws RetriesExhaustedWithDetailsException {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      this.failedPrimaryOperations.add(e.getRow(i));
    }
    this.userListener.onException(e, this);
  }

  @Override
  protected void handleSecondaryException(RetriesExhaustedWithDetailsException e) {
    reportWriteErrors(e);
  }

  @Override
  protected synchronized FlushFutures scheduleFlushScoped(final List<Entry> dataToFlush) {
    final SettableFuture<Void> secondaryFlushFinished = SettableFuture.create();

    ListenableFuture<Void> primaryFlushFinished = schedulePrimaryFlush();

    Futures.addCallback(
        primaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                performSecondaryFlush(dataToFlush, secondaryFlushFinished);
              }

              @Override
              public void onFailure(Throwable throwable) {
                if (throwable instanceof RetriesExhaustedWithDetailsException) {
                  // If user-defined listener has thrown an exception
                  // (RetriesExhaustedWithDetailsException is the only exception that can be
                  // thrown), we know that some of the writes failed. Our handler has already
                  // handled those errors. We should also rethrow this exception when user
                  // calls mutate/flush the next time.
                  saveExceptionToBeThrown((RetriesExhaustedWithDetailsException) throwable);

                  performSecondaryFlush(dataToFlush, secondaryFlushFinished);
                } else {
                  // In other cases, we do not know what caused the error and we have no idea
                  // what was really written to the primary DB, the best we can do is write
                  // them to on-disk log. Trying to save them to secondary database is not a
                  // good idea - if current thread was interrupted then next flush might also
                  // be, only increasing our confusion, moreover, that may cause secondary
                  // writes that were not completed on primary.
                  reportWriteErrors(Entry.mergeMutations(dataToFlush), throwable);
                  releaseReservations(dataToFlush);
                  secondaryFlushFinished.setException(throwable);
                }
              }
            }),
        MoreExecutors.directExecutor());
    return new FlushFutures(primaryFlushFinished, secondaryFlushFinished, secondaryFlushFinished);
  }

  private void performSecondaryFlush(
      List<Entry> dataToFlush, SettableFuture<Void> completionFuture) {
    List<Mutation> mutations = Entry.mergeMutations(dataToFlush);
    final List<? extends Mutation> successfulOperations = removeFailedMutations(mutations);
    try {
      if (!successfulOperations.isEmpty()) {
        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                secondaryBufferedMutator.mutate(successfulOperations);
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);

        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                secondaryBufferedMutator.flush();
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_FLUSH);
      }
      releaseReservations(dataToFlush);
      completionFuture.set(null);
    } catch (Throwable e) {
      // Our listener is registered and should catch non-fatal errors. This is either
      // InterruptedIOException or some RuntimeError, in both cases we should consider operation as
      // not completed - the worst that can happen is that we will have some writes in both
      // secondary database and on-disk log.
      reportWriteErrors(mutations, e);
      releaseReservations(dataToFlush);
      completionFuture.setException(e);
    }
  }

  private void releaseReservations(List<Entry> entries) {
    for (Entry entry : entries) {
      entry.reservation.release();
    }
  }

  private List<? extends Mutation> removeFailedMutations(List<? extends Mutation> dataToFlush) {
    List<Mutation> successfulMutations = new ArrayList<>();
    for (Mutation mutation : dataToFlush) {
      if (!this.failedPrimaryOperations.remove(mutation)) {
        successfulMutations.add(mutation);
      }
    }
    return successfulMutations;
  }

  protected final synchronized void saveExceptionToBeThrown(
      RetriesExhaustedWithDetailsException exception) {
    this.exceptionsToBeThrown.add(exception);
  }

  protected final RetriesExhaustedWithDetailsException getExceptionsToBeThrown() {
    List<RetriesExhaustedWithDetailsException> exceptions;
    synchronized (this) {
      if (this.exceptionsToBeThrown.isEmpty()) {
        return null;
      }
      exceptions = this.exceptionsToBeThrown;
      this.exceptionsToBeThrown = new ArrayList<>();
    }

    List<Row> rows = new ArrayList<>();
    List<Throwable> causes = new ArrayList<>();
    List<String> hostnames = new ArrayList<>();

    for (RetriesExhaustedWithDetailsException e : exceptions) {
      for (int i = 0; i < e.getNumExceptions(); i++) {
        rows.add(e.getRow(i));
        causes.add(e.getCause(i));
        hostnames.add(e.getHostnamePort(i));
      }
    }
    return new RetriesExhaustedWithDetailsException(causes, rows, hostnames);
  }

  private void throwExceptionIfAvailable() throws RetriesExhaustedWithDetailsException {
    RetriesExhaustedWithDetailsException e = getExceptionsToBeThrown();
    if (e != null) {
      throw e;
    }
  }

  private void reportWriteErrors(RetriesExhaustedWithDetailsException e) {
    try (Scope scope = this.mirroringTracer.spanFactory.writeErrorScope()) {
      for (int i = 0; i < e.getNumExceptions(); i++) {
        this.secondaryWriteErrorConsumer.consume(
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, e.getRow(i), e.getCause(i));
      }
    }
  }

  private void reportWriteErrors(List<? extends Mutation> mutations, Throwable cause) {
    try (Scope scope = this.mirroringTracer.spanFactory.writeErrorScope()) {
      this.secondaryWriteErrorConsumer.consume(
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, mutations, cause);
    }
  }

  public static class Entry {
    public final List<? extends Mutation> mutations;
    public final ResourceReservation reservation;

    public Entry(List<? extends Mutation> mutations, ResourceReservation reservation) {
      this.mutations = mutations;
      this.reservation = reservation;
    }

    public static List<Mutation> mergeMutations(List<Entry> entries) {
      List<Mutation> mutations = new ArrayList<>();
      for (Entry e : entries) {
        mutations.addAll(e.mutations);
      }
      return mutations;
    }
  }
}
