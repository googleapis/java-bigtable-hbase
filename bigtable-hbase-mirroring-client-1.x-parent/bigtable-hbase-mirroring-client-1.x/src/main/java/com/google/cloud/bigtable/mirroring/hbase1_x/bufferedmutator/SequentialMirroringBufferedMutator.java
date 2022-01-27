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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.Timestamper;
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
import java.util.concurrent.ConcurrentLinkedQueue;
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
 *
 * <p>Notes about error handling in SequentialMirroringBufferedMutator:
 *
 * <p>The HBase 1.x BufferedMutator's API notifies the user about failed mutations by calling user
 * supplied exception handler with appropriate {@link RetriesExhaustedWithDetailsException}
 * exception as a parameter. The error handlers are not called asynchronously from BufferedMutator's
 * internal thread - instead BufferedMutator implementations gather encountered failed mutations and
 * reasons of their failures in an internal data structure. The exception handler is called when the
 * user code interacts with the BufferedMutator again - when {@link
 * BufferedMutator#mutate(Mutation)} or {@link BufferedMutator#flush()} is called (HBase 1.x
 * BufferedMutator's implementation calls the exception handler performing the operation requested
 * by the user).
 *
 * <p>Because the exception handlers are called synchronously in user thread they can throw
 * exceptions that will be propagated to the user (what can be seen in {@link
 * ExceptionListener#onException(RetriesExhaustedWithDetailsException, BufferedMutator)}'s
 * signature). The default exception handler is implemented in such a way, it just throws supplied
 * exception.
 *
 * <p>The goal of SequentialMirroringBufferedMutator is to mirror only those mutations that were
 * successful. For this reason we inject our own error handler to primary BufferedMutator ({@link
 * #handlePrimaryException(RetriesExhaustedWithDetailsException)}) that gathers failed mutations
 * that shouldn't be mirrored into the secondary (into {@link #failedPrimaryOperations} collection)
 * and forwards the exception supplied in the parameter to user's exception handler (as the user
 * expects it). If the user-supplied error handler throws an exception, then {@link
 * #primaryBufferedMutator#mutate(Mutation)} etc. will also throw.
 *
 * <p>We want to forward every exception thrown by user supplied error handler back to the user.
 * Those exceptions can be thrown in two places: first - when the user calls {@link
 * BufferedMutator#mutate(List)} or {@link BufferedMutator#flush()} on our MirroringBufferedMutator
 * and we synchronously call corresponding operation on {@link #primaryBufferedMutator}; second -
 * when we perform an asynchronous {@link BufferedMutator#flush()} on primaryBufferedMutator from a
 * worker thread. In the first case the exception is rethrown to the user directly from the called
 * method. In the second case we gather the asynchronously caught exception in a {@link
 * #exceptionsToBeReportedToTheUser} structure and rethrow them on next user interaction with our
 * MirroringBufferedMutator.
 *
 * <p>In such a way we ensure two things - we are notified about every failed mutation and the user
 * receives every exception that was thrown by {@link #primaryBufferedMutator}.
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
   * <p>This set uses {@code WeakReferences<Row>} as keys and compares their content using {@code
   * ==} instead of {@code equals}. This is faster than comparing Rows using {@code equals} and is
   * safe because we always check if a specific Row object has failed.
   */
  private final ConcurrentRowSetWithWeakKeys failedPrimaryOperations =
      new ConcurrentRowSetWithWeakKeys();
  /** Stores exceptions thrown by asynchronous operations that were not yet thrown to the user. */
  private final UserExceptionsBuffer exceptionsToBeReportedToTheUser = new UserExceptionsBuffer();

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
      Timestamper timestamper,
      MirroringTracer mirroringTracer)
      throws IOException {
    super(
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        configuration,
        executorService,
        timestamper,
        mirroringTracer);
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.flowController = flowController;
  }

  @Override
  protected void mutateScoped(final List<? extends Mutation> list) throws IOException {
    AccumulatedExceptions primaryExceptions = new AccumulatedExceptions();
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
      primaryExceptions.add(e);
    } catch (RuntimeException e) {
      primaryExceptions.add(e);
    } finally {
      try {
        // This call might block - we have confirmed that mutate() calls on BufferedMutator from
        // HBase client library might also block.
        // Submitting write errors to secondaryWriteErrorConsumer in case of exceptions is handled
        // by this method.
        addSecondaryMutation(list);
      } catch (InterruptedException | ExecutionException e) {
        setInterruptedFlagIfInterruptedException(e);
        primaryExceptions.add(new IOException(e));
      } catch (RuntimeException e) {
        primaryExceptions.add(e);
      }
      primaryExceptions.rethrowIfCaptured();
    }
    // If primary #mutate() has thrown, the exception is propagated to the user and we don't reach
    // here.
    // Otherwise we will try to throw exceptions thrown by asynchronous #flush() on primary, if
    // there were any.
    throwExceptionIfAvailable();
  }

  private void addSecondaryMutation(List<? extends Mutation> mutations)
      throws ExecutionException, InterruptedException {
    try {
      RequestResourcesDescription resourcesDescription = new RequestResourcesDescription(mutations);
      ListenableFuture<ResourceReservation> reservationFuture =
          flowController.asyncRequestResource(resourcesDescription);

      ResourceReservation reservation;
      try (Scope scope = this.mirroringTracer.spanFactory.flowControlScope()) {
        reservation = reservationFuture.get();
      }
      storeResourcesAndFlushIfNeeded(new Entry(mutations, reservation), resourcesDescription);
    } catch (InterruptedException | ExecutionException | RuntimeException e) {
      // We won't write those mutations to secondary database, they should be reported to
      // secondaryWriteErrorConsumer.
      reportWriteErrors(mutations, e);
      throw e;
    }
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
  protected FlushFutures scheduleFlushScoped(
      final List<Entry> dataToFlush, final FlushFutures previousFlushFutures) {
    final SettableFuture<Void> secondaryFlushFinished = SettableFuture.create();

    final ListenableFuture<Void> primaryFlushFinished =
        schedulePrimaryFlush(previousFlushFutures.primaryFlushFinished);

    final SettableFuture<Void> primaryFlushErrorsReported = SettableFuture.create();

    Futures.addCallback(
        primaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                primaryFlushErrorsReported.set(null);
                performSecondaryFlush(
                    dataToFlush,
                    secondaryFlushFinished,
                    previousFlushFutures.secondaryFlushFinished);
              }

              @Override
              public void onFailure(Throwable throwable) {
                if (throwable instanceof RetriesExhaustedWithDetailsException) {
                  // If user-defined listener has thrown an exception
                  // (RetriesExhaustedWithDetailsException is the only exception that can be
                  // thrown), we know that some of the writes failed. Our handler has already
                  // handled those errors. We should also rethrow this exception when user
                  // calls mutate/flush the next time.
                  exceptionsToBeReportedToTheUser.addRetriesExhaustedException(
                      (RetriesExhaustedWithDetailsException) throwable);
                  primaryFlushErrorsReported.set(null);

                  performSecondaryFlush(
                      dataToFlush,
                      secondaryFlushFinished,
                      previousFlushFutures.secondaryFlushFinished);
                } else {
                  // In other cases, we do not know what caused the error and we have no idea
                  // what was really written to the primary DB. We will behave as if nothing was
                  // written and throw the exception to the user. Writing mutations to the faillog
                  // would cause confusion as the user would think that those writes were successful
                  // on primary, but they were not.
                  exceptionsToBeReportedToTheUser.addThrowable(throwable);
                  primaryFlushErrorsReported.set(null);

                  releaseReservations(dataToFlush);
                  secondaryFlushFinished.setException(throwable);
                }
              }
            }),
        MoreExecutors.directExecutor());
    return new FlushFutures(
        primaryFlushFinished,
        secondaryFlushFinished,
        // Both flushes have finished when the secondary has finished because flushes are called
        // sequentially.
        secondaryFlushFinished,
        // Flush operation can be unblocked after errors (if any) are stored in buffers.
        primaryFlushErrorsReported);
  }

  private void performSecondaryFlush(
      List<Entry> dataToFlush,
      SettableFuture<Void> completionFuture,
      ListenableFuture<?> previousFlushCompletedFuture) {

    List<Mutation> mutations = Entry.mergeMutations(dataToFlush);
    final List<? extends Mutation> successfulOperations = removeFailedMutations(mutations);

    try {
      try {
        previousFlushCompletedFuture.get();
      } catch (ExecutionException ignored) {
        // InterruptedExceptions are threaded as failed mutations.
        // ExecutionExceptions are ignored, we only care if the previous flush finished, not its
        // result.
      }

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

  private static void releaseReservations(List<Entry> entries) {
    for (Entry entry : entries) {
      entry.reservation.release();
    }
  }

  /**
   * Iterates over {@code dataToFlush} and checks if any of the mutations in it have failed by
   * consulting {@link #failedPrimaryOperations} collection. Failed mutations are removed from
   * {@link #failedPrimaryOperations}.
   *
   * <p>This method is called from secondary thread that performs asynchronous flush() of {@link
   * #primaryBufferedMutator} and {@link #failedPrimaryOperations} might contain mutations that are
   * not in {@code dataToFlush} parameter (for instance, asynchronous flush was scheduled after one
   * call to mutate(), but the user thread performed two mutate() calls before the async task was
   * started - the flush() on primary would flush mutations from both mutate() calls, but
   * dataToFlush would only contain mutations from the first one). For this reason we cannot just
   * clear the {@link #failedPrimaryOperations} collection, if there are some operations left in it
   * after this method ends then they will be removed in one of subsequent calls.
   *
   * @return List of successful mutations.
   */
  private List<? extends Mutation> removeFailedMutations(List<? extends Mutation> dataToFlush) {
    List<Mutation> successfulMutations = new ArrayList<>();
    for (Mutation mutation : dataToFlush) {
      if (!this.failedPrimaryOperations.remove(mutation)) {
        successfulMutations.add(mutation);
      }
    }
    return successfulMutations;
  }

  @Override
  protected void throwExceptionIfAvailable() throws IOException {
    this.exceptionsToBeReportedToTheUser.throwAccumulatedExceptions();
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

  /**
   * Set of {@link Row} objects that keeps weak references to them for faster comparision. It is
   * threadsafe.
   *
   * <p>Underlying map uses {@code WeakReferences<Row>} as keys and compares their content using
   * {@code ==} instead of {@code equals}. This is faster than comparing Rows using {@code equals}
   * and is safe, because we always check if a specific {@link Row} object has failed.
   */
  static class ConcurrentRowSetWithWeakKeys {
    private final Set<Row> set =
        Collections.newSetFromMap((new MapMaker()).weakKeys().<Row, Boolean>makeMap());

    public void add(Row entry) {
      set.add(entry);
    }

    public boolean remove(Row entry) {
      return set.remove(entry);
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

  /**
   * Stores exceptions thrown by asynchronous primary mutations that should be reported to the user.
   *
   * <p>{@link RetriesExhaustedWithDetailsException} are handled separately because multiple such
   * exceptions can be combined into a single exception and throw to the user only once.
   *
   * <p>Thread-safe.
   */
  private static class UserExceptionsBuffer {
    private final Object retriesExhaustedWithDetailsExceptionListLock = new Object();
    /** Thread-safe. */
    private final ConcurrentLinkedQueue<Throwable> otherExceptionsList =
        new ConcurrentLinkedQueue<>();
    /** Locked by {@link #retriesExhaustedWithDetailsExceptionListLock} */
    private List<RetriesExhaustedWithDetailsException> retriesExhaustedWithDetailsExceptionList =
        new ArrayList<>();

    private static RetriesExhaustedWithDetailsException mergeRetiresExhaustedExceptions(
        List<RetriesExhaustedWithDetailsException> exceptions) {
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

    public void addRetriesExhaustedException(RetriesExhaustedWithDetailsException e) {
      synchronized (this.retriesExhaustedWithDetailsExceptionListLock) {
        this.retriesExhaustedWithDetailsExceptionList.add(e);
      }
    }

    public void addThrowable(Throwable e) {
      this.otherExceptionsList.add(e);
    }

    /**
     * This method first throws oldest non-{@link RetriesExhaustedWithDetailsException} exception.
     * If there is no such exception, then all {@link RetriesExhaustedWithDetailsException} are
     * accumulated and thrown at once. If no exceptions were accumulated then nothing is thrown.
     */
    public void throwAccumulatedExceptions() throws IOException {
      IOException exception = getOldestIOException();
      if (exception != null) {
        throw exception;
      }

      RetriesExhaustedWithDetailsException e = getMergedRetiresExhaustedExceptions();
      if (e != null) {
        throw e;
      }
    }

    private IOException getOldestIOException() {
      Throwable operationException = this.otherExceptionsList.poll();
      if (operationException == null) {
        return null;
      }
      if (operationException instanceof IOException) {
        return (IOException) operationException;
      }
      return new IOException(operationException);
    }

    private RetriesExhaustedWithDetailsException getMergedRetiresExhaustedExceptions() {
      List<RetriesExhaustedWithDetailsException> exceptions;
      synchronized (this.retriesExhaustedWithDetailsExceptionListLock) {
        if (this.retriesExhaustedWithDetailsExceptionList.isEmpty()) {
          return null;
        }
        exceptions = this.retriesExhaustedWithDetailsExceptionList;
        this.retriesExhaustedWithDetailsExceptionList = new ArrayList<>();
      }

      return mergeRetiresExhaustedExceptions(exceptions);
    }
  }
}
