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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * BufferedMutator that mirrors writes performed on first database to secondary database.
 *
 * <p>We want to perform a secondary write only if we are certain that it was successfully applied
 * on primary database. The HBase 1.x API doesn't give its user any indication when asynchronous
 * writes were performed, only performing a synchronous {@link BufferedMutator#flush()} ensures that
 * all previously buffered mutations are done. To achieve our goal we store a copy of all mutations
 * sent to primary BufferedMutator in a internal buffer. When size of the buffer reaches a threshold
 * of {@link
 * com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper#MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH}
 * bytes, we perform a flush in a worker thread. After flush we pass collected mutations to
 * secondary BufferedMutator and flush it. Writes that have failed on primary are not forwarded to
 * secondary, writes that have failed on secondary are forwarded to {@link
 * SecondaryWriteErrorConsumer#consume(Mutation)} handler.
 *
 * <p>Moreover, we perform our custom flow control to prevent unbounded growth of memory - calls to
 * mutate() might block if secondary database lags behind. We account size of all operations that
 * were placed in primary BufferedMutator but weren't yet executed and confirmed on secondary
 * BufferedMutator (or until we are informed that they have failed on primary).
 */
@InternalApi("For internal usage only")
public class MirroringBufferedMutator implements BufferedMutator {
  private static final Logger Log = new Logger(MirroringBufferedMutator.class);
  private final BufferedMutator primaryBufferedMutator;
  private final BufferedMutator secondaryBufferedMutator;
  private final FlowController flowController;
  private final ListeningExecutorService executorService;
  private final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  private final MirroringTracer mirroringTracer;

  /** Configuration that was used to configure this instance. */
  private final Configuration configuration;
  /** Parameters that were used to create this instance. */
  private final BufferedMutatorParams bufferedMutatorParams;
  /** Size that {@link #mutationsBuffer} should reach to invoke a flush() on primary database. */
  private final long mutationsBufferFlushIntervalBytes;

  /**
   * Set of {@link Row}s that were passed to primary BufferedMutator but failed. We create a entry
   * in this collection every time our error handler is called by primary BufferedMutator. Those
   * entries are consulted before we perform mutations on secondary BufferedMutator, if a {@link
   * Row} instance scheduled for insertion is in this collection, then it is omitted and
   * corresponding entry is removed from the set.
   */
  private final Set<Row> failedPrimaryOperations =
      Collections.newSetFromMap(new ConcurrentHashMap<Row, Boolean>());

  /**
   * Internal buffer with mutations that were passed to primary BufferedMutator but were not yet
   * scheduled to be confirmed and written to the secondary database in {@link #scheduleFlush()}.
   */
  private ArrayList<Mutation> mutationsBuffer;

  private long mutationsBufferSizeBytes;

  /**
   * {@link ResourceReservation}s obtained from {@link #flowController} that represent resources
   * used by mutations kept in {@link #mutationsBuffer}.
   */
  private ArrayList<ResourceReservation> reservations;

  /**
   * Exceptions caught when performing asynchronous flush() on primary BufferedMutator that should
   * be rethrown to inform the user about failed writes.
   */
  private List<RetriesExhaustedWithDetailsException> exceptionsToBeThrown = new ArrayList<>();

  private boolean closed = false;

  public MirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer)
      throws IOException {
    final ExceptionListener userListener = bufferedMutatorParams.getListener();
    ExceptionListener primaryErrorsListener =
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
            handlePrimaryException(e);
            userListener.onException(e, bufferedMutator);
          }
        };

    ExceptionListener secondaryErrorsListener =
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
            handleSecondaryExceptions(e);
          }
        };

    this.primaryBufferedMutator =
        primaryConnection.getBufferedMutator(
            copyMutatorParamsAndSetListener(bufferedMutatorParams, primaryErrorsListener));
    this.secondaryBufferedMutator =
        secondaryConnection.getBufferedMutator(
            copyMutatorParamsAndSetListener(bufferedMutatorParams, secondaryErrorsListener));
    this.flowController = flowController;
    this.mutationsBufferFlushIntervalBytes =
        configuration.mirroringOptions.bufferedMutatorBytesToFlush;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.configuration = configuration;
    this.bufferedMutatorParams = bufferedMutatorParams;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;

    this.mutationsBuffer = new ArrayList<>();
    this.reservations = new ArrayList<>();
    this.mirroringTracer = mirroringTracer;
  }

  @Override
  public TableName getName() {
    return this.bufferedMutatorParams.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public void mutate(Mutation mutation) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_MUTATE)) {
      mutateScoped(Collections.singletonList(mutation));
    }
  }

  @Override
  public void mutate(final List<? extends Mutation> list) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST)) {
      mutateScoped(list);
    }
  }

  private void mutateScoped(final List<? extends Mutation> list) throws IOException {
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
      reportWriteErrors(mutations);

      setInterruptedFlagInInterruptedException(e);
      if (primaryException != null) {
        // We are currently in a finally block handling an exception, we shouldn't throw anything.
        primaryException.addSuppressed(e);
        return;
      } else {
        throw new IOException(e);
      }
    }

    synchronized (this) {
      this.mutationsBuffer.addAll(mutations);
      this.reservations.add(reservation);
      this.mutationsBufferSizeBytes += resourcesDescription.sizeInBytes;
      if (this.mutationsBufferSizeBytes > this.mutationsBufferFlushIntervalBytes) {
        // We are not afraid of multiple simultaneous flushes:
        // - HBase clients are thread-safe.
        // - Each failed Row should be reported and placed in `failedPrimaryOperations` once.
        // - Each issued Row will be consulted with `failedPrimaryOperations` only once, because
        //   each flush sets up a clean buffer for incoming mutations.
        scheduleFlush();
      }
    }
  }

  private void reportWriteErrors(List<? extends Mutation> mutations) {
    try (Scope scope = this.mirroringTracer.spanFactory.writeErrorScope()) {
      this.secondaryWriteErrorConsumer.consume(
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, mutations);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_CLOSE)) {
      if (this.closed) {
        this.mirroringTracer
            .spanFactory
            .getCurrentSpan()
            .addAnnotation("MirroringBufferedMutator closed more than once.");
        return;
      }
      this.closed = true;

      List<IOException> exceptions = new ArrayList<>();

      try {
        scheduleFlush().secondaryFlushFinished.get();
      } catch (InterruptedException | ExecutionException e) {
        setInterruptedFlagInInterruptedException(e);
        exceptions.add(new IOException(e));
      }
      try {
        this.mirroringTracer.spanFactory.wrapPrimaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                MirroringBufferedMutator.this.primaryBufferedMutator.close();
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_CLOSE);
      } catch (IOException e) {
        exceptions.add(e);
      }
      try {
        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                MirroringBufferedMutator.this.secondaryBufferedMutator.close();
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_CLOSE);
      } catch (IOException e) {
        exceptions.add(e);
      }
      if (!exceptions.isEmpty()) {
        Iterator<IOException> exceptionIterator = exceptions.iterator();
        IOException firstException = exceptionIterator.next();
        while (exceptionIterator.hasNext()) {
          firstException.addSuppressed(exceptionIterator.next());
        }
        throw firstException;
      }
    }
  }

  @Override
  public void flush() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_FLUSH)) {
      try {
        scheduleFlush().primaryFlushFinished.get();
      } catch (InterruptedException | ExecutionException e) {
        setInterruptedFlagInInterruptedException(e);
        throw new IOException(e);
      }
      throwExceptionIfAvailable();
    }
  }

  @Override
  public long getWriteBufferSize() {
    return this.bufferedMutatorParams.getWriteBufferSize();
  }

  private void handlePrimaryException(RetriesExhaustedWithDetailsException e) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      failedPrimaryOperations.add(e.getRow(i));
    }
  }

  private void handleSecondaryExceptions(RetriesExhaustedWithDetailsException e) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < e.getNumExceptions(); i++) {
      mutations.add((Mutation) e.getRow(i));
    }
    reportWriteErrors(mutations);
  }

  private BufferedMutatorParams copyMutatorParamsAndSetListener(
      BufferedMutatorParams bufferedMutatorParams, ExceptionListener exceptionListener) {
    BufferedMutatorParams params = new BufferedMutatorParams(bufferedMutatorParams.getTableName());
    params.writeBufferSize(bufferedMutatorParams.getWriteBufferSize());
    params.pool(bufferedMutatorParams.getPool());
    params.maxKeyValueSize(bufferedMutatorParams.getMaxKeyValueSize());
    params.listener(exceptionListener);
    return params;
  }

  private static class FlushFutures {
    ListenableFuture<Void> primaryFlushFinished;
    ListenableFuture<Void> secondaryFlushFinished;

    public FlushFutures(
        ListenableFuture<Void> primaryFlushFinished, SettableFuture<Void> secondaryFlushFinished) {
      this.primaryFlushFinished = primaryFlushFinished;
      this.secondaryFlushFinished = secondaryFlushFinished;
    }
  }

  private synchronized FlushFutures scheduleFlush() {
    try (Scope scope = this.mirroringTracer.spanFactory.scheduleFlushScope()) {
      this.mutationsBufferSizeBytes = 0;

      final List<? extends Mutation> dataToFlush = this.mutationsBuffer;
      this.mutationsBuffer = new ArrayList<>();

      final List<ResourceReservation> flushReservations = this.reservations;
      this.reservations = new ArrayList<>();

      final SettableFuture<Void> secondaryFlushFinished = SettableFuture.create();

      ListenableFuture<Void> primaryFlushFinished =
          this.executorService.submit(
              this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
                  new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                      MirroringBufferedMutator.this.mirroringTracer.spanFactory
                          .wrapPrimaryOperation(
                              new CallableThrowingIOException<Void>() {
                                @Override
                                public Void call() throws IOException {
                                  MirroringBufferedMutator.this.primaryBufferedMutator.flush();
                                  return null;
                                }
                              },
                              HBaseOperation.BUFFERED_MUTATOR_FLUSH);
                      return null;
                    }
                  }));

      Futures.addCallback(
          primaryFlushFinished,
          this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
              new FutureCallback<Void>() {
                @Override
                public void onSuccess(@NullableDecl Void aVoid) {
                  performSecondaryFlush(dataToFlush, flushReservations, secondaryFlushFinished);
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

                    performSecondaryFlush(dataToFlush, flushReservations, secondaryFlushFinished);
                  } else {
                    // In other cases, we do not know what caused the error and we have no idea
                    // what was really written to the primary DB, the best we can do is write
                    // them to on-disk log. Trying to save them to secondary database is not a
                    // good idea - if current thread was interrupted then next flush might also
                    // be, only increasing our confusion, moreover, that may cause secondary
                    // writes that were not completed on primary.
                    reportWriteErrors(dataToFlush);
                    releaseReservations(flushReservations);
                    secondaryFlushFinished.setException(throwable);
                  }
                }
              }),
          MoreExecutors.directExecutor());
      return new FlushFutures(primaryFlushFinished, secondaryFlushFinished);
    }
  }

  private synchronized void saveExceptionToBeThrown(
      RetriesExhaustedWithDetailsException exception) {
    this.exceptionsToBeThrown.add(exception);
  }

  private RetriesExhaustedWithDetailsException getExceptionsToBeThrown() {
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

  private void performSecondaryFlush(
      List<? extends Mutation> dataToFlush,
      List<ResourceReservation> flushReservations,
      SettableFuture<Void> completionFuture) {
    final List<? extends Mutation> successfulOperations = removeFailedMutations(dataToFlush);
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
      releaseReservations(flushReservations);
      completionFuture.set(null);
    } catch (Throwable e) {
      // Our listener is registered and should catch non-fatal errors. This is either
      // InterruptedIOException or some RuntimeError, in both cases we should consider operation as
      // not completed - the worst that can happen is that we will have some writes in both
      // secondary database and on-disk log.
      reportWriteErrors(dataToFlush);
      releaseReservations(flushReservations);
      completionFuture.setException(e);
    }
  }

  private void releaseReservations(List<ResourceReservation> flushReservations) {
    for (ResourceReservation reservation : flushReservations) {
      reservation.release();
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

  private void setInterruptedFlagInInterruptedException(Exception e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }
}
