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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounterUtils.holdReferenceUntilCompletion;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.HierarchicalReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.Timestamper;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

/**
 * Base class for {@code BufferedMutator}s that mirrors writes performed on first database to
 * secondary database.
 *
 * <p>Consult {@link SequentialMirroringBufferedMutator} and {@link
 * ConcurrentMirroringBufferedMutator} for available mirroring strategies.
 *
 * <p>This base class handles tracing, management of internal mutations buffer and starting
 * asynchronous flushes.
 *
 * <p>Sharing code by inheritance was the cleanest approach we could come up with.
 */
@InternalApi("For internal usage only")
public abstract class MirroringBufferedMutator<BufferEntryType> implements BufferedMutator {

  private final SettableFuture<Void> closedFuture = SettableFuture.create();
  private final Timestamper timestamper;

  public static BufferedMutator create(
      boolean concurrent,
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      ReferenceCounter connectionReferenceCounter,
      Timestamper timestamper,
      MirroringTracer mirroringTracer)
      throws IOException {
    if (concurrent) {
      return new ConcurrentMirroringBufferedMutator(
          primaryConnection,
          secondaryConnection,
          bufferedMutatorParams,
          configuration,
          executorService,
          connectionReferenceCounter,
          timestamper,
          mirroringTracer);
    } else {
      return new SequentialMirroringBufferedMutator(
          primaryConnection,
          secondaryConnection,
          bufferedMutatorParams,
          configuration,
          flowController,
          executorService,
          secondaryWriteErrorConsumer,
          connectionReferenceCounter,
          timestamper,
          mirroringTracer);
    }
  }

  protected final BufferedMutator primaryBufferedMutator;
  protected final BufferedMutator secondaryBufferedMutator;
  protected final ListeningExecutorService executorService;
  protected final MirroringTracer mirroringTracer;

  /** Configuration that was used to configure this instance. */
  protected final MirroringConfiguration configuration;
  /** Parameters that were used to create this instance. */
  private final BufferedMutatorParams bufferedMutatorParams;
  /**
   * Size that mutations kept in {@link FlushSerializer#mutationEntries} should reach to invoke a
   * asynchronous flush() on the primary database.
   */
  protected final long mutationsBufferFlushThresholdBytes;

  private final FlushSerializer flushSerializer;

  /** ExceptionListener supplied by the user. */
  protected final ExceptionListener userListener;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final HierarchicalReferenceCounter referenceCounter;

  public MirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      ExecutorService executorService,
      ReferenceCounter connectionReferenceCounter,
      Timestamper timestamper,
      MirroringTracer mirroringTracer)
      throws IOException {
    this.userListener = bufferedMutatorParams.getListener();

    // Our primary exception listeners do not throw exception but might call user-supplied handler
    // which might throw. All exceptions thrown by that handler are rethrown to the user in places
    // where they expect it.
    ExceptionListener primaryErrorsListener =
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
            handlePrimaryException(e);
          }
        };

    ExceptionListener secondaryErrorsListener =
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) {
            handleSecondaryException(e);
          }
        };

    this.primaryBufferedMutator =
        primaryConnection.getBufferedMutator(
            createBufferedMutatorParamsWithListener(bufferedMutatorParams, primaryErrorsListener));
    this.secondaryBufferedMutator =
        secondaryConnection.getBufferedMutator(
            createBufferedMutatorParamsWithListener(
                bufferedMutatorParams, secondaryErrorsListener));
    this.mutationsBufferFlushThresholdBytes =
        configuration.mirroringOptions.bufferedMutatorBytesToFlush;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.configuration = configuration;
    this.bufferedMutatorParams = bufferedMutatorParams;

    this.mirroringTracer = mirroringTracer;
    this.flushSerializer = new FlushSerializer();
    this.timestamper = timestamper;

    this.referenceCounter = new HierarchicalReferenceCounter(connectionReferenceCounter);
  }

  @Override
  public void mutate(Mutation mutation) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_MUTATE)) {
      mutation = timestamper.fillTimestamp(mutation);
      mutateScoped(Collections.singletonList(mutation));
    }
  }

  @Override
  public void mutate(final List<? extends Mutation> list) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST)) {
      List<? extends Mutation> timestampedList = timestamper.fillTimestamp(list);
      mutateScoped(timestampedList);
    }
  }

  protected abstract void mutateScoped(final List<? extends Mutation> list) throws IOException;

  @Override
  public void flush() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_FLUSH)) {
      try {
        // Wait until flush has finished.
        scheduleFlush().flushOperationCanContinueFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        setInterruptedFlagIfInterruptedException(e);
        throw new IOException(e);
      }
      // If the #flush() above has thrown an exception, it will be propagated to the user now.
      // Otherwise we might still have an exception from an asynchronous #flush() to propagate.

      // If the flush operation started in this method throws, we guarantee that the exception will
      // be propagated to the user. The rationale depends on whether we use the synchronous or
      // concurrent implementation.
      // Synchronous case:
      //   flushOperationCanContinueFuture is completed after primaryFlushErrorsReported is set,
      //   which happens after storing errors in the exceptionsToBeReportedToTheUser.
      // Concurrent case:
      //   flushOperationCanContinueFuture is completed after bothFlushesFinished is set,
      //   which happens after storing errors from both primary and secondary flushes in the
      //   mirroringExceptionBuilder.
      throwExceptionIfAvailable();
    }
  }

  protected abstract void throwExceptionIfAvailable() throws IOException;

  /**
   * Schedules asynchronous flushes of both buffered mutators (either sequentially or concurrently).
   *
   * @param dataToFlush List of entries that are were accumulated since last flush and should be
   *     flushed now.
   * @param previousFlushFutures Futures that will be completed when previously scheduled flush will
   *     finish. Used to serialize asynchronous flushes.
   * @return a pack of Futures that complete in various stages of flush operation.
   */
  protected abstract FlushFutures scheduleFlushScoped(
      List<BufferEntryType> dataToFlush, FlushFutures previousFlushFutures);

  abstract void handlePrimaryException(RetriesExhaustedWithDetailsException e)
      throws RetriesExhaustedWithDetailsException;

  abstract void handleSecondaryException(RetriesExhaustedWithDetailsException e);

  protected final void storeResourcesAndFlushIfNeeded(
      BufferEntryType entry, RequestResourcesDescription resourcesDescription) {
    this.flushSerializer.storeResourcesAndFlushIfThresholdIsExceeded(entry, resourcesDescription);
  }

  protected final FlushFutures scheduleFlush() {
    return this.flushSerializer.scheduleFlush();
  }

  private void flushBufferedMutatorBeforeClosing()
      throws ExecutionException, InterruptedException, TimeoutException {
    scheduleFlush()
        .flushOperationCanContinueFuture
        .get(
            this.configuration.mirroringOptions.connectionTerminationTimeoutMillis,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public final void close() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_CLOSE)) {
      if (this.closed.getAndSet(true)) {
        this.mirroringTracer
            .spanFactory
            .getCurrentSpan()
            .addAnnotation("MirroringBufferedMutator closed more than once.");
        return;
      }

      final AccumulatedExceptions exceptions = new AccumulatedExceptions();

      try {
        // Schedule flush of all buffered data and:
        // sequential) wait for primary flush to finish;
        // concurrent) wait for both flushes to finish.
        flushBufferedMutatorBeforeClosing();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        setInterruptedFlagIfInterruptedException(e);
        exceptions.add(new IOException(e));
      }

      // Close the primary buffered mutator, if is flushed in both cases.
      try {
        closePrimaryBufferedMutator();
      } catch (IOException e) {
        exceptions.add(e);
      }

      // We are freeing the initial reference to current level reference counter.
      referenceCounter.current.decrementReferenceCount();
      // But we are scheduling asynchronous secondary operation and we should increment our parent's
      // ref counter until this operation is finished.
      holdReferenceUntilCompletion(this.referenceCounter.parent, this.closedFuture);

      try {
        // Schedule closing secondary buffered mutator.
        // sequential) it will be called at some point in the future.
        // concurrent) it will be called immediately because all asynchronous operations are already
        // finished and directExecutor will call it in this thread.
        referenceCounter
            .current
            .getOnLastReferenceClosed()
            .addListener(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      closeSecondaryBufferedMutator();
                      closedFuture.set(null);
                    } catch (IOException e) {
                      closedFuture.setException(e);
                    }
                  }
                },
                MoreExecutors.directExecutor());
      } catch (RuntimeException e) {
        exceptions.add(e);
      }

      exceptions.rethrowIfCaptured();
      // Throw exceptions from async operations, if any.
      // synchronous) all exceptions from primary operation were reported, because
      // flushBufferedMutatorBeforeClosing waits for errors to be reported after primary operation
      // finishes;
      // concurrent) all exceptions are reported because flushBufferedMutatorBeforeClosing waits for
      // both flushes to finish and report errors.
      throwExceptionIfAvailable();
    }
  }

  private void closePrimaryBufferedMutator() throws IOException {
    this.mirroringTracer.spanFactory.wrapPrimaryOperation(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            MirroringBufferedMutator.this.primaryBufferedMutator.close();
            return null;
          }
        },
        HBaseOperation.BUFFERED_MUTATOR_CLOSE);
  }

  private void closeSecondaryBufferedMutator() throws IOException {
    mirroringTracer.spanFactory.wrapSecondaryOperation(
        new CallableThrowingIOException<Void>() {
          @Override
          public Void call() throws IOException {
            MirroringBufferedMutator.this.secondaryBufferedMutator.close();
            return null;
          }
        },
        HBaseOperation.BUFFERED_MUTATOR_CLOSE);
  }

  @Override
  public long getWriteBufferSize() {
    return this.bufferedMutatorParams.getWriteBufferSize();
  }

  @Override
  public TableName getName() {
    return this.bufferedMutatorParams.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration.baseConfiguration;
  }

  protected final ListenableFuture<Void> schedulePrimaryFlush(
      final ListenableFuture<?> previousFlushCompletedFuture) {
    return this.executorService.submit(
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                mirroringTracer.spanFactory.wrapPrimaryOperation(
                    createFlushTask(primaryBufferedMutator, previousFlushCompletedFuture),
                    HBaseOperation.BUFFERED_MUTATOR_FLUSH);
                return null;
              }
            }));
  }

  protected final CallableThrowingIOException<Void> createFlushTask(
      final BufferedMutator bufferedMutator,
      final ListenableFuture<?> previousFlushCompletedFuture) {
    return new CallableThrowingIOException<Void>() {
      @Override
      public Void call() throws IOException {
        try {
          previousFlushCompletedFuture.get();
        } catch (InterruptedException | ExecutionException ignored) {
          // We do not care about errors, just if the previous flush is over.
        }
        bufferedMutator.flush();
        return null;
      }
    };
  }

  protected final void setInterruptedFlagIfInterruptedException(Exception e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Create a new instance of {@link BufferedMutatorParams} based on supplied parameters but with
   * replaced listener. Objects created by this method can be safely used for creating underlying
   * buffered mutator.
   */
  private static BufferedMutatorParams createBufferedMutatorParamsWithListener(
      BufferedMutatorParams bufferedMutatorParams, ExceptionListener exceptionListener) {
    BufferedMutatorParams params = new BufferedMutatorParams(bufferedMutatorParams.getTableName());
    params.writeBufferSize(bufferedMutatorParams.getWriteBufferSize());
    params.pool(bufferedMutatorParams.getPool());
    params.maxKeyValueSize(bufferedMutatorParams.getMaxKeyValueSize());
    params.listener(exceptionListener);
    return params;
  }

  protected static class FlushFutures {

    /**
     * Future completed when the primary operation is finished. Used to sequence asynchronous
     * flushes of the primary buffered mutator.
     */
    public final ListenableFuture<Void> primaryFlushFinished;

    /**
     * Future completed when the secondary operation is finished. Used to sequence asynchronous
     * flushes of the secondary buffered mutator.
     */
    public final ListenableFuture<Void> secondaryFlushFinished;

    /**
     * Future completed when both asynchronous flush operations are finished. Used in {@link
     * ConcurrentMirroringBufferedMutator#close()} method.
     */
    public final ListenableFuture<Void> bothFlushesFinished;

    /**
     * Future completed when an implementation decides that the {@link BufferedMutator#flush()}
     * operation performed by the user can unblock. If the asynchronous flush operation throws an
     * exception, the implementation should make sure the exception will be correctly read by {@link
     * #throwExceptionIfAvailable()} method, which is called immediately after the completion of
     * this future.
     */
    public final ListenableFuture<Void> flushOperationCanContinueFuture;

    public FlushFutures(
        ListenableFuture<Void> primaryFlushFinished,
        ListenableFuture<Void> secondaryFlushFinished,
        ListenableFuture<Void> bothFlushesFinished,
        ListenableFuture<Void> flushOperationCanContinueFuture) {
      this.primaryFlushFinished = primaryFlushFinished;
      this.secondaryFlushFinished = secondaryFlushFinished;
      this.bothFlushesFinished = bothFlushesFinished;
      this.flushOperationCanContinueFuture = flushOperationCanContinueFuture;
    }
  }

  /**
   * Helper class that manager performing asynchronous flush operations and correctly ordering them.
   *
   * <p>This is a non-static inner class with only a single instance per MirroringBufferedMutator
   * instance, but it is not inlined to facilitate correct synchronization of scheduling flushes and
   * to logically separate flush scheduling from other concerns.
   *
   * <p>Thread-safe.
   */
  class FlushSerializer {

    /**
     * Internal buffer that should keep mutations that were not yet flushed asynchronously. Type of
     * the entry is specified by subclasses and can contain more elements than just mutations, e.g.
     * related resource reservations.
     *
     * <p>{@link #storeResourcesAndFlushIfThresholdIsExceeded} relies on the fact that access to
     * this field is synchronized.
     *
     * <p>{@link BufferedMutations} is not thread safe and usage of this field should be
     * synchronized on current instance of {@link FlushSerializer}.
     */
    private final BufferedMutations<BufferEntryType> mutationEntries;

    /**
     * We have to ensure that order of asynchronously called {@link BufferedMutator#flush()} is the
     * same as order in which callbacks for these operations were created. To enforce this property
     * each scheduled flush will wait for previously scheduled flush to finish before performing its
     * operation. We are storing futures of last scheduled flush operation in this field.
     *
     * <p>Because each scheduled flush has to wait for previously scheduled flush we are implicitly
     * creating a chain of flushes to be performed. Length of this chain is limited by the
     * FlowController - once there are not more resources to be used asynchronously scheduling of
     * new operations will block.
     *
     * <p>Access to {@code lastFlushFutures} field should be synchronized on current instance of
     * {@link FlushSerializer}.
     *
     * <p>We have to ensure the ordering to prevent the following scenario:
     *
     * <ol>
     *   <li>main thread: user calls mutate([1,2,3])
     *   <li>main thread: scheduleFlush with dataToFlush = [1,2,3] (flush1) because threshold is
     *       exceeded.
     *   <li>main thread: user call flush()
     *   <li>main thread: scheduleFlush with dataToFlush = [] (flush2).
     *   <li>main thread: waits for flush2 to finish.
     *   <li>worker thread 1: performs flush1 and blocks, underlying buffered mutator flushes
     *       [1,2,3].
     *   <li>worker thread 2: performs flush2 - there is nothing more to flush, call finishes
     *       immediately (there is not guarantee that this call would wait for flush1 to finish).
     *   <li>main thread: continues running, but flush1 is still in progress.
     * </ol>
     *
     * <p>Ensuring the order of flushes forces flush2 to be run after flush1 is finished.
     */
    private FlushFutures lastFlushFutures = createCompletedFlushFutures();

    public FlushSerializer() {
      this.mutationEntries = new BufferedMutations<>();
    }

    private FlushFutures createCompletedFlushFutures() {
      SettableFuture<Void> future = SettableFuture.create();
      future.set(null);
      return new FlushFutures(future, future, future, future);
    }

    public final synchronized FlushFutures scheduleFlush() {
      // This method is synchronized to make sure that order of scheduled flushes matches order of
      // created dataToFlush lists.
      List<BufferEntryType> dataToFlush = this.mutationEntries.getAndReset();
      return scheduleFlush(dataToFlush);
    }

    public final synchronized void storeResourcesAndFlushIfThresholdIsExceeded(
        BufferEntryType entry, RequestResourcesDescription resourcesDescription) {
      // This method is synchronized to make sure that order of scheduled flushes matches order of
      // created dataToFlush lists.
      this.mutationEntries.add(entry, resourcesDescription.sizeInBytes);
      if (this.mutationEntries.getMutationsBufferSizeBytes() > mutationsBufferFlushThresholdBytes) {
        scheduleFlush(this.mutationEntries.getAndReset());
      }
    }

    private synchronized FlushFutures scheduleFlush(List<BufferEntryType> dataToFlush) {
      try (Scope scope = mirroringTracer.spanFactory.scheduleFlushScope()) {
        referenceCounter.incrementReferenceCount();

        FlushFutures resultFutures = scheduleFlushScoped(dataToFlush, lastFlushFutures);
        this.lastFlushFutures = resultFutures;

        resultFutures.secondaryFlushFinished.addListener(
            new Runnable() {
              @Override
              public void run() {
                referenceCounter.decrementReferenceCount();
              }
            },
            MoreExecutors.directExecutor());
        return resultFutures;
      }
    }
  }

  /**
   * A container for mutations that were issued to primary buffered mutator. Generic EntryType can
   * be used to store additional data with mutations (sequential buffered mutator uses it to keep
   * FlowController reservations).
   *
   * <p>Keeps track of total size of buffered mutations and detects if there are enough entries to
   * perform a flush.
   *
   * <p>not thread-safe, should be synchronized externally.
   */
  private static class BufferedMutations<EntryType> {
    private List<EntryType> mutationEntries;
    private long mutationsBufferSizeBytes;

    private BufferedMutations() {
      this.mutationEntries = new ArrayList<>();
      this.mutationsBufferSizeBytes = 0;
    }

    public void add(EntryType entry, long sizeInBytes) {
      this.mutationEntries.add(entry);
      this.mutationsBufferSizeBytes += sizeInBytes;
    }

    public long getMutationsBufferSizeBytes() {
      return this.mutationsBufferSizeBytes;
    }

    public List<EntryType> getAndReset() {
      List<EntryType> returnValue = this.mutationEntries;
      this.mutationEntries = new ArrayList<>();
      this.mutationsBufferSizeBytes = 0;
      return returnValue;
    }
  }
}
