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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ListenableReferenceCounter;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
 */
@InternalApi("For internal usage only")
public abstract class MirroringBufferedMutator<BufferEntryType> implements BufferedMutator {
  public static BufferedMutator create(
      boolean concurrent,
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer)
      throws IOException {
    if (concurrent) {
      return new ConcurrentMirroringBufferedMutator(
          primaryConnection,
          secondaryConnection,
          bufferedMutatorParams,
          configuration,
          executorService,
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
          mirroringTracer);
    }
  }

  protected final BufferedMutator primaryBufferedMutator;
  protected final BufferedMutator secondaryBufferedMutator;
  protected final ListeningExecutorService executorService;
  protected final MirroringTracer mirroringTracer;

  /** Configuration that was used to configure this instance. */
  private final Configuration configuration;
  /** Parameters that were used to create this instance. */
  private final BufferedMutatorParams bufferedMutatorParams;
  /**
   * Size that mutations kept in {@link FlushSerializer#mutationEntries} should reach to invoke a
   * asynchronous flush() on the primary database.
   */
  protected final long mutationsBufferFlushThresholdBytes;

  private final FlushSerializer<BufferEntryType> flushSerializer;

  /** ExceptionListener supplied by the user. */
  protected final ExceptionListener userListener;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ListenableReferenceCounter ongoingFlushesCounter = new ListenableReferenceCounter();

  public MirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      ExecutorService executorService,
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
    this.configuration = configuration.baseConfiguration;
    this.bufferedMutatorParams = bufferedMutatorParams;

    this.mirroringTracer = mirroringTracer;
    this.flushSerializer =
        new FlushSerializer<>(
            this,
            this.mutationsBufferFlushThresholdBytes,
            this.ongoingFlushesCounter,
            this.mirroringTracer);
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

  protected abstract void mutateScoped(final List<? extends Mutation> list) throws IOException;

  @Override
  public void flush() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_FLUSH)) {
      scopedFlush();
    }
  }

  protected abstract void scopedFlush() throws IOException;

  abstract void handlePrimaryException(RetriesExhaustedWithDetailsException e)
      throws RetriesExhaustedWithDetailsException;

  abstract void handleSecondaryException(RetriesExhaustedWithDetailsException e);

  protected final void storeResourcesAndFlushIfNeeded(
      BufferEntryType entry, RequestResourcesDescription resourcesDescription) {
    this.flushSerializer.storeResourcesAndFlushIfNeeded(entry, resourcesDescription);
  }

  protected final FlushFutures scheduleFlushAll() {
    return this.flushSerializer.scheduleFlushAll();
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

      List<IOException> exceptions = new ArrayList<>();

      try {
        scheduleFlushAll().secondaryFlushFinished.get();
        this.ongoingFlushesCounter.decrementReferenceCount();
        this.ongoingFlushesCounter.getOnLastReferenceClosed().get();
      } catch (InterruptedException | ExecutionException e) {
        setInterruptedFlagIfInterruptedException(e);
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
  public long getWriteBufferSize() {
    return this.bufferedMutatorParams.getWriteBufferSize();
  }

  @Override
  public TableName getName() {
    return this.bufferedMutatorParams.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
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
    public final ListenableFuture<Void> primaryFlushFinished;
    public final ListenableFuture<Void> secondaryFlushFinished;
    public final ListenableFuture<Void> bothFlushesFinished;

    public FlushFutures(
        ListenableFuture<Void> primaryFlushFinished,
        ListenableFuture<Void> secondaryFlushFinished,
        ListenableFuture<Void> bothFlushesFinished) {
      this.primaryFlushFinished = primaryFlushFinished;
      this.secondaryFlushFinished = secondaryFlushFinished;
      this.bothFlushesFinished = bothFlushesFinished;
    }
  }

  protected abstract FlushFutures scheduleFlushScoped(
      List<BufferEntryType> dataToFlush, FlushFutures previousFlushFutures);

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
   * Helper class that manager performing asynchronous flush operations and correctly ordering them.
   *
   * <p>Thread-safe.
   */
  static class FlushSerializer<BufferEntryType> {

    /**
     * We have to ensure that order of asynchronously called {@link BufferedMutator#flush()} is the
     * same as order in which callbacks for these operations were created. To enforce this property
     * each scheduled flush will wait for previously scheduled flush to finish before performing its
     * operation. We are storing futures of last scheduled flush operation in this field.
     *
     * <p>Access to this field should be synchronized by {@code synchronized(this)}
     *
     * <p>We have to ensure the ordering to prevent the following scenario:
     *
     * <ol>
     *   <li>main thread: scheduleFlush with callback using dataToFlush = [1,2,3] (flush1)
     *   <li>main thread: scheduleFlush with callback using dataToFlush = [] (flush2).
     *   <li>worker thread 1: call flush2, it blocks.
     *   <li>worker thread 2: call flush1, nothing more to flush (there's no guarantee that this
     *       flush would wait for flush2 to finish), callback with dataToFlush = [1,2,3] is called
     *       before corresponding mutations were flushed.
     * </ol>
     *
     * <p>Ensuring the order of flushes forces to be run after flush1 is finished.
     */
    private FlushFutures lastFlushFutures = createCompletedFlushFutures();

    /**
     * Internal buffer that should keep mutations that were not yet flushed asynchronously. Type of
     * the entry is specified by subclasses and can contain more elements than just mutations, e.g.
     * related resource reservations.
     */
    private final BufferedMutations<BufferEntryType> mutationEntries;

    private final ListenableReferenceCounter ongoingFlushesCounter;
    private final MirroringTracer mirroringTracer;
    private final MirroringBufferedMutator<BufferEntryType> bufferedMutator;

    public FlushSerializer(
        MirroringBufferedMutator<BufferEntryType> bufferedMutator,
        long mutationsBufferFlushThresholdBytes,
        ListenableReferenceCounter ongoingFlushesCounter,
        MirroringTracer mirroringTracer) {
      this.mutationEntries = new BufferedMutations<>(mutationsBufferFlushThresholdBytes);
      this.ongoingFlushesCounter = ongoingFlushesCounter;
      this.mirroringTracer = mirroringTracer;
      this.bufferedMutator = bufferedMutator;
    }

    private static FlushFutures createCompletedFlushFutures() {
      SettableFuture<Void> future = SettableFuture.create();
      future.set(null);
      return new FlushFutures(future, future, future);
    }

    public final synchronized void storeResourcesAndFlushIfNeeded(
        BufferEntryType entry, RequestResourcesDescription resourcesDescription) {
      // This method is synchronized to make sure that order of scheduled flushes matches order of
      // created dataToFlush lists.
      List<BufferEntryType> dataToFlush =
          this.mutationEntries.add(entry, resourcesDescription.sizeInBytes);
      if (dataToFlush != null) {
        scheduleFlush(dataToFlush);
      }
    }

    public final synchronized FlushFutures scheduleFlushAll() {
      // This method is synchronized to make sure that order of scheduled flushes matches order of
      // created dataToFlush lists.
      List<BufferEntryType> dataToFlush = this.mutationEntries.flushBuffer();
      return scheduleFlush(dataToFlush);
    }

    private synchronized FlushFutures scheduleFlush(List<BufferEntryType> dataToFlush) {
      try (Scope scope = this.mirroringTracer.spanFactory.scheduleFlushScope()) {
        this.ongoingFlushesCounter.incrementReferenceCount();

        FlushFutures resultFutures =
            this.bufferedMutator.scheduleFlushScoped(dataToFlush, lastFlushFutures);
        this.lastFlushFutures = resultFutures;

        resultFutures.secondaryFlushFinished.addListener(
            new Runnable() {
              @Override
              public void run() {
                ongoingFlushesCounter.decrementReferenceCount();
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
   * <p>Thread-safe.
   */
  private static class BufferedMutations<EntryType> {

    private List<EntryType> mutationEntries;
    private long mutationsBufferSizeBytes;
    protected final long mutationsBufferFlushThresholdBytes;

    private BufferedMutations(long mutationsBufferFlushThresholdBytes) {
      this.mutationsBufferFlushThresholdBytes = mutationsBufferFlushThresholdBytes;
      this.mutationEntries = new ArrayList<>();
      this.mutationsBufferSizeBytes = 0;
    }

    private synchronized List<EntryType> add(EntryType entry, long sizeInBytes) {
      this.mutationEntries.add(entry);
      this.mutationsBufferSizeBytes += sizeInBytes;
      if (this.mutationsBufferSizeBytes > this.mutationsBufferFlushThresholdBytes) {
        return flushBuffer();
      }
      return null;
    }

    private synchronized List<EntryType> flushBuffer() {
      List<EntryType> returnValue = this.mutationEntries;
      this.mutationEntries = new ArrayList<>();
      this.mutationsBufferSizeBytes = 0;
      return returnValue;
    }
  }
}
