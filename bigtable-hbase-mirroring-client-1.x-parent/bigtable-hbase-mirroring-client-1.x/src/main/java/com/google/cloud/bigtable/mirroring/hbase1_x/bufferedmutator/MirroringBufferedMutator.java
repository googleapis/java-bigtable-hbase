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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
   * Size that mutations kept in {@link #mutationEntries} should reach to invoke a asynchronous
   * flush() on the primary database.
   */
  protected final long mutationsBufferFlushIntervalBytes;

  /**
   * Internal buffer that should keep mutations that were not yet flushed asynchronously. Type of
   * the entry is specified by subclasses and can contain more elements than just mutations, e.g.
   * related resource reservations.
   */
  private List<BufferEntryType> mutationEntries;

  /** ExceptionListener supplied by the user. */
  protected final ExceptionListener userListener;

  protected long mutationsBufferSizeBytes;

  private boolean closed = false;

  public MirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      ExecutorService executorService,
      MirroringTracer mirroringTracer)
      throws IOException {
    this.userListener = bufferedMutatorParams.getListener();

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
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
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
    this.mutationsBufferFlushIntervalBytes =
        configuration.mirroringOptions.bufferedMutatorBytesToFlush;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.configuration = configuration;
    this.bufferedMutatorParams = bufferedMutatorParams;

    this.mutationEntries = new ArrayList<>();
    this.mirroringTracer = mirroringTracer;
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

  abstract void handleSecondaryException(RetriesExhaustedWithDetailsException e)
      throws RetriesExhaustedWithDetailsException;

  protected final synchronized void storeResourcesAndFlushIfNeeded(
      BufferEntryType entry, RequestResourcesDescription resourcesDescription) {
    this.mutationEntries.add(entry);
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

  @Override
  public final synchronized void close() throws IOException {
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

  protected final synchronized FlushFutures scheduleFlush() {
    try (Scope scope = this.mirroringTracer.spanFactory.scheduleFlushScope()) {
      this.mutationsBufferSizeBytes = 0;

      final List<BufferEntryType> dataToFlush = this.mutationEntries;
      this.mutationEntries = new ArrayList<>();
      return scheduleFlushScoped(dataToFlush);
    }
  }

  protected abstract FlushFutures scheduleFlushScoped(List<BufferEntryType> dataToFlush);

  protected final ListenableFuture<Void> schedulePrimaryFlush() {
    return this.executorService.submit(
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                mirroringTracer.spanFactory.wrapPrimaryOperation(
                    new CallableThrowingIOException<Void>() {
                      @Override
                      public Void call() throws IOException {
                        primaryBufferedMutator.flush();
                        return null;
                      }
                    },
                    HBaseOperation.BUFFERED_MUTATOR_FLUSH);
                return null;
              }
            }));
  }

  protected final void setInterruptedFlagInInterruptedException(Exception e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }
}
