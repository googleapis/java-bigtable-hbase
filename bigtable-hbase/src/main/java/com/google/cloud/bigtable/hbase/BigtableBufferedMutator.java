/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase.adapters.AppendAdapter;
import com.google.cloud.bigtable.hbase.adapters.BigtableRowAdapter;
import com.google.cloud.bigtable.hbase.adapters.DeleteAdapter;
import com.google.cloud.bigtable.hbase.adapters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.IncrementAdapter;
import com.google.cloud.bigtable.hbase.adapters.MutationAdapter;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;
import com.google.cloud.bigtable.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.bigtable.hbase.adapters.ScanAdapter;
import com.google.cloud.bigtable.hbase.adapters.TableMetadataSetter;
import com.google.cloud.bigtable.hbase.adapters.UnsupportedOperationAdapter;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.GeneratedMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BigtableBufferedMutator implements BufferedMutator {

  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  // Flush is not properly synchronized with respect to waiting. It will never exit
  // improperly, but it might wait more than it has to. Setting this to a low value ensures
  // that improper waiting is minimal.
  private static final long MAX_WAIT_MILLIS = 250;

  // Wait up to MAX_WAIT_COUNT * MAX_WAIT_MILLIS to add another event;
  private static final int MAX_WAIT_COUNT = 8;

  public enum MutationEventType {
    TO_SAVE,
    SAVED,
    ERROR,
  }

  public static class MutationEvent {
    private final long id;
    private final Mutation mutation;
    private final long heap;
    private MutationEventType type;
    private Throwable exception;
    private int retryCount = 0;

    public MutationEvent(long id, Mutation mutation) {
      this.id = id;
      this.mutation = mutation;
      this.type = MutationEventType.TO_SAVE;
      this.heap = mutation.heapSize();
    }

    public void setType(MutationEventType type) {
      this.type = type;
    }

    public void setException(Throwable exception) {
      this.type = MutationEventType.ERROR;
      this.exception = exception;
    }

    public long getHeap() {
      return heap;
    }

    public Mutation getMutation() {
      return mutation;
    }

    public Throwable getException() {
      return exception;
    }

    public MutationEventType getType() {
      return type;
    }

    public long getId() {
      return id;
    }

    public int incrementRetryCount() {
      return ++retryCount;
    }
  }

  private static class AccountingFutureCallback implements FutureCallback<GeneratedMessage> {
    private final MutationEvent event;
    private final EventHandler handler;

    public AccountingFutureCallback(EventHandler handler, MutationEvent event) {
      this.handler = handler;
      this.event = event;
    }

    @Override
    public void onFailure(Throwable t) {
      handler.failure(event, t);
    }

    @Override
    public void onSuccess(GeneratedMessage ignored) {
      handler.success(event);
    }
  }

  /**
   * This class ensures that operations are
   */
  private static class EventHandler implements Runnable {
    private final long maxHeapSize;
    private final int maxInFlightRpcs;
    private final AtomicInteger processedCount = new AtomicInteger();

    private final BatchExecutor batchExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final BlockingQueue<MutationEvent> processingQueue = new LinkedBlockingQueue<>();

    private final AtomicInteger currentInFlight = new AtomicInteger(0);
    private final AtomicInteger waiting = new AtomicInteger(0);
    private final AtomicLong currentWriteBufferSize = new AtomicLong();

    public EventHandler(long maxHeapSize, int maxInflightRpcs, BatchExecutor batchExecutor) {
      this.maxHeapSize = maxHeapSize;
      this.maxInFlightRpcs = maxInflightRpcs;
      this.batchExecutor = batchExecutor;
    }

    public long getMaxHeapSize() {
      return maxHeapSize;
    }

    @Override
    public void run() {
      while (!closed.get() || waiting.get() > 0 || processingQueue.size() > 0) {
        MutationEvent event = null;
        try {
          event = processingQueue.poll(MAX_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
        if (event != null) {
          AccountingFutureCallback callback = new AccountingFutureCallback(this, event);
          try {
            Futures.addCallback(batchExecutor.issueRequest(event.getMutation()), callback);
          } catch (Exception e) {
            int retryCount = event.incrementRetryCount();
            if (retryCount < 5) {
              LOG.warn("Could not process event %d.  Re-adding to the queue for attempt #%d", e,
                  event.getId(), retryCount);
              processingQueue.add(event);
            } else {
              failure(event, e);
            }
          }
        }
      }
    }

    public void add(MutationEvent event) {
      LOG.trace("trying to add event %d", event.getId());
      if (closed.get()) {
        throw new IllegalStateException(
            "Cannot process a mutation once the BufferedMutator is closed");
      }
      int waitCount = 0;
      waiting.incrementAndGet();
      while (waitCount < MAX_WAIT_COUNT) {
        if (canAdd()) {
          waiting.decrementAndGet();
          processingQueue.add(event);
          currentInFlight.incrementAndGet();
          currentWriteBufferSize.addAndGet(event.getHeap());
          return;
        } else {
          synchronized (currentInFlight) {
            try {
              if (!canAdd()) {
                currentInFlight.wait(MAX_WAIT_MILLIS);
                waitCount++;
              }
            } catch (InterruptedException e) {
            }
          }
        }
      }
      waiting.decrementAndGet();
      throw new IllegalStateException(String.format("Could not process mutation %d with key %s",
          event.getId(), Bytes.toString(event.getMutation().getRow())));
    }

    private boolean canAdd(){
      long writeBufferSize = currentWriteBufferSize.get();
      int inFlight = currentInFlight.get();
      return writeBufferSize < maxHeapSize && inFlight < maxInFlightRpcs;
    }


    public void success(MutationEvent event) {
      event.setType(MutationEventType.SAVED);
      finish(event);
    }

    public void failure(MutationEvent event, Throwable t) {
      event.setType(MutationEventType.ERROR);
      event.setException(t);
      finish(event);
    }

    /**
     * @param event
     */
    private void finish(MutationEvent event) {
      currentWriteBufferSize.addAndGet(-event.getHeap());
      currentInFlight.decrementAndGet();
      synchronized (currentInFlight) {
        currentInFlight.notifyAll();
      }
      processedCount.incrementAndGet();
    }

    public void flush() {
      while (true) {
        synchronized (currentInFlight) {
          if (currentInFlight.get() > 0 || waiting.get() > 0) {
            try {
              currentInFlight.wait(MAX_WAIT_MILLIS);
            } catch (InterruptedException e) {
            }
          } else {
            break;
          }
        }
      }
      LOG.info(" DONE FLUSH: in flight %d, waiting %d, processed %d", currentInFlight.get(),
          waiting.get(), processedCount.get());
    }

    public void close(){
      closed.set(true);
      flush();
    }

    public boolean isClosed() {
      return closed.get();
    }
  }

  private final Configuration configuration;
  private final TableName tableName;
  private final EventHandler eventHandler;
  private final AtomicInteger sequence = new AtomicInteger();
  private final BatchExecutor batchExecutor;
  private final ExceptionListener exceptionListener;

  private final List<MutationEvent> exceptions = new ArrayList<>();

  public BigtableBufferedMutator(
      Configuration configuration,
      TableName tableName,
      int maxInflightRpcs,
      long maxHeapSize,
      BigtableClient client,
      BigtableOptions options,
      ExecutorService executorService,
      BufferedMutator.ExceptionListener listener) {
    this.configuration = configuration;
    this.tableName = tableName;
    this.exceptionListener = listener;
    DeleteAdapter deleteAdapter = new DeleteAdapter();
    PutAdapter putAdapter = new PutAdapter(configuration);

    RowMutationsAdapter rowMutationsAdapter =
        new RowMutationsAdapter(
            new MutationAdapter(
                deleteAdapter,
                putAdapter,
                new UnsupportedOperationAdapter<Increment>("increment"),
                new UnsupportedOperationAdapter<Append>("append")));

    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(executorService);

    batchExecutor = new BatchExecutor(
        client,
        options,
        TableMetadataSetter.from(tableName, options),
        listeningExecutorService,
        new GetAdapter(new ScanAdapter(new FilterAdapter())),
        putAdapter,
        deleteAdapter,
        rowMutationsAdapter,
        new AppendAdapter(),
        new IncrementAdapter(),
        new BigtableRowAdapter());

    this.eventHandler = new EventHandler(maxHeapSize, maxInflightRpcs, batchExecutor);

    for (int i=0; i<4; i++) {
      executorService.execute(eventHandler);
    }
  }

  @Override
  public void close() throws IOException {
    if (!eventHandler.isClosed()) {
      eventHandler.close();
    }
  }

  @Override
  public void flush() throws IOException {
    if (eventHandler.isClosed()) {
      throw new IllegalStateException("Cannot flush when the BufferedMutator is closed.");
    }
    eventHandler.flush();
    handleExceptions();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public long getWriteBufferSize() {
    return this.eventHandler.getMaxHeapSize();
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    for (Mutation mutation : mutations) {
      mutate(mutation);
    }
  }

  /**
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
   */
  @Override
  public void mutate(final Mutation mutation) throws IOException {
    if (eventHandler.isClosed()) {
      throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
    }
    handleExceptions();
    eventHandler.add(new MutationEvent(sequence.incrementAndGet(), mutation));
  }

  /**
   * Create a {@link RetriesExhaustedWithDetailsException} if there were any async exceptions and
   * send it to the {@link org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener}.
   */
  private void handleExceptions() {
    if(!exceptions.isEmpty()){
      ArrayList<MutationEvent> mutationExceptions = null;
      synchronized (exceptions) {
        mutationExceptions = new ArrayList<>(exceptions);
        exceptions.clear();
      }

      List<Throwable> problems = new ArrayList<>(mutationExceptions.size());
      ArrayList<String> hostnames = new ArrayList<>(mutationExceptions.size());
      List<Row> failedMutations = new ArrayList<>(mutationExceptions.size());

      for (MutationEvent event : mutationExceptions) {
        problems.add(event.getException());
        failedMutations.add(event.getMutation());
        hostnames.add(null);
      }

      try {
        RetriesExhaustedWithDetailsException exception = new RetriesExhaustedWithDetailsException(
            problems, failedMutations, hostnames);
        exceptionListener.onException(exception, this);
      } catch (RetriesExhaustedWithDetailsException e) {
        for (Throwable throwable : e.getCauses()) {
          throwable.printStackTrace();
        }
        LOG.error("ExceptionListener threw exception", e);
      }
    }
  }
}