/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Bigtable's {@link org.apache.hadoop.hbase.client.BufferedMutator} implementation.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableBufferedMutator implements BufferedMutator {
  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  private static class MutationException {
    private final Row mutation;
    private final Throwable throwable;

    MutationException(Row mutation, Throwable throwable) {
      this.mutation = mutation;
      this.throwable = throwable;
    }
  }

  private final BigtableBufferedMutatorHelper helper;
  private final ExceptionListener listener;

  private final AtomicBoolean hasExceptions = new AtomicBoolean(false);
  private final List<MutationException> globalExceptions = new ArrayList<MutationException>();
  private final String host;

  /**
   * <p>Constructor for BigtableBufferedMutator.</p>
   *
   * @param adapter Converts HBase objects to Bigtable protos
   * @param configuration For Additional configuration. TODO: move this to options
   * @param listener Handles exceptions. By default, it just throws the exception.
   * @param session a {@link com.google.cloud.bigtable.grpc.BigtableSession} to get {@link com.google.cloud.bigtable.config.BigtableOptions}, {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor}
   * and {@link com.google.cloud.bigtable.grpc.async.BulkMutation} objects from
   * starting the async operations on the BigtableDataClient.
   */
  public BigtableBufferedMutator(
      HBaseRequestAdapter adapter,
      Configuration configuration,
      BigtableSession session,
      BufferedMutator.ExceptionListener listener) {
    helper = new BigtableBufferedMutatorHelper(adapter, configuration, session);
    this.listener = listener;
    this.host = session.getOptions().build().getDataHost().toString();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    helper.close();
    handleExceptions();
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    helper.flush();
    handleExceptions();
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return helper.getConfiguration();
  }

  /** {@inheritDoc} */
  @Override
  public TableName getName() {
    return helper.getName();
  }

  /** {@inheritDoc} */
  @Override
  public long getWriteBufferSize() {
    return helper.getWriteBufferSize();
  }

  /** {@inheritDoc} */
  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    handleExceptions();
    List<ListenableFuture<?>> futures = helper.mutate(mutations);
    for (int i = 0; i < mutations.size(); i++) {
      addCallback(futures.get(i), mutations.get(i));
    }
  }

  /**
   * {@inheritDoc}
   *
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
   */
  @Override
  public void mutate(Mutation mutation) throws IOException {
    handleExceptions();
    addCallback(helper.mutate(mutation), mutation);
  }

  /**
   * Create a {@link RetriesExhaustedWithDetailsException} if there were any async exceptions and
   * send it to the {@link org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener}.
   */
  private void handleExceptions() throws RetriesExhaustedWithDetailsException {
    RetriesExhaustedWithDetailsException exceptions = getExceptions();
    if (exceptions != null) {
      listener.onException(exceptions, this);
    }
  }

  @SuppressWarnings("unchecked")
  private void addCallback(ListenableFuture<?> future, Mutation mutation) {
    Futures.addCallback(future, new ExceptionCallback(mutation), MoreExecutors.directExecutor());
  }

  /**
   * <p>hasInflightRequests.</p>
   *
   * @return a boolean.
   */
  public boolean hasInflightRequests() {
    return helper.hasInflightRequests();
  }
  
  /**
   * Create a {@link RetriesExhaustedWithDetailsException} if there were any async exceptions and
   * send it to the {@link org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener}.
   */
  private RetriesExhaustedWithDetailsException getExceptions()
      throws RetriesExhaustedWithDetailsException {
    if (!hasExceptions.get()) {
      return null;
    }

    ArrayList<MutationException> mutationExceptions = null;
    synchronized (globalExceptions) {
      hasExceptions.set(false);
      if (globalExceptions.isEmpty()) {
        return null;
      }

      mutationExceptions = new ArrayList<>(globalExceptions);
      globalExceptions.clear();
    }

    List<Throwable> problems = new ArrayList<>(mutationExceptions.size());
    ArrayList<String> hostnames = new ArrayList<>(mutationExceptions.size());
    List<Row> failedMutations = new ArrayList<>(mutationExceptions.size());

    if (!mutationExceptions.isEmpty()) {
      LOG.warn("Exception occurred in BufferedMutator", mutationExceptions.get(0).throwable);
    }
    for (MutationException mutationException : mutationExceptions) {
      problems.add(mutationException.throwable);
      failedMutations.add(mutationException.mutation);
      hostnames.add(host);
      LOG.debug("Exception occurred in BufferedMutator", mutationException.throwable);
    }

    return new RetriesExhaustedWithDetailsException(problems, failedMutations, hostnames);
  }

  @SuppressWarnings("rawtypes")
  private class ExceptionCallback implements FutureCallback {
    private final Row mutation;

    public ExceptionCallback(Row mutation) {
      this.mutation = mutation;
    }

    @Override
    public void onFailure(Throwable t) {
      addGlobalException(mutation, t);
    }

    @Override
    public void onSuccess(Object ignored) {
    }
  }

  private void addGlobalException(Row mutation, Throwable t) {
    synchronized (globalExceptions) {
      globalExceptions.add(new MutationException(mutation, t));
      hasExceptions.set(true);
    }
  }
}
