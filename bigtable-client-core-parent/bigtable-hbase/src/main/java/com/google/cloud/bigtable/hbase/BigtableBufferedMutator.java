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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.util.concurrent.MoreExecutors;
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

/**
 * Bigtable's {@link org.apache.hadoop.hbase.client.BufferedMutator} implementation.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
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
   * Constructor for BigtableBufferedMutator.
   *
   * @param bigtableApi a {@link BigtableApi} object to access bigtable data client.
   * @param settings a {@link BigtableHBaseSettings} object for bigtable settings.
   * @param adapter a {@link HBaseRequestAdapter} object to convert HBase object to Bigtable protos.
   * @param listener Handles exceptions. By default, it just throws the exception.
   */
  public BigtableBufferedMutator(
      BigtableApi bigtableApi,
      BigtableHBaseSettings settings,
      HBaseRequestAdapter adapter,
      BufferedMutator.ExceptionListener listener) {
    helper = new BigtableBufferedMutatorHelper(bigtableApi, settings, adapter);
    this.listener = listener;
    this.host = settings.getDataHost();
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
    List<ApiFuture<?>> futures = helper.mutate(mutations);
    for (int i = 0; i < mutations.size(); i++) {
      addCallback(futures.get(i), mutations.get(i));
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Being a Mutation. This method will block if either of the following are true: 1) There are
   * more than {@code maxInflightRpcs} RPCs in flight 2) There are more than {@link
   * #getWriteBufferSize()} bytes pending
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
  private void addCallback(ApiFuture<?> future, Mutation mutation) {
    ApiFutures.addCallback(future, new ExceptionCallback(mutation), MoreExecutors.directExecutor());
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
  private class ExceptionCallback implements ApiFutureCallback {
    private final Row mutation;

    ExceptionCallback(Row mutation) {
      this.mutation = mutation;
    }

    @Override
    public void onFailure(Throwable t) {
      addGlobalException(mutation, t);
    }

    @Override
    public void onSuccess(Object ignored) {}
  }

  private void addGlobalException(Row mutation, Throwable t) {
    synchronized (globalExceptions) {
      globalExceptions.add(new MutationException(mutation, t));
      hasExceptions.set(true);
    }
  }
}
