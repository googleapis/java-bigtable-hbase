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
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

/**
 * Bigtable's {@link BufferedMutator} implementation.
 */
// TODO: Cleanup the interface so that @VisibleForTesting can be reduced.
public class BigtableBufferedMutator implements BufferedMutator {

  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  @VisibleForTesting
  static class MutationException {
    private final Row mutation;
    private final Throwable throwable;

    MutationException(Row mutation, Throwable throwable) {
      this.mutation = mutation;
      this.throwable = throwable;
    }
  }

  private final Configuration configuration;
  private final TableName tableName;

  private final ExceptionListener exceptionListener;
  private final BigtableAsyncExecutor asyncExecutor;

  @VisibleForTesting
  final AtomicBoolean hasExceptions = new AtomicBoolean(false);

  @VisibleForTesting
  final List<MutationException> globalExceptions = new ArrayList<MutationException>();

  private final String host;
  private final PutAdapter putAdapter;
  private final BigtableTableName bigtableTableName;

  public BigtableBufferedMutator(
      BigtableAsyncExecutor asyncExecutor,
      BufferedMutator.ExceptionListener listener,
      Configuration configuration,
      TableName tableName,
      BigtableOptions options) {
    this.configuration = configuration;
    this.tableName = tableName;
    this.exceptionListener = listener;

    this.host = options.getDataHost().toString();

    this.bigtableTableName = options.getClusterName().toTableName(tableName.getQualifierAsString());
    this.putAdapter = Adapters.createPutAdapter(configuration);
    this.asyncExecutor = asyncExecutor;
  }

  @Override
  public void close() throws IOException {
    asyncExecutor.close();
  }

  @Override
  public void flush() throws IOException {
    asyncExecutor.flush();
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
    return this.asyncExecutor.getWriteBufferSize();
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    // Ensure that close() or flush() aren't current being called.
    handleExceptions();
    for (Mutation mutation : mutations) {
      doMutate(mutation);
    }
  }


  /**
   * Being a Mutation. This method will block if either of the following are true:
   * 1) There are more than {@code maxInflightRpcs} RPCs in flight
   * 2) There are more than {@link #getWriteBufferSize()} bytes pending
   */
  @Override
  public void mutate(Mutation mutation) throws IOException {
    doMutate(mutation);
  }

  private void doMutate(Mutation mutation) throws RetriesExhaustedWithDetailsException, IOException {
    ListenableFuture<?> future = getFuture(mutation);
    if (future != null) {
      Futures.addCallback(future, createErrorStreamObserver(mutation));
    }
  }

  private ListenableFuture<?> getFuture(final Mutation mutation)
      throws RetriesExhaustedWithDetailsException, IOException {
    handleExceptions();
    try {
      if (mutation instanceof Put) {
        return mutate(this.putAdapter.adapt((Put) mutation));
      } else if (mutation instanceof Delete) {
        return mutate(Adapters.DELETE_ADAPTER.adapt((Delete) mutation));
      } else if (mutation instanceof Append) {
        return readModifyWrite(Adapters.APPEND_ADAPTER.adapt((Append) mutation));
      } else if (mutation instanceof Increment) {
        return readModifyWrite(
          Adapters.INCREMENT_ADAPTER.adapt((Increment) mutation));
      }
    } catch (RuntimeException e) {
      addGlobalException(mutation, e);
      return null;
    }
    IllegalArgumentException throwable =
        new IllegalArgumentException("Encountered unknown action type: " + mutation.getClass());
    addGlobalException(mutation, throwable);
    return null;
  }

  private ListenableFuture<com.google.bigtable.v1.Row> readModifyWrite(
      ReadModifyWriteRowRequest.Builder request) throws IOException {
    request.setTableName(this.bigtableTableName.toString());
    return this.asyncExecutor.readModifyWriteRowAsync(request.build());
  }

  private ListenableFuture<Empty> mutate(MutateRowRequest.Builder request)
      throws IOException {
    request.setTableName(this.bigtableTableName.toString());
    return this.asyncExecutor.mutateRowAsync(request.build());
  }

  private <T> FutureCallback<T> createErrorStreamObserver(final Mutation mutation) {
    return new FutureCallback<T>() {

      @Override
      public void onSuccess(Object result) {
      }

      @Override
      public void onFailure(Throwable t) {
        addGlobalException(mutation, t);
      }
    };
  }

  private void addGlobalException(Row mutation, Throwable t) {
    synchronized (globalExceptions) {
      globalExceptions.add(new MutationException(mutation, t));
      hasExceptions.set(true);
    }
  }

  /**
   * Create a {@link RetriesExhaustedWithDetailsException} if there were any async exceptions and
   * send it to the {@link org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener}.
   */
  @VisibleForTesting
  void handleExceptions() throws RetriesExhaustedWithDetailsException {
    if (hasExceptions.get()) {
      ArrayList<MutationException> mutationExceptions = null;
      synchronized (globalExceptions) {
        hasExceptions.set(false);
        if (globalExceptions.isEmpty()) {
          return;
        }

        mutationExceptions = new ArrayList<>(globalExceptions);
        globalExceptions.clear();
      }

      List<Throwable> problems = new ArrayList<>(mutationExceptions.size());
      ArrayList<String> hostnames = new ArrayList<>(mutationExceptions.size());
      List<Row> failedMutations = new ArrayList<>(mutationExceptions.size());

      for (MutationException mutationException : mutationExceptions) {
        problems.add(mutationException.throwable);
        failedMutations.add(mutationException.mutation);
        hostnames.add(host);
      }

      RetriesExhaustedWithDetailsException exception = new RetriesExhaustedWithDetailsException(
          problems, failedMutations, hostnames);
      exceptionListener.onException(exception, this);
    }
  }

  public boolean hasInflightRequests() {
    return this.asyncExecutor.hasInflightRequests();
  }
}
