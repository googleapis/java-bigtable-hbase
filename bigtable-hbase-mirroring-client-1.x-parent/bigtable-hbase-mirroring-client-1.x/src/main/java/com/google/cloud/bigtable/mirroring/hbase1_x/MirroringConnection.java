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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.reflection.ReflectionConstructor;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;

public class MirroringConnection implements Connection {

  private static final Logger Log = new Logger(MirroringConnection.class);
  private final FlowController flowController;
  private final ExecutorService executorService;
  private final MismatchDetector mismatchDetector;
  private final ListenableReferenceCounter referenceCounter;
  private final MirroringTracer mirroringTracer;
  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;
  private final ReadSampler readSampler;
  private final MirroringConfiguration configuration;
  private final Connection primaryConnection;
  private final Connection secondaryConnection;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean aborted = new AtomicBoolean(false);

  /**
   * The constructor called from {@link
   * org.apache.hadoop.hbase.client.ConnectionFactory#createConnection(Configuration)} and in its
   * many forms via reflection with this specific signature.
   *
   * <p>Parameters are passed down to ConnectionFactory#createConnection method, connection errors
   * are passed back to the user.
   */
  public MirroringConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    assert !managed; // This is always-false legacy hbase parameter.
    this.configuration = new MirroringConfiguration(conf);
    this.mirroringTracer = new MirroringTracer();

    this.primaryConnection =
        ConnectionFactory.createConnection(this.configuration.primaryConfiguration, pool, user);
    this.secondaryConnection =
        ConnectionFactory.createConnection(this.configuration.secondaryConfiguration, pool, user);

    if (pool == null) {
      this.executorService = Executors.newCachedThreadPool();
    } else {
      this.executorService = pool;
    }

    referenceCounter = new ListenableReferenceCounter();
    this.flowController =
        new FlowController(
            ReflectionConstructor.<FlowControlStrategy>construct(
                this.configuration.mirroringOptions.flowControllerStrategyClass,
                this.configuration.mirroringOptions));
    this.mismatchDetector =
        ReflectionConstructor.construct(
            this.configuration.mirroringOptions.mismatchDetectorClass, this.mirroringTracer);

    final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer =
        ReflectionConstructor.construct(
            this.configuration.mirroringOptions.writeErrorConsumerClass);

    this.secondaryWriteErrorConsumer =
        new SecondaryWriteErrorConsumerWithMetrics(
            this.mirroringTracer, secondaryWriteErrorConsumer);
    this.readSampler = new ReadSampler(this.configuration.mirroringOptions.readSamplingRate);
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    Log.trace("getTable(%s)", tableName);
    return this.getTable(tableName, this.executorService);
  }

  @Override
  public Table getTable(final TableName tableName, ExecutorService executorService)
      throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.GET_TABLE)) {
      Log.trace("getTable(%s, executorService)", tableName);
      Table primaryTable =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Table>() {
                @Override
                public Table call() throws IOException {
                  return MirroringConnection.this.primaryConnection.getTable(tableName);
                }
              },
              HBaseOperation.GET_TABLE);
      Table secondaryTable = this.secondaryConnection.getTable(tableName);
      MirroringTable table =
          new MirroringTable(
              primaryTable,
              secondaryTable,
              executorService,
              this.mismatchDetector,
              this.flowController,
              this.secondaryWriteErrorConsumer,
              this.readSampler,
              this.mirroringTracer);
      this.referenceCounter.holdReferenceUntilClosing(table);
      return table;
    }
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams)
      throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.GET_BUFFERED_MUTATOR)) {
      return new MirroringBufferedMutator(
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

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Admin getAdmin() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            HBaseOperation.MIRRORING_CONNECTION_CLOSE)) {
      if (this.closed.getAndSet(true)) {
        return;
      }

      try {
        closeMirroringConnectionAndWaitForAsyncOperations();
      } catch (InterruptedException e) {
        IOException wrapperException = new InterruptedIOException();
        wrapperException.initCause(e);
        throw wrapperException;
      }

      AccumulatedExceptions exceptions = new AccumulatedExceptions();
      try {
        this.primaryConnection.close();
      } catch (IOException e) {
        exceptions.add(e);
      }

      try {
        this.secondaryConnection.close();
      } catch (IOException e) {
        exceptions.add(e);
      }

      exceptions.rethrowIfCaptured();
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void abort(String s, Throwable throwable) {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            HBaseOperation.MIRRORING_CONNECTION_ABORT)) {
      if (this.aborted.getAndSet(true)) {
        return;
      }

      try {
        closeMirroringConnectionAndWaitForAsyncOperations();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      this.primaryConnection.abort(s, throwable);
      this.secondaryConnection.abort(s, throwable);
    }
  }

  @Override
  public boolean isAborted() {
    return this.aborted.get();
  }

  public Connection getPrimaryConnection() {
    return this.primaryConnection;
  }

  public Connection getSecondaryConnection() {
    return this.secondaryConnection;
  }

  private void closeMirroringConnectionAndWaitForAsyncOperations() throws InterruptedException {
    this.referenceCounter.decrementReferenceCount();
    try {
      this.referenceCounter.getOnLastReferenceClosed().get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
