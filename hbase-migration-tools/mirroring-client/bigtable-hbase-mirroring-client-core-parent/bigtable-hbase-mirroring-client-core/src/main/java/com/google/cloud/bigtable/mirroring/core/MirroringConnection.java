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
package com.google.cloud.bigtable.mirroring.core;

import com.google.cloud.bigtable.mirroring.core.bufferedmutator.MirroringBufferedMutator;
import com.google.cloud.bigtable.mirroring.core.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.core.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.core.utils.faillog.FailedMutationLogger;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static final com.google.cloud.bigtable.mirroring.core.utils.Logger Log =
      new com.google.cloud.bigtable.mirroring.core.utils.Logger(MirroringConnection.class);
  protected final FlowController flowController;
  protected final ExecutorService executorService;
  protected final MismatchDetector mismatchDetector;
  /**
   * Counter of all asynchronous operations that are using the secondary connection. Incremented
   * when scheduling operations by underlying {@link MirroringTable} and {@link
   * MirroringResultScanner}.
   */
  protected final ListenableReferenceCounter referenceCounter;

  protected final MirroringTracer mirroringTracer;
  protected final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;
  protected final ReadSampler readSampler;
  private final FailedMutationLogger failedMutationLogger;
  protected final MirroringConfiguration configuration;
  private final Connection primaryConnection;
  private final Connection secondaryConnection;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean aborted = new AtomicBoolean(false);

  /**
   * Enables concurrent writes mode. Should always be enabled together with {@link
   * #waitForSecondaryWrites}.
   *
   * <p>In this mode some of the writes ({@link org.apache.hadoop.hbase.client.Put}s, {@link
   * org.apache.hadoop.hbase.client.Delete}s and {@link
   * org.apache.hadoop.hbase.client.RowMutations}) performed using {@link
   * org.apache.hadoop.hbase.client.Table} API will be performed concurrently and {@link
   * com.google.cloud.bigtable.mirroring.core.bufferedmutator.ConcurrentMirroringBufferedMutator}
   * instances will be returned by {@link
   * org.apache.hadoop.hbase.client.Connection#getBufferedMutator}. Moreover, all operations in this
   * mode wait for secondary database operation to finish before returning to the user (because
   * {@link #waitForSecondaryWrites} should be set).
   */
  protected final boolean performWritesConcurrently;

  protected final boolean waitForSecondaryWrites;
  // Depending on configuration, ensures that mutations have an explicitly set timestamp.
  protected final Timestamper timestamper;

  /**
   * The constructor called from {@link
   * org.apache.hadoop.hbase.client.ConnectionFactory#createConnection(Configuration)} and in its
   * many forms via reflection with this specific signature.
   *
   * <p>Parameters are passed down to ConnectionFactory#createConnection method, connection errors
   * are passed back to the user.
   */
  public MirroringConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws Throwable {
    this(new MirroringConfiguration(conf), pool, user);
    // This is an always-false legacy hbase parameter.
    Preconditions.checkArgument(!managed, "Mirroring client doesn't support managed connections.");
  }

  public MirroringConnection(MirroringConfiguration mirroringConfiguration, ExecutorService pool)
      throws IOException {
    this(
        mirroringConfiguration,
        pool,
        ConnectionFactory.createConnection(mirroringConfiguration.primaryConfiguration, pool),
        ConnectionFactory.createConnection(mirroringConfiguration.secondaryConfiguration, pool));
  }

  private MirroringConnection(MirroringConfiguration conf, ExecutorService pool, User user)
      throws IOException {
    this(
        conf,
        pool,
        ConnectionFactory.createConnection(conf.primaryConfiguration, pool, user),
        ConnectionFactory.createConnection(conf.secondaryConfiguration, pool, user));
  }

  private MirroringConnection(
      MirroringConfiguration conf,
      ExecutorService pool,
      Connection primaryConnection,
      Connection secondaryConnection) {
    this.configuration = conf;
    this.mirroringTracer = new MirroringTracer();

    this.primaryConnection = primaryConnection;
    this.secondaryConnection = secondaryConnection;

    if (pool == null) {
      this.executorService = Executors.newCachedThreadPool();
    } else {
      this.executorService = pool;
    }

    referenceCounter = new ListenableReferenceCounter();

    try {
      this.flowController =
          new FlowController(
              this.configuration
                  .mirroringOptions
                  .flowControllerStrategyFactoryClass
                  .newInstance()
                  .create(this.configuration.mirroringOptions));
      this.mismatchDetector =
          this.configuration
              .mirroringOptions
              .mismatchDetectorFactoryClass
              .newInstance()
              .create(
                  this.mirroringTracer, configuration.mirroringOptions.maxLoggedBinaryValueLength);

      this.failedMutationLogger =
          new FailedMutationLogger(
              this.configuration
                  .mirroringOptions
                  .faillog
                  .writeErrorLogAppenderFactoryClass
                  .newInstance()
                  .create(this.configuration.mirroringOptions.faillog),
              this.configuration
                  .mirroringOptions
                  .faillog
                  .writeErrorLogSerializerFactoryClass
                  .newInstance()
                  .create());

      final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer =
          this.configuration
              .mirroringOptions
              .writeErrorConsumerFactoryClass
              .newInstance()
              .create(this.failedMutationLogger);

      this.timestamper =
          Timestamper.create(this.configuration.mirroringOptions.enableDefaultClientSideTimestamps);

      this.secondaryWriteErrorConsumer =
          new SecondaryWriteErrorConsumerWithMetrics(
              this.mirroringTracer, secondaryWriteErrorConsumer);
      this.readSampler = new ReadSampler(this.configuration.mirroringOptions.readSamplingRate);
      this.performWritesConcurrently =
          this.configuration.mirroringOptions.performWritesConcurrently;
      this.waitForSecondaryWrites = this.configuration.mirroringOptions.waitForSecondaryWrites;
    } catch (Throwable throwable) {
      // Throwable are thrown by `newInstance` and `create` methods.
      throw new RuntimeException(throwable);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration.baseConfiguration;
  }

  @Override
  public String getClusterId() {
    throw new UnsupportedOperationException("not implemented");
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
      return new MirroringTable(
          primaryTable,
          secondaryTable,
          executorService,
          this.mismatchDetector,
          this.flowController,
          this.secondaryWriteErrorConsumer,
          this.readSampler,
          this.timestamper,
          this.performWritesConcurrently,
          this.waitForSecondaryWrites,
          this.mirroringTracer,
          this.referenceCounter,
          this.configuration.mirroringOptions.maxLoggedBinaryValueLength);
    }
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams)
      throws IOException {
    return MirroringBufferedMutator.create(
        performWritesConcurrently,
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        configuration,
        flowController,
        executorService,
        secondaryWriteErrorConsumer,
        referenceCounter,
        timestamper,
        mirroringTracer);
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return this.primaryConnection.getRegionLocator(tableName);
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

      final AccumulatedExceptions exceptions = new AccumulatedExceptions();
      try {
        primaryConnection.close();
      } catch (IOException e) {
        exceptions.add(e);
      }

      CallableThrowingIOException<Void> closeSecondaryConnection =
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() {
              try {
                secondaryConnection.close();
              } catch (IOException e) {
                exceptions.add(e);
              }
              return null;
            }
          };

      try {
        terminateSecondaryConnectionWithTimeout(closeSecondaryConnection);
      } catch (IOException e) {
        exceptions.add(e);
      }

      // Close the failed mutation logger, which will close all the
      // contained resources (file streams and buffers).
      try {
        failedMutationLogger.close();
      } catch (Exception e) {
        Log.error("Failed to close failedMutationLogger.", e);
      }

      exceptions.rethrowIfCaptured();
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void abort(final String s, final Throwable throwable) {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            HBaseOperation.MIRRORING_CONNECTION_ABORT)) {
      if (this.aborted.getAndSet(true)) {
        return;
      }

      primaryConnection.abort(s, throwable);

      final CallableThrowingIOException<Void> abortSecondaryConnection =
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() {
              secondaryConnection.abort(s, throwable);
              return null;
            }
          };
      try {
        terminateSecondaryConnectionWithTimeout(abortSecondaryConnection);
      } catch (IOException e) {
        if (e.getCause() instanceof InterruptedException) {
          throw new RuntimeException(e.getCause());
        }
      }
    }
  }

  @Override
  public boolean isAborted() {
    return this.aborted.get();
  }

  @VisibleForTesting
  public Connection getPrimaryConnection() {
    return this.primaryConnection;
  }

  @VisibleForTesting
  public Connection getSecondaryConnection() {
    return this.secondaryConnection;
  }

  private void terminateSecondaryConnectionWithTimeout(
      final CallableThrowingIOException<Void> terminatingAction) throws IOException {
    final SettableFuture<Void> terminationFinishedFuture = SettableFuture.create();

    // The secondary termination action should be run after all in-flight requests are finished.
    this.referenceCounter
        .getOnLastReferenceClosed()
        .addListener(
            new Runnable() {
              @Override
              public void run() {
                try {
                  terminatingAction.call();
                  terminationFinishedFuture.set(null);
                } catch (Throwable e) {
                  terminationFinishedFuture.setException(e);
                }
              }
            },
            MoreExecutors.directExecutor());

    this.referenceCounter.decrementReferenceCount();
    try {
      // Wait for in-flight requests to be finished but with a timeout to prevent deadlock.
      terminationFinishedFuture.get(
          this.configuration.mirroringOptions.connectionTerminationTimeoutMillis,
          TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      // If the secondary terminating action has thrown while we were waiting, the error will be
      // propagated to the user.
      throw new IOException(e);
    } catch (TimeoutException e) {
      // But if the timeout was reached, we just leave the operation pending.
      Log.error(
          "MirroringConnection#close() timed out. Some of operations on secondary "
              + "database are still in-flight and might be lost, but are not cancelled and "
              + "will be performed asynchronously until the program terminates.");
      // This error is not reported to the user.
    }
  }
}
