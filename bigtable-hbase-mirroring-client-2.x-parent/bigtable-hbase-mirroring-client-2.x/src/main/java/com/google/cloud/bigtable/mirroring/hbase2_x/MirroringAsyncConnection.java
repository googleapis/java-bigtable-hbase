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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.FailedMutationLogger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncAdminBuilder;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.AsyncBufferedMutatorBuilder;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.apache.hadoop.hbase.security.User;

public class MirroringAsyncConnection implements AsyncConnection {
  private static final Logger Log = new Logger(MirroringAsyncConnection.class);

  private final MirroringAsyncConfiguration configuration;
  private final AsyncConnection primaryConnection;
  private final AsyncConnection secondaryConnection;
  private final MismatchDetector mismatchDetector;
  private final ListenableReferenceCounter referenceCounter;
  private final FlowController flowController;
  private final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  private final MirroringTracer mirroringTracer;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ReadSampler readSampler;
  private final ExecutorService executorService;
  private final Timestamper timestamper;

  /**
   * The constructor called from {@link
   * org.apache.hadoop.hbase.client.ConnectionFactory#createAsyncConnection(Configuration)} and in
   * its many forms via reflection with this specific signature.
   *
   * <p>Parameters are passed down to ConnectionFactory#createAsyncConnection method, connection
   * errors are passed back to the user (wrapped in a CompletableFuture).
   */
  public MirroringAsyncConnection(
      Configuration conf,
      /**
       * The constructor is passed a ConnectionRegistry, which is a private interface in
       * org.apache.hadoop.hbase.client.
       */
      Object ignoredRegistry,
      String ignoredClusterId,
      User user)
      throws Throwable {
    this.configuration = new MirroringAsyncConfiguration(conf);

    this.mirroringTracer = new MirroringTracer();

    this.primaryConnection =
        ConnectionFactory.createAsyncConnection(this.configuration.primaryConfiguration, user)
            .get();
    this.secondaryConnection =
        ConnectionFactory.createAsyncConnection(this.configuration.secondaryConfiguration, user)
            .get();

    this.referenceCounter = new ListenableReferenceCounter();
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
                this.mirroringTracer,
                this.configuration.mirroringOptions.maxLoggedBinaryValueLength);

    FailedMutationLogger failedMutationLogger =
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

    SecondaryWriteErrorConsumer writeErrorConsumer =
        this.configuration
            .mirroringOptions
            .writeErrorConsumerFactoryClass
            .newInstance()
            .create(failedMutationLogger);

    this.secondaryWriteErrorConsumer =
        new SecondaryWriteErrorConsumerWithMetrics(this.mirroringTracer, writeErrorConsumer);

    this.readSampler = new ReadSampler(this.configuration.mirroringOptions.readSamplingRate);
    this.executorService = Executors.newCachedThreadPool();
    this.timestamper =
        Timestamper.create(this.configuration.mirroringOptions.enableDefaultClientSideTimestamps);
  }

  public AsyncConnection getPrimaryConnection() {
    return this.primaryConnection;
  }

  public AsyncConnection getSecondaryConnection() {
    return this.secondaryConnection;
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration.baseConfiguration;
  }

  @Override
  public boolean isClosed() {
    return this.closed.get();
  }

  @Override
  public void close() throws IOException {
    if (this.closed.getAndSet(true)) {
      return;
    }

    final AccumulatedExceptions exceptions = new AccumulatedExceptions();
    try {
      primaryConnection.close();
    } catch (IOException e) {
      exceptions.add(e);
    }

    CompletableFuture<Void> closingFinishedFuture = new CompletableFuture<>();

    // The secondary connection can only be closed after all in-flight requests are finished.
    this.referenceCounter
        .getOnLastReferenceClosed()
        .addListener(
            () -> {
              try {
                secondaryConnection.close();
                closingFinishedFuture.complete(null);
              } catch (IOException e) {
                closingFinishedFuture.completeExceptionally(e);
              }
            },
            MoreExecutors.directExecutor());

    this.referenceCounter.decrementReferenceCount();
    try {
      // Wait for in-flight requests to be finished but with a timeout to prevent deadlock.
      closingFinishedFuture.get(
          this.configuration.mirroringOptions.connectionTerminationTimeoutMillis,
          TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      // If the secondary close has thrown while we were waiting, the error will be
      // propagated to the user.
      exceptions.add(new IOException(e));
    } catch (TimeoutException e) {
      // But if the timeout was reached, we just leave the operation pending.
      Log.error(
          "MirroringAsyncConnection#close() timed out. Some of operations on secondary "
              + "database are still in-flight and might be lost, but are not cancelled and "
              + "will be performed asynchronously until the program terminates.");
      // This error is not reported to the user.
    }

    exceptions.rethrowIfCaptured();
  }

  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    return new MirroringAsyncTableBuilder<>(
        this.primaryConnection.getTableBuilder(tableName),
        this.secondaryConnection.getTableBuilder(tableName));
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(
      TableName tableName, ExecutorService executorService) {
    return new MirroringAsyncTableBuilder<>(
        this.primaryConnection.getTableBuilder(tableName, executorService),
        this.secondaryConnection.getTableBuilder(tableName, executorService));
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    return new MirroringAsyncBufferedMutatorBuilder(
        this.primaryConnection.getBufferedMutatorBuilder(tableName),
        this.secondaryConnection.getBufferedMutatorBuilder(tableName));
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(
      TableName tableName, ExecutorService executorService) {
    return getBufferedMutatorBuilder(tableName);
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return this.primaryConnection.getRegionLocator(tableName);
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Hbck getHbck(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  private class MirroringAsyncTableBuilder<C extends ScanResultConsumerBase>
      extends BuilderParameterSetter<AsyncTableBuilder<C>> implements AsyncTableBuilder<C> {
    private final AsyncTableBuilder<C> primaryTableBuilder;
    private final AsyncTableBuilder<C> secondaryTableBuilder;

    public MirroringAsyncTableBuilder(
        AsyncTableBuilder<C> primaryTableBuilder, AsyncTableBuilder<C> secondaryTableBuilder) {
      this.primaryTableBuilder = primaryTableBuilder;
      this.secondaryTableBuilder = secondaryTableBuilder;
    }

    @Override
    public AsyncTable<C> build() {
      return new MirroringAsyncTable<>(
          this.primaryTableBuilder.build(),
          this.secondaryTableBuilder.build(),
          mismatchDetector,
          flowController,
          secondaryWriteErrorConsumer,
          mirroringTracer,
          readSampler,
          timestamper,
          referenceCounter,
          executorService,
          configuration.mirroringOptions.resultScannerBufferedMismatchedResults);
    }

    @Override
    public AsyncTableBuilder<C> setOperationTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setOperationTimeout,
          this.secondaryTableBuilder::setOperationTimeout);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setScanTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setScanTimeout,
          this.secondaryTableBuilder::setScanTimeout);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setRpcTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setRpcTimeout,
          this.secondaryTableBuilder::setRpcTimeout);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setReadRpcTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setReadRpcTimeout,
          this.secondaryTableBuilder::setReadRpcTimeout);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setWriteRpcTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setWriteRpcTimeout,
          this.secondaryTableBuilder::setWriteRpcTimeout);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setRetryPause(long pause, TimeUnit unit) {
      setTimeParameter(
          pause,
          unit,
          this.primaryTableBuilder::setRetryPause,
          this.secondaryTableBuilder::setRetryPause);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setRetryPauseForCQTBE(long pause, TimeUnit unit) {
      setTimeParameter(
          pause,
          unit,
          this.primaryTableBuilder::setRetryPauseForCQTBE,
          this.secondaryTableBuilder::setRetryPauseForCQTBE);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setMaxAttempts(int maxAttempts) {
      setIntegerParameter(
          maxAttempts,
          this.primaryTableBuilder::setMaxAttempts,
          this.secondaryTableBuilder::setMaxAttempts);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setStartLogErrorsCnt(int maxRetries) {
      setIntegerParameter(
          maxRetries,
          this.primaryTableBuilder::setStartLogErrorsCnt,
          this.secondaryTableBuilder::setStartLogErrorsCnt);
      return this;
    }
  }

  private class MirroringAsyncBufferedMutatorBuilder
      extends BuilderParameterSetter<AsyncBufferedMutatorBuilder>
      implements AsyncBufferedMutatorBuilder {
    private final AsyncBufferedMutatorBuilder primaryMutatorBuilder;
    private final AsyncBufferedMutatorBuilder secondaryMutatorBuilder;

    public MirroringAsyncBufferedMutatorBuilder(
        AsyncBufferedMutatorBuilder primaryMutatorBuilder,
        AsyncBufferedMutatorBuilder secondaryMutatorBuilder) {
      this.primaryMutatorBuilder = primaryMutatorBuilder;
      this.secondaryMutatorBuilder = secondaryMutatorBuilder;
    }

    @Override
    public AsyncBufferedMutator build() {
      return new MirroringAsyncBufferedMutator(
          this.primaryMutatorBuilder.build(),
          this.secondaryMutatorBuilder.build(),
          configuration,
          flowController,
          secondaryWriteErrorConsumer,
          timestamper);
    }

    @Override
    public AsyncBufferedMutatorBuilder setOperationTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryMutatorBuilder::setOperationTimeout,
          this.secondaryMutatorBuilder::setOperationTimeout);
      return this;
    }

    @Override
    public AsyncBufferedMutatorBuilder setRpcTimeout(long timeout, TimeUnit unit) {
      setTimeParameter(
          timeout,
          unit,
          this.primaryMutatorBuilder::setRpcTimeout,
          this.secondaryMutatorBuilder::setRpcTimeout);
      return this;
    }

    @Override
    public AsyncBufferedMutatorBuilder setRetryPause(long pause, TimeUnit unit) {
      setTimeParameter(
          pause,
          unit,
          this.primaryMutatorBuilder::setRetryPause,
          this.secondaryMutatorBuilder::setRetryPause);
      return this;
    }

    @Override
    public AsyncBufferedMutatorBuilder setWriteBufferSize(long writeBufferSize) {
      setLongParameter(
          writeBufferSize,
          this.primaryMutatorBuilder::setWriteBufferSize,
          this.secondaryMutatorBuilder::setWriteBufferSize);
      return this;
    }

    @Override
    public AsyncBufferedMutatorBuilder setMaxAttempts(int maxAttempts) {
      setIntegerParameter(
          maxAttempts,
          this.primaryMutatorBuilder::setMaxAttempts,
          this.secondaryMutatorBuilder::setMaxAttempts);
      return this;
    }

    @Override
    public AsyncBufferedMutatorBuilder setStartLogErrorsCnt(int startLogErrorsCnt) {
      setIntegerParameter(
          startLogErrorsCnt,
          this.primaryMutatorBuilder::setStartLogErrorsCnt,
          this.secondaryMutatorBuilder::setStartLogErrorsCnt);
      return this;
    }

    @Override
    public AsyncBufferedMutatorBuilder setMaxKeyValueSize(int maxKeyValueSize) {
      setIntegerParameter(
          maxKeyValueSize,
          this.primaryMutatorBuilder::setMaxKeyValueSize,
          this.secondaryMutatorBuilder::setMaxKeyValueSize);
      return this;
    }
  }

  private static class BuilderParameterSetter<T> {
    protected void setTimeParameter(
        long timeAmount,
        TimeUnit timeUnit,
        BiFunction<Long, TimeUnit, T> primaryFunction,
        BiFunction<Long, TimeUnit, T> secondaryFunction) {
      primaryFunction.apply(timeAmount, timeUnit);
      secondaryFunction.apply(timeAmount, timeUnit);
    }

    protected void setIntegerParameter(
        int value, Function<Integer, T> primaryFunction, Function<Integer, T> secondaryFunction) {
      primaryFunction.apply(value);
      secondaryFunction.apply(value);
    }

    protected void setLongParameter(
        long value, Function<Long, T> primaryFunction, Function<Long, T> secondaryFunction) {
      primaryFunction.apply(value);
      secondaryFunction.apply(value);
    }
  }
}
