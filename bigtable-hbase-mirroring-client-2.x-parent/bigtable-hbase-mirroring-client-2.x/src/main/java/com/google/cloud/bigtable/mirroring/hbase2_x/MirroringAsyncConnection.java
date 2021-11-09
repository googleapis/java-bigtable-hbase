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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.reflection.ReflectionConstructor;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncAdminBuilder;
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
      throws ExecutionException, InterruptedException {
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
            ReflectionConstructor.construct(
                this.configuration.mirroringOptions.flowControllerStrategyClass,
                this.configuration.mirroringOptions));
    this.mismatchDetector =
        ReflectionConstructor.construct(
            this.configuration.mirroringOptions.mismatchDetectorClass, this.mirroringTracer);

    Logger failedWritesLogger =
        new Logger(
            ReflectionConstructor.construct(
                this.configuration.mirroringOptions.writeErrorLogAppenderClass,
                Configuration.class,
                this.configuration),
            ReflectionConstructor.construct(
                this.configuration.mirroringOptions.writeErrorLogSerializerClass));

    SecondaryWriteErrorConsumer writeErrorConsumer =
        ReflectionConstructor.construct(
            this.configuration.mirroringOptions.writeErrorConsumerClass, failedWritesLogger);

    this.secondaryWriteErrorConsumer =
        new SecondaryWriteErrorConsumerWithMetrics(this.mirroringTracer, writeErrorConsumer);

    this.readSampler = new ReadSampler(this.configuration.mirroringOptions.readSamplingRate);
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

    this.referenceCounter.decrementReferenceCount();
    try {
      this.referenceCounter.getOnLastReferenceClosed().get();
      this.primaryConnection.close();
      this.secondaryConnection.close();
    } catch (InterruptedException e) {
      IOException wrapperException = new InterruptedIOException();
      wrapperException.initCause(e);
      throw wrapperException;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
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
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    throw new UnsupportedOperationException();
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
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(
      TableName tableName, ExecutorService executorService) {
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
      implements AsyncTableBuilder<C> {
    private final AsyncTableBuilder<C> primaryTableBuilder;
    private final AsyncTableBuilder<C> secondaryTableBuilder;

    public MirroringAsyncTableBuilder(
        AsyncTableBuilder<C> primaryTableBuilder, AsyncTableBuilder<C> secondaryTableBuilder) {
      this.primaryTableBuilder = primaryTableBuilder;
      this.secondaryTableBuilder = secondaryTableBuilder;
    }

    @Override
    public AsyncTable<C> build() {
      return new MirroringAsyncTable<C>(
          this.primaryTableBuilder.build(),
          this.secondaryTableBuilder.build(),
          mismatchDetector,
          flowController,
          secondaryWriteErrorConsumer,
          mirroringTracer,
          readSampler,
          referenceCounter);
    }

    private AsyncTableBuilder<C> setTimeParameter(
        long timeAmount,
        TimeUnit timeUnit,
        BiFunction<Long, TimeUnit, AsyncTableBuilder<C>> primaryFunction,
        BiFunction<Long, TimeUnit, AsyncTableBuilder<C>> secondaryFunction) {
      primaryFunction.apply(timeAmount, timeUnit);
      secondaryFunction.apply(timeAmount, timeUnit);
      return this;
    }

    private AsyncTableBuilder<C> setIntegerParameter(
        int value,
        Function<Integer, AsyncTableBuilder<C>> primaryFunction,
        Function<Integer, AsyncTableBuilder<C>> secondaryFunction) {
      primaryFunction.apply(value);
      secondaryFunction.apply(value);
      return this;
    }

    @Override
    public AsyncTableBuilder<C> setOperationTimeout(long timeout, TimeUnit unit) {
      return setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setOperationTimeout,
          this.secondaryTableBuilder::setOperationTimeout);
    }

    @Override
    public AsyncTableBuilder<C> setScanTimeout(long timeout, TimeUnit unit) {
      return setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setScanTimeout,
          this.secondaryTableBuilder::setScanTimeout);
    }

    @Override
    public AsyncTableBuilder<C> setRpcTimeout(long timeout, TimeUnit unit) {
      return setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setRpcTimeout,
          this.secondaryTableBuilder::setRpcTimeout);
    }

    @Override
    public AsyncTableBuilder<C> setReadRpcTimeout(long timeout, TimeUnit unit) {
      return setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setReadRpcTimeout,
          this.secondaryTableBuilder::setReadRpcTimeout);
    }

    @Override
    public AsyncTableBuilder<C> setWriteRpcTimeout(long timeout, TimeUnit unit) {
      return setTimeParameter(
          timeout,
          unit,
          this.primaryTableBuilder::setWriteRpcTimeout,
          this.secondaryTableBuilder::setWriteRpcTimeout);
    }

    @Override
    public AsyncTableBuilder<C> setRetryPause(long pause, TimeUnit unit) {
      return setTimeParameter(
          pause,
          unit,
          this.primaryTableBuilder::setRetryPause,
          this.secondaryTableBuilder::setRetryPause);
    }

    @Override
    public AsyncTableBuilder<C> setRetryPauseForCQTBE(long pause, TimeUnit unit) {
      return setTimeParameter(
          pause,
          unit,
          this.primaryTableBuilder::setRetryPauseForCQTBE,
          this.secondaryTableBuilder::setRetryPauseForCQTBE);
    }

    @Override
    public AsyncTableBuilder<C> setMaxAttempts(int maxAttempts) {
      return setIntegerParameter(
          maxAttempts,
          this.primaryTableBuilder::setMaxAttempts,
          this.secondaryTableBuilder::setMaxAttempts);
    }

    @Override
    public AsyncTableBuilder<C> setStartLogErrorsCnt(int maxRetries) {
      return setIntegerParameter(
          maxRetries,
          this.primaryTableBuilder::setStartLogErrorsCnt,
          this.secondaryTableBuilder::setStartLogErrorsCnt);
    }
  }
}
