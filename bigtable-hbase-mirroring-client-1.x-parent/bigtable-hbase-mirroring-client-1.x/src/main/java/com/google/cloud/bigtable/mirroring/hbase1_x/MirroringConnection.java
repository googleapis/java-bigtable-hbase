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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.reflection.ReflectionConstructor;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  private final FlowController flowController;
  private MirroringConfiguration configuration;
  private Connection primaryConnection;
  private Connection secondaryConnection;
  private final ExecutorService executorService;
  private final MismatchDetector mismatchDetector;
  private final ListenableReferenceCounter referenceCounter;
  private static final Logger Log = new Logger(MirroringConnection.class);
  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;

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
        ReflectionConstructor.construct(this.configuration.mirroringOptions.mismatchDetectorClass);

    this.secondaryWriteErrorConsumer =
        ReflectionConstructor.construct(
            this.configuration.mirroringOptions.writeErrorConsumerClass);
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    Log.trace("getTable(%s)", tableName);
    MirroringTable table =
        new MirroringTable(
            this.primaryConnection.getTable(tableName),
            this.secondaryConnection.getTable(tableName),
            this.executorService,
            this.mismatchDetector,
            this.flowController,
            this.secondaryWriteErrorConsumer);
    this.referenceCounter.holdReferenceUntilClosing(table);
    return table;
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
    Log.trace("getTable(%s, executorService)", tableName);
    MirroringTable table =
        new MirroringTable(
            this.primaryConnection.getTable(tableName, executorService),
            this.secondaryConnection.getTable(tableName, executorService),
            executorService,
            this.mismatchDetector,
            this.flowController,
            this.secondaryWriteErrorConsumer);
    this.referenceCounter.holdReferenceUntilClosing(table);
    return table;
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams)
      throws IOException {
    return new MirroringBufferedMutator(
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        configuration,
        flowController,
        executorService,
        secondaryWriteErrorConsumer);
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
    Log.trace("close()");
    this.referenceCounter.decrementReferenceCount();
    try {
      this.referenceCounter.getOnLastReferenceClosed().get();
      Log.trace("close(): closed");
    } catch (InterruptedException e) {
      IOException wrapperException = new InterruptedIOException();
      wrapperException.initCause(e);
      throw wrapperException;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abort(String s, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAborted() {
    throw new UnsupportedOperationException();
  }

  public Connection getPrimaryConnection() {
    return this.primaryConnection;
  }

  public Connection getSecondaryConnection() {
    return this.secondaryConnection;
  }
}
