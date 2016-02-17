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
package com.google.cloud.bigtable.dataflow;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class can be used as a superclass for {@link DoFn}s that require a {@link Connection} in
 * Dataflow.
 * 
 * @param <In> the type of the (main) input elements
 * @param <Out> the type of the (main) output elements
 */
public abstract class AbstractCloudBigtableTableDoFn<In, Out> extends DoFn<In, Out> {
  private static final long serialVersionUID = 1L;

  private static final CloudBigtableConnectionPool pool = new CloudBigtableConnectionPool();

  protected final Logger DOFN_LOG = LoggerFactory.getLogger(getClass());
  protected CloudBigtableConfiguration config;
  protected Connection connection;
  protected long initialStart;
  protected long workStart;
  private AtomicLong operationCounter = new AtomicLong();

  public AbstractCloudBigtableTableDoFn(CloudBigtableConfiguration config) {
    this.config = config;
  }

  @Override
  public void startBundle(DoFn<In, Out>.Context c) throws Exception {
    super.startBundle(c);
    initialStart = System.currentTimeMillis();
  }

  protected synchronized Connection getConnection() throws IOException {
    if (connection == null) {
      long connectionStart = System.currentTimeMillis();
      DOFN_LOG.info("{} Starting work. Initialization took: {} ms.",
        getClass().getSimpleName(),
        connectionStart - initialStart);
      connection = pool.getConnection(config.toHBaseConfig());
      workStart = System.currentTimeMillis();
      DOFN_LOG.info("{} Creating Connection took: {} ms.",
        getClass().getSimpleName(),
        workStart - connectionStart);
    }
    return connection;
  }

  /**
   * Logs the {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context} and the exception's
   * {@link RetriesExhaustedWithDetailsException#getExhaustiveDescription()}.
   */
  protected void logExceptions(Context context, RetriesExhaustedWithDetailsException exception) {
    DOFN_LOG.warn("For context {}: exception occured during bulk writing: {}", context,
      exception.getExhaustiveDescription());
  }

  protected long incrementOperationCounter() {
    return operationCounter.incrementAndGet();
  }

  protected long incrementOperationCounter(long count) {
    return operationCounter.addAndGet(count);
  }

  protected void logOperationStatistics() {
    long totalOps = operationCounter.get();
    long elapsedTimeMs = System.currentTimeMillis() - workStart;
    long operationsPerSecond = totalOps * 1000 / elapsedTimeMs;
    DOFN_LOG.info(
        "{} Processed {} operations in {} ms. That's {} operations/sec",
        getClass().getSimpleName(),
        totalOps,
        elapsedTimeMs,
        operationsPerSecond);
  }

  @Override
  public void finishBundle(DoFn<In, Out>.Context c) throws Exception {
    logOperationStatistics();
    super.finishBundle(c);
  }

  protected static void retrowException(RetriesExhaustedWithDetailsException exception)
      throws Exception {
    if (exception.getCauses().size() == 1) {
      throw (Exception) exception.getCause(0);
    } else {
      throw exception;
    }
  }
}