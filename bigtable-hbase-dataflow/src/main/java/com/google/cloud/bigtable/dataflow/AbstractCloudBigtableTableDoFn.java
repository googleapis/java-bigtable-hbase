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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class can be used as a superclass for {@link DoFn}s that require a {@link Connection} in
 * Dataflow.
 * 
 * @param <In> the type of the (main) input elements
 * @param <Out> the type of the (main) output elements
 */
public abstract class AbstractCloudBigtableTableDoFn<In, Out> extends DoFn<In, Out> {
  private static final long serialVersionUID = 1L;

  static final CloudBigtableConnectionPool pool = new CloudBigtableConnectionPool();

  public static void logRetriesExhaustedWithDetailsException(
      Logger log, String context, RetriesExhaustedWithDetailsException exception) {

    if (log.isDebugEnabled()) {
      log.error("For context {}: exception occured during a bulk operation: {}", context,
        exception.getExhaustiveDescription());
    }
    if (exception.getNumExceptions() == 0) {
      log.error(
          "For context {}: Got a RetriesExhaustedWithDetailsException without any details.",
          exception);
      return;
    }
    Map<String, Integer> furtherInfo = new TreeMap<>();
    List<Throwable> causes = exception.getCauses();
    for (Throwable throwable : causes) {
      String message = throwable.getMessage();
      Integer count = furtherInfo.get(message);
      if (count == null) {
        furtherInfo.put(message, 1);
      } else {
        furtherInfo.put(message, 1 + count);
      }
    }
    log.error(
        String.format(
            "For context %s: %d exceptions occured during a bulk operation:\n\t%s.\n"
                + "The stack trace is a sample exception, of the first exception.\n"
                + "Breakdown of exceptions {type - count}: %s",
            context,
            exception.getNumExceptions(),
            exception.getMessage(),
            furtherInfo),
        exception.getCause(0));
  }

  protected static void retrowException(RetriesExhaustedWithDetailsException exception)
      throws Exception {
    if (exception.getCauses().size() == 1) {
      throw (Exception) exception.getCause(0);
    } else {
      throw exception;
    }
  }

  protected final Logger DOFN_LOG = LoggerFactory.getLogger(getClass());
  protected final CloudBigtableConfiguration config;
  protected Connection connection;

  public AbstractCloudBigtableTableDoFn(CloudBigtableConfiguration config) {
    this.config = config;
  }

  protected synchronized Connection getConnection() throws IOException {
    if (connection == null) {
      connection = pool.getConnection(config.toHBaseConfig());
    }
    return connection;
  }

  /**
   * Logs the {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context} and the exception's
   * {@link RetriesExhaustedWithDetailsException#getExhaustiveDescription()}.
   */
  protected void logExceptions(Context context, RetriesExhaustedWithDetailsException exception) {
    logRetriesExhaustedWithDetailsException(DOFN_LOG, context.toString(), exception);
  }
}
