/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.beam;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used as a superclass for {@link DoFn}s that require a {@link Connection} in
 * Dataflow.
 *
 * @param <In> the type of the (main) input elements
 * @param <Out> the type of the (main) output elements
 */
public abstract class AbstractCloudBigtableTableDoFn<In, Out> extends DoFn<In, Out> {
  private static final long serialVersionUID = 1L;

  public static void logRetriesExhaustedWithDetailsException(
      Logger log, String context, RetriesExhaustedWithDetailsException exception) {

    if (log.isDebugEnabled()) {
      log.error(
          "For context {}: exception occurred during a bulk operation: {}",
          context,
          exception.getExhaustiveDescription());
    }
    if (exception.getNumExceptions() == 0) {
      log.error(
          "For context {}: Got a RetriesExhaustedWithDetailsException without any details.",
          exception);
      return;
    }
    log.error(
        String.format(
            "For context %s: %d exceptions occurred during a bulk operation:\n\t%s.\n"
                + "The stack trace is a sample exception, of the first exception.\n"
                + "Breakdown of exceptions {type - count}: %s",
            context,
            exception.getNumExceptions(),
            exception.getMessage(),
            getFurtherInfo(exception)),
        exception.getCause(0));
  }

  private static Map<String, Integer> getFurtherInfo(
      RetriesExhaustedWithDetailsException exception) {
    Map<String, Integer> furtherInfo = new TreeMap<>();
    try {
      List<Throwable> causes = exception.getCauses();
      for (Throwable throwable : causes) {
        String message = throwable.getClass().getName() + ": " + throwable.getMessage();
        Integer count = furtherInfo.get(message);
        if (count == null) {
          furtherInfo.put(message, 1);
        } else {
          furtherInfo.put(message, 1 + count);
        }
      }
    } catch (Throwable t) {
      // Don't fail just because information gathering failed.
    }
    return furtherInfo;
  }

  protected static void rethrowException(RetriesExhaustedWithDetailsException exception)
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

  protected synchronized Connection getConnection() {
    if (connection == null) {
      // This uses cached grpc channels, if there was a previous connection created.
      connection = BigtableConfiguration.connect(config.toHBaseConfig());
    }
    return connection;
  }

  /**
   * Logs a context and the exception's {@link
   * RetriesExhaustedWithDetailsException#getExhaustiveDescription()}.
   */
  protected void logExceptions(Object context, RetriesExhaustedWithDetailsException exception) {
    logRetriesExhaustedWithDetailsException(DOFN_LOG, String.valueOf(context), exception);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    config.populateDisplayData(builder);
  }

  public CloudBigtableConfiguration getConfig() {
    return config;
  }
}
