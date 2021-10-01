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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Failed mutation log.
 *
 * <p>Objects of this class enable persisting failed mutations.
 */
public class Logger implements AutoCloseable {
  private final Serializer serializer;
  private final Appender appender;

  Logger() throws IOException {
    this("/tmp/hbase_mirroring_client_failed_mutations", 1024 * 1024, false);
  }

  /**
   * Create a logger with a default implementation.
   *
   * <p>The default implementation will write to files with the given prefix. Most of the time, the
   * actual writing should be asynchronous, because the failed mutations are buffered and flushed to
   * disk by another thread.
   *
   * @param pathPrefix the prefix of the created log files
   * @param maxBufferSize the maximum amount of log entries kept in memory before flushing to disk
   * @param dropOnOverFlow if this logger is not keeping up with flushing the incoming mutations to
   *     disk (i.e. the buffer is becoming full), this parameter governs what to do with following
   *     mutations; if set to true, they will be dropped; if set to false, the log will block the
   *     thread attempting to write until there some data is flushed to disk
   * @throws IOException on failure to write the log
   */
  Logger(String pathPrefix, int maxBufferSize, boolean dropOnOverFlow) throws IOException {
    this(new DefaultAppender(pathPrefix, maxBufferSize, dropOnOverFlow), new DefaultSerializer());
  }

  /**
   * Create a logger with a user-provided implementation of how to serialize log entries and where
   * to store them.
   *
   * @param appender an object responsible for storing log entries
   * @param serializer on object responsible for transforming failed mutations into log entries
   */
  Logger(Appender appender, Serializer serializer) {
    this.appender = appender;
    this.serializer = serializer;
  }

  /**
   * Log a failed mutation.
   *
   * <p>This method is thread safe.
   *
   * @param mutation the mutation which failed
   * @param failureCause the cause of the failure
   * @throws InterruptedException in case the appender thread was interrupted
   */
  void mutationFailed(Mutation mutation, Throwable failureCause) throws InterruptedException {
    byte[] serializedEntry = serializer.serialize(mutation, failureCause);
    appender.append(serializedEntry);
  }

  @Override
  public void close() throws Exception {
    appender.close();
  }
}
