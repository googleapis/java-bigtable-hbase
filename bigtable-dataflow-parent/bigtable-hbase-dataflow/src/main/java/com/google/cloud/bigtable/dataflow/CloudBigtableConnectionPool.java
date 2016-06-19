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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase1_0.BigtableConnection;
import com.google.common.annotations.VisibleForTesting;

/**
 * Pubsub and other windowed sources can have a large quantity of bundles in short amounts of time.
 * {@link AbstractCloudBigtableTableDoFn} should not create a connection per
 * bundle, since that could happen ever few milliseconds. Rather, it should rely on a connection
 * pool to better manage connection life-cycles.
 */
public class CloudBigtableConnectionPool {

  protected static final Logger LOG = LoggerFactory.getLogger(CloudBigtableConnectionPool.class);

  private final Map<String, Connection> connections = new HashMap<>();

  public CloudBigtableConnectionPool() {
  }

  /**
   * Gets a shared connection where the cluster name from the config is the key.
   *
   * <p>NOTE: Do not call close() on the connection, since it's shared.
   *
   * @param config
   * @return
   * @throws IOException
   */
  public Connection getConnection(Configuration config) throws IOException {
    String key = BigtableOptionsFactory.fromConfiguration(config).getInstanceName().toString();
    return getConnection(config, key);
  }

  protected synchronized Connection getConnection(Configuration config, String key)
      throws IOException {
    Connection connection = connections.get(key);
    if (connection == null) {
      connection = createConnection(config);
      connections.put(key, connection);
    }
    return connection;
  }

  @VisibleForTesting
  protected Connection createConnection(Configuration config) throws IOException {
    return new BigtableConnection(config) {
      @Override
      public void close() throws IOException {
        // Users should not actually close the shared connection. Make sure that if a user does call
        // close, that nothing bad happens to other potential users.
        // All of the resources will be cleaned up when the JVM closes.
        LOG.info("Calling close() on the connection from dataflow is a noop. "
            + "Please don't close() the connection yourself.");
      }
    };
  }
}
