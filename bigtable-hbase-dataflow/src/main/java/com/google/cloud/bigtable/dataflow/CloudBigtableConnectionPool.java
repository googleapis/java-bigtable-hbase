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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase1_0.BigtableConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Pubsub and other windowed sources can have a large quantity of bundles in short amounts of time.
 * {@link AbstractCloudBigtableTableDoFn} should not create a connection per
 * bundle, since that could happen ever few milliseconds. Rather, it should rely on a connection
 * pool to better manage connection life-cycles.
 */
public class CloudBigtableConnectionPool {

  private static final int MAX_TTL_MILLISECONDS = 30 * 60 * 1000;
  protected static final Logger LOG = LoggerFactory.getLogger(CloudBigtableConnectionPool.class);

  /**
   * This class abstracts a connection that could be returned to a
   * {@link CloudBigtableConnectionPool}.
   */
  public static class PoolEntry {
    private final String key;
    private final Connection connection;
    private final long expiresTimeMs;

    private static long getExpiration() {
      return System.currentTimeMillis() + MAX_TTL_MILLISECONDS;
    }

    public PoolEntry(String key, Connection connection) {
      this(key, connection, getExpiration());
    }

    @VisibleForTesting
    PoolEntry(String key, Connection connection, long expiresTimeMs) {
      this.key = key;
      this.connection = connection;
      this.expiresTimeMs = expiresTimeMs;
    }

    public Connection getConnection() {
      return connection;
    }

    public String getKey() {
      return key;
    }

    public boolean isExpired() {
      return System.currentTimeMillis() > expiresTimeMs;
    }

    @Override
    public String toString() {
      return String.format("%s key=%s, hash=%s", getClass().getName(), key, super.toString());
    }
  }

  private static ExecutorService createDefaultCloseExecutorService() {
    return Executors.newCachedThreadPool(
      new ThreadFactoryBuilder()
          .setNameFormat("CloudBigtableConnectionPool-cleanup-%s")
          .setDaemon(true)
          .build());
  }

  private final LinkedHashMultimap<String, PoolEntry> connections = LinkedHashMultimap.create();
  private final ExecutorService connectionCloseExecutor;

  public CloudBigtableConnectionPool() {
    this(createDefaultCloseExecutorService());
  }

  @VisibleForTesting
  CloudBigtableConnectionPool(ExecutorService executorService) {
    this.connectionCloseExecutor = executorService;
  }

  public PoolEntry getConnection(Configuration config) throws IOException {
    String key = BigtableOptionsFactory.fromConfiguration(config).getClusterName().toString();
    return getConnection(config, key);
  }

  protected synchronized PoolEntry getConnection(Configuration config, String key)
      throws IOException {
    Set<PoolEntry> entries = connections.get(key);
    if (entries.isEmpty()) {
      return createConnection(config, key);
    }
    for (Iterator<PoolEntry> iterator = entries.iterator(); iterator.hasNext();) {
      PoolEntry entry = iterator.next();
      iterator.remove();
      if (entry.isExpired()) {
        closeAsynchronously(entry);
      } else {
        return entry;
      }
    }
    return createConnection(config, key);
  }

  @VisibleForTesting
  protected PoolEntry createConnection(Configuration config, String key) throws IOException {
    return new PoolEntry(key, new BigtableConnection(config));
  }

  public synchronized void returnConnection(PoolEntry entry) {
    if (entry.isExpired()) {
      closeAsynchronously(entry);
    } else {
      connections.put(entry.getKey(), entry);
    }
  }

  private void closeAsynchronously(final PoolEntry entry) {
    connectionCloseExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          entry.connection.close();
        } catch (Exception e) {
          LOG.warn("Could not close a connection asynchronously.", e);
        }
        return null;
      }
    });
  }
}
