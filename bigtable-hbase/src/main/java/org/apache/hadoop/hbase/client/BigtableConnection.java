/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

// Because MasterKeepAliveConnection is default scope, we have to use this package.  :-/
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
import com.google.cloud.bigtable.hbase.BigtableClientPool;
import com.google.cloud.bigtable.hbase.BigtableClientPool.BigtableClientFactory;
import com.google.cloud.bigtable.hbase.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.cloud.bigtable.hbase.BigtableTable;
import com.google.cloud.bigtable.hbase.Logger;
import com.google.cloud.hadoop.hbase.BigtableAdminClient;
import com.google.cloud.hadoop.hbase.BigtableAdminGrpcClient;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.cloud.hadoop.hbase.BigtableGrpcClient;
import com.google.cloud.hadoop.hbase.ChannelOptions;
import com.google.cloud.hadoop.hbase.TransportOptions;

public class BigtableConnection implements Connection, Closeable {
  public static final String MAX_INFLIGHT_RPCS = "MAX_INFLIGHT_RPCS";


  /**
   * The number of grpc channels to open for asynchronous processing such as puts.
   */
  public static final String BIGTABLE_CHANNEL_COUNT_KEY = "google.bigtable.grpc.channel.count";
  public static final int BIGTABLE_CHANNEL_COUNT_DEFAULT = 4;

  /**
   * The maximum length of time to keep a Bigtable grpc channel open.
   */
  public static final String BIGTABLE_CHANNEL_TIMEOUT_MS_KEY =
      "google.bigtable.grpc.channel.timeout.ms";
  public static final long BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT = 30 * 60 * 1000;

  private static final Logger LOG = new Logger(BigtableConnection.class);

  private static final Set<RegionLocator> locatorCache = new CopyOnWriteArraySet<>();

  private final Configuration conf;
  private volatile boolean closed;
  private volatile boolean aborted;
  private volatile ExecutorService batchPool = null;
  private BigtableClient client;
  private BigtableAdminClient bigtableAdminClient;

  private volatile boolean cleanupPool = false;
  private final BigtableOptions options;
  private final TableConfiguration tableConfig;

  // A set of tables that have been disabled via BigtableAdmin.
  private Set<TableName> disabledTables = new HashSet<>();

  public BigtableConnection(Configuration conf) throws IOException {
    this(conf, false, null, null);
  }

  BigtableConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    this.batchPool = pool;
    this.closed = false;
    this.conf = conf;
    if (managed) {
      throw new IllegalArgumentException("Bigtable does not support managed connections.");
    }

    this.options = BigtableOptionsFactory.fromConfiguration(conf);
    this.tableConfig = new TableConfiguration(conf);
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    return getTable(tableName, getBatchPool());
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
    return new BigtableTable(tableName, options, conf, getClient(), getBatchPool());
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    if (params.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null.");
    }
    if (params.getPool() == null) {
      params.pool(getBatchPool());
    }
    if (params.getWriteBufferSize() == BufferedMutatorParams.UNSET) {
      params.writeBufferSize(tableConfig.getWriteBufferSize());
    }

    int bufferCount = conf.getInt(MAX_INFLIGHT_RPCS, 30);

    return new BigtableBufferedMutator(conf,
        params.getTableName(),
        bufferCount,
        params.getWriteBufferSize(),
        getClient(),
        options,
        params.getPool(),
        params.getListener());
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  private synchronized BigtableClient getClient() {
    if (this.client == null) {
      int channelCount =
          conf.getInt(BIGTABLE_CHANNEL_COUNT_KEY, BIGTABLE_CHANNEL_COUNT_DEFAULT);

      long channelTimeout =
          conf.getLong(BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT);

      BigtableClientFactory clientFactory = new BigtableClientPool.BigtableClientFactory() {
        @Override
        public BigtableClient create() {
          try {
            ChannelOptions channelOptions = options.getChannelOptions();
            return BigtableGrpcClient.createClient(options.getTransportOptions(), channelOptions,
                getBatchPool());
          } catch (IOException e) {
            LOG.warn("Could not create transport options", e);
            return null;
          }
        }
      };

      this.client =
          new BigtableClientPool(channelCount, channelTimeout, getBatchPool(), clientFactory);
    }
    return client;

  }
  /** This should not be used.  The hbase shell needs this in hbsae 0.99.2.  Remove this once
   * 1.0.0 comes out
   */
  @Deprecated
  public Table getTable(String tableName) throws IOException {
    return getTable(TableName.valueOf(tableName));
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    for (RegionLocator locator : locatorCache) {
      if (locator.getName().equals(tableName)) {
        return locator;
      }
    }

    RegionLocator newLocator = new BigtableRegionLocator(tableName, options, client);

    if (locatorCache.add(newLocator)) {
      return newLocator;
    }

    for (RegionLocator locator : locatorCache) {
      if (locator.getName().equals(tableName)) {
        return locator;
      }
    }

    throw new IllegalStateException(newLocator + " was supposed to be in the cache");
  }

  @Override
  public Admin getAdmin() throws IOException {
    if (bigtableAdminClient == null) {
      TransportOptions adminTransportOptions = options.getAdminTransportOptions();
      ChannelOptions channelOptions = options.getChannelOptions();
      this.bigtableAdminClient =
          BigtableAdminGrpcClient.createClient(adminTransportOptions, channelOptions, getBatchPool());
    }
    return new BigtableAdmin(options, conf, this, bigtableAdminClient, this.disabledTables);
  }

  @Override
  public void abort(final String msg, Throwable t) {
    if (t != null) {
      LOG.fatal(msg, t);
    } else {
      LOG.fatal(msg);
    }
    this.aborted = true;
    close();
    this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  @Override
  public void close() {
    if (this.closed) {
      return;
    }
    try {
      if (bigtableAdminClient != null) {
        bigtableAdminClient.close();
      }
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    shutdownBatchPool();
    this.closed = true;
  }

  // Copied from org.apache.hadoop.hbase.client.HConnectionManager#getBatchPool()
  private ExecutorService getBatchPool() {
    if (batchPool == null) {
      // shared HTable thread executor not yet initialized
      synchronized (this) {
        if (batchPool == null) {
          int maxThreads = conf.getInt("hbase.hconnection.threads.max", 256);
          if (maxThreads == 0) {
            maxThreads = Runtime.getRuntime().availableProcessors() * 8;
          }
          long keepAliveTime = conf.getLong(
              "hbase.hconnection.threads.keepalivetime", 60);
          LinkedBlockingQueue<Runnable> workQueue =
              new LinkedBlockingQueue<Runnable>(128 *
                  conf.getInt("hbase.client.max.total.tasks", 200));
          this.batchPool = new ThreadPoolExecutor(
              maxThreads,
              maxThreads,
              keepAliveTime,
              TimeUnit.SECONDS,
              workQueue,
              Threads.newDaemonThreadFactory("bigtable-connection-shared-executor"));
        }
        this.cleanupPool = true;
      }
    }
    return this.batchPool;
  }

  // Copied from org.apache.hadoop.hbase.client.HConnectionManager#shutdownBatchPool()
  private void shutdownBatchPool() {
    if (this.cleanupPool && this.batchPool != null && !this.batchPool.isShutdown()) {
      this.batchPool.shutdown();
      try {
        if (!this.batchPool.awaitTermination(10, TimeUnit.SECONDS)) {
          this.batchPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        this.batchPool.shutdownNow();
      }
    }
  }
}
