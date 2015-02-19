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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
import com.google.cloud.bigtable.hbase.BigtableOptions;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.cloud.bigtable.hbase.BigtableTable;
import com.google.cloud.bigtable.hbase.Logger;
import com.google.cloud.hadoop.hbase.AnviltopAdminBlockingGrpcClient;
import com.google.cloud.hadoop.hbase.AnviltopAdminClient;
import com.google.cloud.hadoop.hbase.BigtableGrpcClient;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.cloud.hadoop.hbase.ChannelOptions;
import com.google.cloud.hadoop.hbase.TransportOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BigtableConnection implements Connection, Closeable {
  public static final String BUFFERED_MUTATOR_MAX_THREADS = "com.google.cloud.bigtable.hbase.buffered_mutator.max_threads";
  private static final Logger LOG = new Logger(BigtableConnection.class);

  private static final Set<RegionLocator> locatorCache = new CopyOnWriteArraySet<>();

  private final Configuration conf;
  private volatile boolean closed;
  private volatile boolean aborted;
  private volatile ExecutorService batchPool = null;
  private BigtableClient client;
  private AnviltopAdminClient bigtableAdminClient;
  private User user = null;
  private volatile boolean cleanupPool = false;
  private final BigtableOptions options;
  private final TableConfiguration tableConfig;

  public BigtableConnection(Configuration conf) throws IOException {
    this(conf, false, null, null);
  }

  BigtableConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    this.user = user;
    this.batchPool = pool;
    this.closed = false;
    this.conf = conf;
    if (managed) {
      throw new IllegalArgumentException("Bigtable does not support managed connections.");
    }

    if (batchPool == null) {
      batchPool = getBatchPool();
    }

    this.options = BigtableOptionsFactory.fromConfiguration(conf);
    TransportOptions transportOptions = options.getTransportOptions();
    ChannelOptions channelOptions = options.getChannelOptions();
    TransportOptions adminTransportOptions = options.getAdminTransportOptions();

    LOG.info("Opening connection for project %s on data host:port %s:%s, "
        + "admin host:port %s:%s, using transport %s.",
        options.getProjectId(),
        options.getTransportOptions().getHost(),
        options.getTransportOptions().getPort(),
        options.getAdminTransportOptions().getHost(),
        options.getAdminTransportOptions().getPort(),
        options.getTransportOptions().getTransport());

    this.client = getBigtableClient(
        transportOptions,
        channelOptions,
        batchPool);
    this.bigtableAdminClient = getAdminClient(
        adminTransportOptions,
        channelOptions,
        batchPool);
    this.tableConfig = new TableConfiguration(conf);
  }

  private AnviltopAdminClient getAdminClient(
      TransportOptions transportOptions,
      ChannelOptions channelOptions,
      ExecutorService executorService) {

    return AnviltopAdminBlockingGrpcClient.createClient(
        transportOptions, channelOptions, executorService);
  }

  protected BigtableClient getBigtableClient(
      TransportOptions transportOptions,
      ChannelOptions channelOptions,
      ExecutorService executorService) {

    return BigtableGrpcClient.createClient(
        transportOptions, channelOptions, executorService);
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
    return new BigtableTable(tableName, options, conf, client, pool);
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

    int bufferCount = conf.getInt(BUFFERED_MUTATOR_MAX_THREADS, 30);

    return new BigtableBufferedMutator(conf,
        params.getTableName(),
        bufferCount,
        params.getWriteBufferSize(),
        client,
        options,
        params.getPool(),
        params.getListener());
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return getBufferedMutator(new BufferedMutatorParams(tableName));
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
    return new BigtableAdmin(options, conf, this, bigtableAdminClient);
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
      bigtableAdminClient.close();
      client.close();
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
