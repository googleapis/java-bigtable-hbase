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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
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
  public static final String MAX_INFLIGHT_RPCS_KEY =
      "google.bigtable.buffered.mutator.max.inflight.rpcs";

  // Default rpc count per channel.
  public static final int MAX_INFLIGHT_RPCS_DEFAULT = 2000;

  /**
   * The maximum amount of memory to be used for asynchronous buffered mutator RPCs.
   */
  public static final String BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY =
      "google.bigtable.buffered.mutator.max.memory";

  private static final AtomicInteger SEQUENCE_GENERATOR = new AtomicInteger();
  private static final Set<Integer> ACTIVE_CONNECTIONS =
      Collections.synchronizedSet(new HashSet<Integer>());

  // Default to 16MB.
  //
  // HBase uses 2MB as the write buffer size.  Their limit is the size to reach before performing
  // a batch async write RPC.  Our limit is a memory cap for async RPC after which we throttle.
  // HBase does not throttle like we do.
  public static final long BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_DEFAULT =
      8 * TableConfiguration.WRITE_BUFFER_SIZE_DEFAULT;

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
  private final Integer id;

  // A set of tables that have been disabled via BigtableAdmin.
  private Set<TableName> disabledTables = new HashSet<>();

  public static int getActiveConnectionCount() {
    return ACTIVE_CONNECTIONS.size();
  }

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

    id = SEQUENCE_GENERATOR.incrementAndGet();
    ACTIVE_CONNECTIONS.add(id);

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

  private BigtableAdminClient getAdminClient(
      TransportOptions transportOptions,
      ChannelOptions channelOptions,
      ExecutorService executorService) {

    return BigtableAdminGrpcClient.createClient(
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

    int defaultRpcCount = MAX_INFLIGHT_RPCS_DEFAULT * options.getChannelCount();
    int maxInflightRpcs = conf.getInt(MAX_INFLIGHT_RPCS_KEY, defaultRpcCount);

    return new BigtableBufferedMutator(conf,
        params.getTableName(),
        maxInflightRpcs,
        params.getWriteBufferSize(),
        client,
        options,
        params.getPool(),
        params.getListener());
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    long maxMemory = conf.getLong(
        BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY,
        BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_DEFAULT);
    return getBufferedMutator(
        new BufferedMutatorParams(tableName)
            .writeBufferSize(maxMemory));
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
      bigtableAdminClient.close();
      client.close();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    shutdownBatchPool();
    ACTIVE_CONNECTIONS.remove(id);
    this.closed = true;
  }

  @Override
  public String toString() {
    return String.format("BigtableConnection-0x%s.  Project: %s, Zone: %sm Cluster: %s",
      Integer.toHexString(hashCode()), options.getProjectId(), options.getZone(),
      options.getCluster());
  }

  // Copied from org.apache.hadoop.hbase.client.HConnectionManager#getBatchPool()
  private ExecutorService getBatchPool() {
    if (batchPool == null) {
      // shared HTable thread executor not yet initialized
      synchronized (this) {
        if (batchPool == null) {
          int maxThreads = getMaxThreads();
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

  private int getMaxThreads() {
    int maxThreads = conf.getInt("hbase.hconnection.threads.max", 256);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    return maxThreads;
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
