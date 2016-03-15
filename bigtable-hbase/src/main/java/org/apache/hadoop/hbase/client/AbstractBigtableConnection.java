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
// Because MasterKeepAliveConnection is default scope, we have to use this package.  :-/
package org.apache.hadoop.hbase.client;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.RpcThrottler;
import com.google.cloud.bigtable.grpc.async.ResourceLimiter;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.cloud.bigtable.hbase.BigtableTable;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractBigtableConnection implements Connection, Closeable {
  public static final String MAX_INFLIGHT_RPCS_KEY =
      "google.bigtable.buffered.mutator.max.inflight.rpcs";

  /**
   * The maximum amount of memory to be used for asynchronous buffered mutator RPCs.
   */
  public static final String BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY =
      "google.bigtable.buffered.mutator.max.memory";

  private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong();
  private static final Map<Long, BigtableBufferedMutator> ACTIVE_BUFFERED_MUTATORS =
      Collections.synchronizedMap(new HashMap<Long, BigtableBufferedMutator>());

  static {
    Runnable shutDownRunnable = new Runnable() {
      @Override
      public void run() {
        for (BigtableBufferedMutator bbm : ACTIVE_BUFFERED_MUTATORS.values()) {
          if (bbm.hasInflightRequests()) {
            int size = ACTIVE_BUFFERED_MUTATORS.size();
            new Logger(AbstractBigtableConnection.class).warn(
              "Shutdown is commencing and you have open %d buffered mutators."
                  + "You need to close() or flush() them so that is not lost", size);
            break;
          }
        }
      }
    };
    Runtime.getRuntime().addShutdownHook(new Thread(shutDownRunnable));
  }

  private final Logger LOG = new Logger(getClass());

  private static final Set<RegionLocator> locatorCache = new CopyOnWriteArraySet<>();

  private final Configuration conf;
  private volatile boolean closed = false;
  private volatile boolean aborted;
  private volatile ExecutorService batchPool = null;
  private ExecutorService bufferedMutatorExecutorService;

  private BigtableSession session;

  private volatile boolean cleanupPool = false;
  private final BigtableOptions options;

  // A set of tables that have been disabled via BigtableAdmin.
  private Set<TableName> disabledTables = new HashSet<>();
  private static ResourceLimiter resourceLimiter;

  public AbstractBigtableConnection(Configuration conf) throws IOException {
    this(conf, false, null, null);
  }

  /**
   * The constructor called from {@link ConnectionFactory#createConnection(Configuration)} and
   * in its many forms via reflection with this specific signature.
   *
   * @param conf The configuration for this channel. See {@link BigtableOptionsFactory} for more details.
   * @param managed This should always be false. It's an artifact of old HBase behavior.
   * @param pool An {@link ExecutorService} to run HBase/Bigtable object conversions on. The RPCs
   *             themselves run via NIO, and not on a waiting thread
   * @param user This is an artifact of HBase which Cloud Bigtable ignores. User information is
   * captured in the Credentials configuration in conf.
   *
   * @throws IOException if the setup is not correct. The most likely issue is ALPN or OpenSSL 
   * misconfiguration.
   */
  protected AbstractBigtableConnection(Configuration conf, boolean managed, ExecutorService pool,
      User user) throws IOException {
    if (managed) {
      throw new IllegalArgumentException("Bigtable does not support managed connections.");
    }
    this.conf = conf;
    try {
      this.options = BigtableOptionsFactory.fromConfiguration(conf);
    } catch (IOException ioe) {
      LOG.error("Error loading BigtableOptions from Configuration.", ioe);
      throw ioe;
    }

    this.batchPool = pool;
    this.closed = false;
    this.session = new BigtableSession(options);

    initializeResourceLimiter(conf, options);
  }

  private synchronized static void initializeResourceLimiter(
      Configuration conf, BigtableOptions options) {
    if (resourceLimiter == null) {
      int defaultRpcCount = AsyncExecutor.MAX_INFLIGHT_RPCS_DEFAULT * options.getChannelCount();
      int maxInflightRpcs = conf.getInt(MAX_INFLIGHT_RPCS_KEY, defaultRpcCount);
      long maxMemory = conf.getLong(
          BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY,
          AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT);
      AbstractBigtableConnection.resourceLimiter = new ResourceLimiter(maxMemory, maxInflightRpcs);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    return getTable(tableName, batchPool);
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
    BigtableDataClient client = session.getDataClient();
    if (pool == null) {
      pool = BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool();
    }
    BatchExecutor batchExecutor = new BatchExecutor(
         new AsyncExecutor(client, new RpcThrottler(resourceLimiter)),
         options,
         MoreExecutors.listeningDecorator(pool),
         createAdapter(tableName));
    return new BigtableTable(this, tableName, options, client, createAdapter(tableName), batchExecutor);
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    TableName tableName = params.getTableName();
    if (tableName == null) {
      throw new IllegalArgumentException("TableName cannot be null.");
    }

    final long id = SEQUENCE_GENERATOR.incrementAndGet();

    ExecutorService pool = batchPool != null ? batchPool
        : BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool();
    BigtableBufferedMutator bigtableBufferedMutator = new BigtableBufferedMutator(
        session.getDataClient(),
        createAdapter(tableName),
        conf,
        options,
        params.getListener(),
        new RpcThrottler(resourceLimiter),
        pool) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          ACTIVE_BUFFERED_MUTATORS.remove(id);
        }
      }
    };
    ACTIVE_BUFFERED_MUTATORS.put(id, bigtableBufferedMutator);
    return bigtableBufferedMutator;
  }

  private HBaseRequestAdapter createAdapter(TableName tableName) {
    return new HBaseRequestAdapter(options.getClusterName(), tableName, conf);
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    // HBase uses 2MB as the write buffer size.  Their limit is the size to reach before performing
    // a batch async write RPC.  Our limit is a memory cap for async RPC after which we throttle.
    // HBase does not throttle like we do.
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

    RegionLocator newLocator =
        new BigtableRegionLocator(tableName, options, session.getDataClient());

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
  public void abort(final String msg, Throwable t) {
    if (t != null) {
      LOG.fatal(msg, t);
    } else {
      LOG.fatal(msg);
    }
    this.aborted = true;
    try {
      close();
    } catch(IOException e) {
      throw new RuntimeException("Could not close the connection", e);
    }
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
  public void close() throws IOException{
    if (!this.closed) {
      this.session.close();
      // If the clients are shutdown, there shouldn't be any more activity on the
      // batch pool (assuming we created it ourselves). If exceptions were raised
      // shutting down the clients, it's not entirely safe to shutdown the pool
      // (via a finally block).
      shutdownBatchPool();
      if (this.bufferedMutatorExecutorService != null) {
        this.bufferedMutatorExecutorService.shutdown();
        this.bufferedMutatorExecutorService = null;
      }
      this.closed = true;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(AbstractBigtableConnection.class)
      .add("zone", options.getZoneId())
      .add("project", options.getProjectId())
      .add("cluster", options.getClusterId())
      .add("dataHost", options.getDataHost())
      .add("tableAdminHost", options.getTableAdminHost())
      .toString();
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

  @Override
  public abstract Admin getAdmin() throws IOException;

  /* Methods needed to construct a Bigtable Admin implementation: */
  protected BigtableTableAdminClient getBigtableTableAdminClient() throws IOException {
    return session.getTableAdminClient();
  }

  protected BigtableOptions getOptions() {
    return options;
  }

  protected Set<TableName> getDisabledTables() {
    return disabledTables;
  }

  public BigtableSession getSession() {
    return session;
  }
}
