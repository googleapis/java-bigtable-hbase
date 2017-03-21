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
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.cloud.bigtable.hbase.BigtableTable;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter.MutationAdapters;
import com.google.common.base.MoreObjects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
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

/**
 * <p>Abstract AbstractBigtableConnection class.</p>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public abstract class AbstractBigtableConnection implements Connection, Closeable {
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

    // Force the loading of HConstants.class, which shares a bi-directional reference with KeyValue.
    // This bi-drectional relationship causes problems when KeyValue and HConstants are class loaded
    // in different threads. This forces a clean class loading of both HConstants and KeyValue along
    // with a whole bunch of other classes.
    Adapters.class.getName();
  }

  private final Logger LOG = new Logger(getClass());

  private final Set<RegionLocator> locatorCache = new CopyOnWriteArraySet<>();

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
  private MutationAdapters mutationAdapters;

  /**
   * <p>Constructor for AbstractBigtableConnection.</p>
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object.
   * @throws java.io.IOException if any.
   */
  public AbstractBigtableConnection(Configuration conf) throws IOException {
    this(conf, false, null, null);
  }

  /**
   * The constructor called from {@link org.apache.hadoop.hbase.client.ConnectionFactory#createConnection(Configuration)} and
   * in its many forms via reflection with this specific signature.
   *
   * @param conf The configuration for this channel. See {@link com.google.cloud.bigtable.hbase.BigtableOptionsFactory} for more details.
   * @param managed This should always be false. It's an artifact of old HBase behavior.
   * @param pool An {@link java.util.concurrent.ExecutorService} to run HBase/Bigtable object conversions on. The RPCs
   *             themselves run via NIO, and not on a waiting thread
   * @param user This is an artifact of HBase which Cloud Bigtable ignores. User information is
   * captured in the Credentials configuration in conf.
   * @throws java.io.IOException if the setup is not correct. The most likely issue is ALPN or OpenSSL
   * misconfiguration.
   */
  protected AbstractBigtableConnection(Configuration conf, boolean managed, ExecutorService pool,
      User user) throws IOException {
    if (managed) {
      throw new IllegalArgumentException("Bigtable does not support managed connections.");
    }
    this.conf = conf;

    BigtableOptions opts;
    try {
      opts = BigtableOptionsFactory.fromConfiguration(conf);
    } catch (IOException ioe) {
      LOG.error("Error loading BigtableOptions from Configuration.", ioe);
      throw ioe;
    }

    this.batchPool = pool;
    this.closed = false;
    this.session = new BigtableSession(opts);

    // Note: Reset options here because BigtableSession could potentially modify the input
    // options by resolving legacy parameters into current ones.
    this.options = this.session.getOptions();
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(TableName tableName) throws IOException {
    return getTable(tableName, batchPool);
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
    return new BigtableTable(this, createAdapter(tableName));
  }

  /** {@inheritDoc} */
  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    TableName tableName = params.getTableName();
    if (tableName == null) {
      throw new IllegalArgumentException("TableName cannot be null.");
    }

    final long id = SEQUENCE_GENERATOR.incrementAndGet();

    ExecutorService pool = batchPool != null ? batchPool
        : BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool();
    HBaseRequestAdapter adapter = createAdapter(tableName);
    ExceptionListener listener = params.getListener();
    BigtableBufferedMutator bigtableBufferedMutator =
        new BigtableBufferedMutator(adapter, conf, session, listener, pool) {
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

  public HBaseRequestAdapter createAdapter(TableName tableName) {
    if (mutationAdapters == null) {
      synchronized(this) {
        if (mutationAdapters == null) {
          mutationAdapters = new HBaseRequestAdapter.MutationAdapters(options, conf);
        }
      }
    }
    return new HBaseRequestAdapter(options, tableName, mutationAdapters);
  }

  /** {@inheritDoc} */
  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    // HBase uses 2MB as the write buffer size.  Their limit is the size to reach before performing
    // a batch async write RPC.  Our limit is a memory cap for async RPC after which we throttle.
    // HBase does not throttle like we do.
    return getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  /**
   * This should not be used.  The hbase shell needs this in hbase 0.99.2. Remove this once
   * 1.0.0 comes out.
   *
   * @param tableName a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.hbase.client.Table} object.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public Table getTable(String tableName) throws IOException {
    return getTable(TableName.valueOf(tableName));
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  @Override
  public boolean isClosed() {
    return this.closed;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(AbstractBigtableConnection.class)
      .add("project", options.getProjectId())
      .add("instance", options.getInstanceId())
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
        Thread.currentThread().interrupt();
        this.batchPool.shutdownNow();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public abstract Admin getAdmin() throws IOException;

  /* Methods needed to construct a Bigtable Admin implementation: */
  /**
   * <p>getBigtableTableAdminClient.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableTableAdminClient} object.
   * @throws java.io.IOException if any.
   */
  protected BigtableTableAdminClient getBigtableTableAdminClient() throws IOException {
    return session.getTableAdminClient();
  }

  /**
   * <p>Getter for the field <code>options</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  protected BigtableOptions getOptions() {
    return options;
  }

  /**
   * <p>Getter for the field <code>disabledTables</code>.</p>
   *
   * @return a {@link java.util.Set} object.
   */
  protected Set<TableName> getDisabledTables() {
    return disabledTables;
  }

  /**
   * <p>Getter for the field <code>session</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableSession} object.
   */
  public BigtableSession getSession() {
    return session;
  }
}
