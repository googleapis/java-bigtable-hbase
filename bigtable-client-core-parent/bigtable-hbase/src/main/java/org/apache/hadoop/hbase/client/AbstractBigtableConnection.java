/*
 * Copyright 2015 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutator;
import com.google.cloud.bigtable.hbase.BigtableHBaseVersion;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter.MutationAdapters;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.base.MoreObjects;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.security.User;

/**
 * Abstract AbstractBigtableConnection class.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class AbstractBigtableConnection
    implements Connection, CommonConnection, Closeable {

  static {

    // Force the loading of HConstants.class, which shares a bi-directional reference with KeyValue.
    // This bi-directional relationship causes problems when KeyValue and HConstants are class
    // loaded in different threads. This forces a clean class loading of both HConstants and
    // KeyValue along with a whole bunch of other classes.
    Adapters.class.getName();
  }

  private final Logger LOG = new Logger(getClass());

  protected final Set<RegionLocator> locatorCache = new CopyOnWriteArraySet<>();

  private volatile boolean closed = false;
  private volatile boolean aborted;
  private volatile ExecutorService batchPool = null;
  private ExecutorService bufferedMutatorExecutorService;

  private final BigtableApi bigtableApi;

  private volatile boolean cleanupPool = false;
  private final BigtableHBaseSettings settings;

  // A set of tables that have been disabled via BigtableAdmin.
  private Set<TableName> disabledTables = new HashSet<>();
  private MutationAdapters mutationAdapters;

  /**
   * Constructor for AbstractBigtableConnection.
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object.
   * @throws java.io.IOException if any.
   */
  public AbstractBigtableConnection(Configuration conf) throws IOException {
    this(conf, false, null, null);
  }

  /**
   * The constructor called from {@link
   * org.apache.hadoop.hbase.client.ConnectionFactory#createConnection(Configuration)} and in its
   * many forms via reflection with this specific signature.
   *
   * @param conf The configuration for this channel. See {@link
   *     com.google.cloud.bigtable.hbase.BigtableOptionsFactory} for more details.
   * @param managed This should always be false. It's an artifact of old HBase behavior.
   * @param pool An {@link java.util.concurrent.ExecutorService} to run HBase/Bigtable object
   *     conversions on. The RPCs themselves run via NIO, and not on a waiting thread
   * @param user This is an artifact of HBase which Cloud Bigtable ignores. User information is
   *     captured in the Credentials configuration in conf.
   * @throws java.io.IOException if the setup is not correct. The most likely issue is ALPN or
   *     OpenSSL misconfiguration.
   */
  protected AbstractBigtableConnection(
      Configuration conf, boolean managed, ExecutorService pool, User user) throws IOException {
    if (managed) {
      if (conf.getBoolean(BigtableOptionsFactory.MANAGED_CONNECTION_WARNING, false)) {
        LOG.warn(
            "Bigtable does not support managed connections. This connection will end up leaking.");
      } else {
        throw new IllegalArgumentException("Bigtable does not support managed connections.");
      }
    }

    try {
      this.settings = BigtableHBaseSettings.create(conf);
    } catch (IOException ioe) {
      LOG.error("Error loading BigtableOptions from Configuration.", ioe);
      throw ioe;
    }

    logStartup(LOG, conf, settings);

    this.batchPool = pool;
    this.closed = false;
    this.bigtableApi = BigtableApi.create(settings);
  }

  private static final AtomicBoolean firstConnection = new AtomicBoolean();

  private void logStartup(Logger logger, Configuration userConfig, BigtableHBaseSettings settings) {
    if (firstConnection.compareAndSet(false, true)) {
      String jarPath;
      try {
        jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().toString();
      } catch (RuntimeException e) {
        jarPath = "<unknown>";
      }

      logger.info(
          "Using bigtable-hbase client from jar %s. Version: %s",
          jarPath, BigtableHBaseVersion.getVersion());
    }
    // Dump user configuration
    MoreObjects.ToStringHelper configHelper = MoreObjects.toStringHelper("BigtableConfiguration");
    for (Map.Entry<String, String> entry : userConfig) {
      if (!entry.getKey().startsWith("google.bigtable")) {
        continue;
      }
      configHelper.add(entry.getKey(), entry.getValue());
    }
    logger.info(
        "User Configuration:\n" + ToStringHelperPrettyPrinter.print(configHelper.toString()));
    settings
        .toDebugStrings()
        .forEach(
            (k, v) ->
                logger.info(
                    "Effective settings: BigtableHBaseVeneerSettings\n"
                        + ToStringHelperPrettyPrinter.print(v)));
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return this.settings.getConfiguration();
  }

  /** {@inheritDoc} */
  @Override
  public Table getTable(TableName tableName) throws IOException {
    return getTable(tableName, batchPool);
  }

  /** {@inheritDoc} */
  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    TableName tableName = params.getTableName();
    if (tableName == null) {
      throw new IllegalArgumentException("TableName cannot be null.");
    }

    HBaseRequestAdapter adapter = createAdapter(tableName);
    ExceptionListener listener = params.getListener();
    return new BigtableBufferedMutator(bigtableApi, settings, adapter, listener);
  }

  public HBaseRequestAdapter createAdapter(TableName tableName) {
    if (mutationAdapters == null) {
      synchronized (this) {
        if (mutationAdapters == null) {
          mutationAdapters = new HBaseRequestAdapter.MutationAdapters(settings);
        }
      }
    }
    return new HBaseRequestAdapter(settings, tableName, mutationAdapters);
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
   * This should not be used. The hbase shell needs this in hbase 0.99.2. Remove this once 1.0.0
   * comes out.
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
    RegionLocator locator = getCachedLocator(tableName);

    if (locator == null) {
      locator =
          new BigtableRegionLocator(tableName, settings, bigtableApi.getDataClient()) {

            @Override
            public SampledRowKeysAdapter getSampledRowKeysAdapter(
                TableName tableName, ServerName serverName) {
              return createSampledRowKeysAdapter(tableName, serverName);
            }
          };

      locatorCache.add(locator);
    }
    return locator;
  }

  private RegionLocator getCachedLocator(TableName tableName) {
    for (RegionLocator locator : locatorCache) {
      if (locator.getName().equals(tableName)) {
        return locator;
      }
    }

    return null;
  }

  /**
   * There are some hbase 1.x and 2.x incompatibilities which require this abstract method. See
   * {@link SampledRowKeysAdapter} for more details.
   *
   * @param tableName a {@link TableName} object.
   * @param serverName a {@link ServerName} object.
   * @return a {@link SampledRowKeysAdapter} object.
   */
  protected abstract SampledRowKeysAdapter createSampledRowKeysAdapter(
      TableName tableName, ServerName serverName);

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
    } catch (IOException e) {
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
  public void close() throws IOException {
    if (!this.closed) {
      this.bigtableApi.close();
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
        .add("project", settings.getProjectId())
        .add("instance", settings.getInstanceId())
        .add("dataHost", settings.getDataHost())
        .add("tableAdminHost", settings.getAdminHost())
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

  @Override
  public BigtableHBaseSettings getBigtableSettings() {
    return this.settings;
  }

  /**
   * Getter for the field <code>disabledTables</code>.
   *
   * @return a {@link java.util.Set} object.
   */
  public Set<TableName> getDisabledTables() {
    return disabledTables;
  }

  public BigtableApi getBigtableApi() {
    return bigtableApi;
  }

  // A helper function to parse the pretty print the string from ToStringHelper.
  private static class ToStringHelperPrettyPrinter {
    // A state machine to parse the string.
    enum State {
      DEFAULT, // Print special symbols, new lines, indentation and omit spaces.
      IN_PHRASE, // Print verbatim for strings, including spaces, e.g. "Hello World".
      IN_SET, // Print verbatim for sets, including commas and spaces, e.g. "[foo, bar]".
    }

    private static void appendIndent(StringBuilder sb, int level) {
      for (int i = 0; i < level; i++) {
        sb.append("  ");
      }
    }

    private static String print(String str) {
      StringBuilder sb = new StringBuilder();
      int indent = 0;
      State state = State.DEFAULT;
      for (char c : str.toCharArray()) {
        switch (c) {
          case '{':
            state = State.DEFAULT;
            sb.append(" {\n");
            appendIndent(sb, ++indent);
            break;
          case '=':
            state = State.DEFAULT;
            sb.append(" = ");
            break;
          case ',':
            if (state == State.IN_SET) {
              sb.append(',');
              break;
            }
            state = State.DEFAULT;
            sb.append(",\n");
            appendIndent(sb, indent);
            break;
          case '}':
            state = State.DEFAULT;
            sb.append("\n");
            appendIndent(sb, --indent);
            sb.append("}");
            break;
          case ' ':
            if (state == State.IN_PHRASE || state == State.IN_SET) {
              sb.append(" ");
            }
            break;
          case '[':
            sb.append('[');
            state = State.IN_SET;
            break;
          case ']':
            sb.append(']');
            state = State.DEFAULT;
            break;
          default:
            if (state != State.IN_SET) {
              state = State.IN_PHRASE;
            }
            sb.append(c);
        }
      }
      return sb.toString();
    }
  }
}
