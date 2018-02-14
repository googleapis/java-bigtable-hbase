/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter.MutationAdapters;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncAdmin;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncBufferedMutator;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTable;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTableRegionLocator;

/**
 * Bigtable implementation of {@link AsyncConnection}
 *
 * @author spollapally
 */
public class BigtableAsyncConnection implements AsyncConnection, Closeable {
  private final Logger LOG = new Logger(getClass());

  private final Configuration conf;
  private BigtableSession session;
  private BigtableOptions options;
  private volatile boolean closed = false;

  private Set<TableName> disabledTables = new HashSet<>();
  private MutationAdapters mutationAdapters;

  static {
    // This forces a clean class loading of both HConstants and KeyValue along
    // with a whole bunch of other classes.
    Adapters.class.getName();
  }

  public BigtableAsyncConnection(Configuration conf) throws IOException {
    this(conf, null, null, null);
  }

  public BigtableAsyncConnection(Configuration conf, AsyncRegistry ignoredRegistry,
      String ignoredClusterId, User ignoredUser) throws IOException {
    LOG.debug("Creating BigtableAsyncConnection");
    this.conf = conf;

    BigtableOptions opts;
    try {
      opts = BigtableOptionsFactory.fromConfiguration(conf);
    } catch (IOException ioe) {
      LOG.error("Error loading BigtableOptions from Configuration.", ioe);
      throw ioe;
    }

    this.closed = false;
    this.session = new BigtableSession(opts);
    this.options = this.session.getOptions();
  }

  public HBaseRequestAdapter createAdapter(TableName tableName) {
    if (mutationAdapters == null) {
      synchronized (this) {
        if (mutationAdapters == null) {
          mutationAdapters = new HBaseRequestAdapter.MutationAdapters(options, conf);
        }
      }
    }
    return new HBaseRequestAdapter(options, tableName, mutationAdapters);
  }

  public BigtableSession getSession() {
    return this.session;
  }

  public BigtableOptions getOptions() {
    return this.options;
  }

  public Set<TableName> getDisabledTables() {
    return disabledTables;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closeing BigtableAsyncConnection");
    if (!this.closed) {
      this.session.close();
      this.closed = true;
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    return new AsyncAdminBuilder() {

      @Override
      public AsyncAdminBuilder setStartLogErrorsCnt(int arg0) {
        return this;
      }

      @Override
      public AsyncAdminBuilder setRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncAdminBuilder setRetryPause(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncAdminBuilder setOperationTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncAdminBuilder setMaxAttempts(int arg0) {
        return this;
      }

      @Override
      public AsyncAdmin build() {
        try {
          return new BigtableAsyncAdmin(BigtableAsyncConnection.this);
        } catch (IOException e) {
          LOG.error("failed to build BigtableAsyncAdmin", e);
          throw new UncheckedIOException("failed to build BigtableAsyncAdmin", e);
        }
      }
    };
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService arg0) {
    return getAdminBuilder();
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(final TableName tableName) {
    return new AsyncBufferedMutatorBuilder() {

      @Override
      public AsyncBufferedMutatorBuilder setWriteBufferSize(long arg0) {
        return this;
      }

      @Override
      public AsyncBufferedMutatorBuilder setStartLogErrorsCnt(int arg0) {
        return this;
      }

      @Override
      public AsyncBufferedMutatorBuilder setRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncBufferedMutatorBuilder setRetryPause(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncBufferedMutatorBuilder setOperationTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncBufferedMutatorBuilder setMaxAttempts(int arg0) {
        return this;
      }

      @Override
      public AsyncBufferedMutator build() {
        return new BigtableAsyncBufferedMutator();
      }
    };
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName,
      ExecutorService es) {
    return getBufferedMutatorBuilder(tableName);
  }

  @Override
  public AsyncTableBuilder<RawAsyncTable> getRawTableBuilder(TableName tableName) {
    return new AsyncTableBuilder<RawAsyncTable>() {
      @Override
      public RawAsyncTable build() {
      throw new UnsupportedOperationException("getRawTableBuilder"); // TODO
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setMaxAttempts(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setOperationTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setReadRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setRetryPause(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setScanTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setStartLogErrorsCnt(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<RawAsyncTable> setWriteRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }
    };
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return new BigtableAsyncTableRegionLocator(tableName, options, this.session.getDataClient());
  }

  @Override
  public AsyncTableBuilder<AsyncTable> getTableBuilder(TableName tableName,
      final ExecutorService executorService) {
    return new AsyncTableBuilder<AsyncTable>() {
      @Override
      public AsyncTable build() {
        return new BigtableAsyncTable(BigtableAsyncConnection.this, createAdapter(tableName),
            executorService);
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setMaxAttempts(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setOperationTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setReadRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setRetryPause(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setScanTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setStartLogErrorsCnt(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AsyncTable> setWriteRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }
    };
  }
}
