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

  public BigtableAsyncConnection(Configuration conf, AsyncRegistry registry, String clusterId,
      User user) throws IOException {
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
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setRpcTimeout(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setRetryPause(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setOperationTimeout(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setMaxAttempts(int arg0) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
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
    throw new UnsupportedOperationException("getAdminBuilder ExecutorService"); // TODO
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName arg0) {
    throw new UnsupportedOperationException("getBufferedMutatorBuilder"); // TODO
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName arg0,
      ExecutorService arg1) {
    throw new UnsupportedOperationException("getBufferedMutatorBuilder"); // TODO
  }

  @Override
  public AsyncTableBuilder<RawAsyncTable> getRawTableBuilder(TableName arg0) {
    throw new UnsupportedOperationException("getRawTableBuilder"); // TODO
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    throw new UnsupportedOperationException("getRegionLocator"); // TODO
  }

  @Override
  public AsyncTableBuilder<AsyncTable> getTableBuilder(TableName tableName,
      ExecutorService executorService) {
    throw new UnsupportedOperationException("getTableBuilder"); // TODO
  }
}
