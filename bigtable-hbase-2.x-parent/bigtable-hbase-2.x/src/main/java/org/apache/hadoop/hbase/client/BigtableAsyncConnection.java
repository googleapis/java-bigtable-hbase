/*
 * Copyright 2017 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter.MutationAdapters;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.bigtable.hbase.util.FutureUtil;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncAdmin;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncBufferedMutator;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTable;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTableRegionLocator;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;

/**
 * Bigtable implementation of {@link AsyncConnection}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAsyncConnection implements AsyncConnection, CommonConnection, Closeable {
  private final Logger LOG = new Logger(getClass());

  private final BigtableApi bigtableApi;
  private final BigtableHBaseSettings settings;
  private volatile boolean closed = false;

  private final Set<TableName> disabledTables = Collections.synchronizedSet(new HashSet<>());
  private MutationAdapters mutationAdapters;

  static {
    // This forces a clean class loading of both HConstants and KeyValue along
    // with a whole bunch of other classes.
    Adapters.class.getName();
  }

  public BigtableAsyncConnection(Configuration conf) throws IOException {
    this(conf, null, null, null);
  }

  // This constructor is used in HBase 2 version < 2.3
  public BigtableAsyncConnection(
      Configuration conf, Object ignoredAsyncRegistry, String ignoredClusterId, User ignoredUser)
      throws IOException {
    LOG.debug("Creating BigtableAsyncConnection");

    try {
      settings = BigtableHBaseSettings.create(conf);
    } catch (IOException ioe) {
      LOG.error("Error loading BigtableOptions from Configuration.", ioe);
      throw ioe;
    }

    this.closed = false;
    this.bigtableApi = BigtableApi.create(settings);
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

  public BigtableApi getBigtableApi() {
    return bigtableApi;
  }

  @Override
  public BigtableHBaseSettings getBigtableSettings() {
    return this.settings;
  }

  public Set<TableName> getDisabledTables() {
    return disabledTables;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing BigtableAsyncConnection");
    if (!this.closed) {
      this.bigtableApi.close();
      this.closed = true;
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.settings.getConfiguration();
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
      public AsyncAdminBuilder setRetryPauseForCQTBE(long l, TimeUnit timeUnit) {
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
      public AsyncAdminBuilder setRetryPauseForServerOverloaded(long l, TimeUnit timeUnit) {
        return this;
      }

      @Override
      public AsyncAdmin build() {
        try {
          return BigtableAsyncAdmin.createInstance(BigtableAsyncConnection.this);
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
      public AsyncBufferedMutatorBuilder setMaxKeyValueSize(int i) {
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
        return new BigtableAsyncBufferedMutator(bigtableApi, settings, createAdapter(tableName));
      }
    };
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(
      TableName tableName, ExecutorService es) {
    return getBufferedMutatorBuilder(tableName);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    return new AsyncTableBuilder<AdvancedScanResultConsumer>() {

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setWriteRpcTimeout(
          long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setStartLogErrorsCnt(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setScanTimeout(
          long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setRetryPause(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setRetryPauseForCQTBE(
          long l, TimeUnit timeUnit) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setReadRpcTimeout(
          long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setOperationTimeout(
          long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setMaxAttempts(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<AdvancedScanResultConsumer> setRetryPauseForServerOverloaded(
          long l, TimeUnit timeUnit) {
        return this;
      }

      @Override
      public AsyncTable build() {
        return new BigtableAsyncTable(BigtableAsyncConnection.this, createAdapter(tableName));
      }
    };
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return new BigtableAsyncTableRegionLocator(tableName, settings, bigtableApi.getDataClient());
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException("clearRegionLocationCache is not supported.");
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(
      TableName tableName, final ExecutorService ignored) {
    return new AsyncTableBuilder<ScanResultConsumer>() {
      @Override
      public AsyncTable build() {
        return new BigtableAsyncTable(BigtableAsyncConnection.this, createAdapter(tableName));
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setMaxAttempts(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setOperationTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setReadRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setRetryPause(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setRetryPauseForCQTBE(
          long l, TimeUnit timeUnit) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setScanTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setStartLogErrorsCnt(int arg0) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setWriteRpcTimeout(long arg0, TimeUnit arg1) {
        return this;
      }

      @Override
      public AsyncTableBuilder<ScanResultConsumer> setRetryPauseForServerOverloaded(
          long l, TimeUnit timeUnit) {
        return this;
      }
    };
  }

  @Override
  public List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException {
    ServerName serverName = ServerName.valueOf(settings.getDataHost(), settings.getPort(), 0);
    SampleRowKeysRequest.Builder request = SampleRowKeysRequest.newBuilder();
    request.setTableName(
        NameUtil.formatTableName(
            settings.getProjectId(), settings.getInstanceId(), tableName.getNameAsString()));
    List<KeyOffset> sampleRowKeyResponse =
        FutureUtil.unwrap(
            this.bigtableApi.getDataClient().sampleRowKeysAsync(tableName.getNameAsString()));

    return getSampledRowKeysAdapter(tableName, serverName)
        .adaptResponse(sampleRowKeyResponse)
        .stream()
        .map(HRegionLocation::getRegionInfo)
        .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
  }

  @Override
  public Hbck getHbck(ServerName serverName) {
    throw new UnsupportedOperationException("getHbck is not supported.");
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    throw new UnsupportedOperationException("getHbck is not supported.");
  }

  private SampledRowKeysAdapter getSampledRowKeysAdapter(
      TableName tableNameAdapter, ServerName serverNameAdapter) {
    return new SampledRowKeysAdapter(tableNameAdapter, serverNameAdapter) {
      @Override
      protected HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey) {
        RegionInfo regionInfo =
            RegionInfoBuilder.newBuilder(tableNameAdapter)
                .setStartKey(startKey)
                .setEndKey(endKey)
                .build();
        return new HRegionLocation(regionInfo, serverNameAdapter);
      }
    };
  }
}
