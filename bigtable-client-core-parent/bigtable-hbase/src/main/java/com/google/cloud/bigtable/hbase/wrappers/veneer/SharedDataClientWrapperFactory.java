/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.FixedWatchdogProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.common.base.Objects;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages shared resources between multiple clients.
 *
 * <p>This class is meant to support channel pool caching feature.
 */
class SharedDataClientWrapperFactory {
  private final Map<Key, ClientContext> cachedContexts = new HashMap<>();
  private final Map<Key, Integer> refCounts = new HashMap<>();

  private final Map<Key, Integer> channelPoolSizes = new HashMap<>();

  synchronized DataClientWrapper createDataClient(BigtableHBaseVeneerSettings settings)
      throws IOException {

    Key key = Key.createFromSettings(settings.getDataSettings());

    // Get or create ClientContext that will contained the shared resources
    ClientContext sharedCtx = cachedContexts.get(key);
    if (sharedCtx == null) {
      EnhancedBigtableStubSettings stubSettings = settings.getDataSettings().getStubSettings();
      sharedCtx = ClientContext.create(stubSettings);
      cachedContexts.put(key, sharedCtx);
      refCounts.put(key, 0);
      int channelPoolSize = BigtableVeneerApi.getChannelPoolSize(stubSettings);
      for (int i = 0; i < channelPoolSize; i++) {
        BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active").inc();
      }
      channelPoolSizes.put(key, channelPoolSize);
    }
    // Increment the count
    refCounts.put(key, refCounts.get(key) + 1);

    try {
      // Patch settings to use shared resources
      Builder builder = settings.getDataSettings().toBuilder();
      builder
          .stubSettings()
          .setRefreshingChannel(false)
          .setTransportChannelProvider(
              FixedTransportChannelProvider.create(sharedCtx.getTransportChannel()))
          .setCredentialsProvider(FixedCredentialsProvider.create(sharedCtx.getCredentials()))
          .setExecutorProvider(FixedExecutorProvider.create(sharedCtx.getExecutor()))
          .setStreamWatchdogProvider(FixedWatchdogProvider.create(sharedCtx.getStreamWatchdog()))
          .setHeaderProvider(FixedHeaderProvider.create(sharedCtx.getHeaders()))
          .setClock(sharedCtx.getClock());

      BigtableDataSettings data = builder.build();
      // Create a reference counted client wrapper
      return new SharedDataClientWrapper(
          this,
          key,
          new DataClientVeneerApi(BigtableDataClient.create(data), settings.getClientTimeouts()));
    } catch (IOException | RuntimeException e) {
      release(key);
      throw e;
    }
  }

  synchronized void release(Key key) {
    int refCount = refCounts.get(key);
    if (--refCount > 0) {
      refCounts.put(key, refCount);
      return;
    }

    refCounts.remove(key);
    ClientContext clientContext = cachedContexts.remove(key);
    for (int i = 0; i < channelPoolSizes.get(key); i++) {
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active").dec();
    }
    channelPoolSizes.remove(key);
    for (BackgroundResource resource : clientContext.getBackgroundResources()) {
      resource.shutdown();
    }
  }

  /**
   * Identity for a shared {@link ClientContext}.
   *
   * <p>This value class contains primary identifying information for a ClientContext. It can be
   * extracted from {@link BigtableDataSettings} and be used to check if a ClientContext would be
   * compatible with a ClientContext required by {@link BigtableDataSettings}.
   */
  static final class Key {
    private final String endpoint;
    private final Map<String, String> headers;
    private final CredentialsProvider credentialsProvider;

    static Key createFromSettings(BigtableDataSettings settings) {
      EnhancedBigtableStubSettings stubSettings = settings.getStubSettings();

      return new Key(
          stubSettings.getEndpoint(),
          stubSettings.getHeaderProvider().getHeaders(),
          stubSettings.getCredentialsProvider());
    }

    private Key(
        String endpoint, Map<String, String> headers, CredentialsProvider credentialsProvider) {
      this.endpoint = endpoint;
      this.headers = headers;
      this.credentialsProvider = credentialsProvider;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return Objects.equal(endpoint, key.endpoint)
          && Objects.equal(headers, key.headers)
          && Objects.equal(credentialsProvider, key.credentialsProvider);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(endpoint, headers, credentialsProvider);
    }
  }
}
