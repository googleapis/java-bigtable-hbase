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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.ClientContext;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClientFactory;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
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
  private final Map<Key, BigtableDataClientFactory> cachedContexts = new HashMap<>();
  private final Map<Key, Integer> refCounts = new HashMap<>();

  private final Map<Key, Integer> channelPoolSizes = new HashMap<>();

  synchronized DataClientWrapper createDataClient(BigtableHBaseVeneerSettings settings)
      throws IOException {

    Key key = Key.createFromSettings(settings.getDataSettings());

    // Get or create ClientContext that will contained the shared resources
    BigtableDataClientFactory sharedCtx = cachedContexts.get(key);

    if (sharedCtx == null) {
      sharedCtx = BigtableDataClientFactory.create(settings.getDataSettings());
      cachedContexts.put(key, sharedCtx);
      refCounts.put(key, 0);
      int channelPoolSize =
          BigtableVeneerApi.getChannelPoolSize(settings.getDataSettings().getStubSettings());
      for (int i = 0; i < channelPoolSize; i++) {
        BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active").inc();
      }
      channelPoolSizes.put(key, channelPoolSize);
    }
    // Increment the count
    refCounts.put(key, refCounts.get(key) + 1);

    try {
      final BigtableDataClient client;
      if (settings.getAppProfileId() == null) {
        client = sharedCtx.createForInstance(settings.getProjectId(), settings.getInstanceId());
      } else {
        client =
            sharedCtx.createForInstance(
                settings.getProjectId(), settings.getInstanceId(), settings.getAppProfileId());
      }
      // Create a reference counted client wrapper
      return new SharedDataClientWrapper(
          this, key, new DataClientVeneerApi(client, settings.getClientTimeouts()));
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
    BigtableDataClientFactory clientContext = cachedContexts.remove(key);
    for (int i = 0; i < channelPoolSizes.get(key); i++) {
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active").dec();
    }
    channelPoolSizes.remove(key);
    try {
      clientContext.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
    private static final NoCredentialsProvider NO_CREDENTIALS_PROVIDER_INSTANCE =
        NoCredentialsProvider.create();
    private final String endpoint;
    private final CredentialsProvider credentialsProvider;

    static Key createFromSettings(BigtableDataSettings settings) {
      EnhancedBigtableStubSettings stubSettings = settings.getStubSettings();
      CredentialsProvider effectiveCredProvider = stubSettings.getCredentialsProvider();
      // NoCredentialsProvider doesnt implement equals, but all instances are equivalent
      if (effectiveCredProvider instanceof NoCredentialsProvider) {
        effectiveCredProvider = NO_CREDENTIALS_PROVIDER_INSTANCE;
      }

      return new Key(stubSettings.getEndpoint(), effectiveCredProvider);
    }

    private Key(String endpoint, CredentialsProvider credentialsProvider) {
      this.endpoint = endpoint;
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
          && Objects.equal(credentialsProvider, key.credentialsProvider);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(endpoint, credentialsProvider);
    }
  }
}
