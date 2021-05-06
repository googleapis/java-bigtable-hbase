/*
 * Copyright 2020 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Counter;
import java.io.IOException;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableVeneerApi extends BigtableApi {

  private final Counter activeSessions =
      BigtableClientMetrics.counter(MetricLevel.Info, "session.active");
  private final Counter activeChannels =
      BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active");

  private static final SharedDataClientWrapperFactory sharedClientFactory =
      new SharedDataClientWrapperFactory();
  private final DataClientWrapper dataClientWrapper;
  private final AdminClientWrapper adminClientWrapper;
  private final int channelPoolSize;

  public BigtableVeneerApi(BigtableHBaseVeneerSettings settings) throws IOException {
    super(settings);

    if (settings.isChannelPoolCachingEnabled()) {
      dataClientWrapper = sharedClientFactory.createDataClient(settings);
    } else {
      dataClientWrapper =
          new DataClientVeneerApi(BigtableDataClient.create(settings.getDataSettings()));
    }
    BigtableInstanceAdminSettings instanceAdminSettings = settings.getInstanceAdminSettings();
    adminClientWrapper =
        new AdminClientVeneerApi(
            BigtableTableAdminClient.create(settings.getTableAdminSettings()),
            BigtableInstanceAdminClient.create(instanceAdminSettings));
    activeSessions.inc();
    // active channel count is hard coded at client creation time based on the setting. If
    // transportChannelProvider in the data setting is not InstantiatingGrpcChannelProvider, this
    // count wil not be present.
    this.channelPoolSize = getConnectionCount(settings.getDataSettings().getStubSettings());
    for (int i = 0; i < channelPoolSize; i++) {
      activeChannels.inc();
    }
  }

  @Override
  public AdminClientWrapper getAdminClient() {
    return adminClientWrapper;
  }

  @Override
  public DataClientWrapper getDataClient() {
    return dataClientWrapper;
  }

  @Override
  public void close() throws IOException {
    dataClientWrapper.close();
    adminClientWrapper.close();
    activeSessions.dec();
    for (int i = 0; i < channelPoolSize; i++) {
      activeChannels.dec();
    }
  }

  private int getConnectionCount(EnhancedBigtableStubSettings stubSettings) {
    if (stubSettings.getTransportChannelProvider() instanceof InstantiatingGrpcChannelProvider) {
      InstantiatingGrpcChannelProvider channelProvider =
          (InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider();
      return channelProvider.toBuilder().getPoolSize();
    }
    return 0;
  }
}
