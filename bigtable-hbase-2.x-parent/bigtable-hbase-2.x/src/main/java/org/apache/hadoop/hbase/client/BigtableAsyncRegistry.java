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
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;

/**
 * Bigtable implementation of {@link ConnectionRegistry}. The default Habse 2 implementation
 * provided by {@link ZKConnectionRegistry} assumes a ZooKeeper environment, which is not the case
 * for Bigtable.
 *
 * <p>This class is injected via the system property: "hbase.client.registry.impl" For further
 * details See {@link ConnectionRegistryFactory#REGISTRY_IMPL_CONF_KEY}, and {@link
 * ConnectionFactory#createAsyncConnection()}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
// AsyncRegistry was renamed to ConnectionRegistry in https://github.com/apache/hbase/pull/957
public class BigtableAsyncRegistry implements ConnectionRegistry {

  public BigtableAsyncRegistry(Configuration conf) {}

  @Override
  public void close() {}

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocations() {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * A non null return value is required for successful creation of asyncConnection. see {@link
   * ConnectionFactory#createAsyncConnection()}
   */
  @Override
  public CompletableFuture<String> getClusterId() {
    return CompletableFuture.completedFuture("TestClusterID");
  }

  @Override
  public CompletableFuture<ServerName> getActiveMaster() {
    throw new UnsupportedOperationException("not implemented");
  }
}
