/*
 * Copyright 2023 Google LLC
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

/** Implementation for `hbase.client.registry.impl` for HBase >= 2.3 */
@InternalApi
public class BigtableConnectionRegistry implements ConnectionRegistry {
  public BigtableConnectionRegistry() {
    // noop
  }

  public BigtableConnectionRegistry(Configuration ignored) {
    // noop
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocations() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return CompletableFuture.completedFuture("NoopClusterId");
  }

  @Override
  public CompletableFuture<ServerName> getActiveMaster() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // noop
  }
}
