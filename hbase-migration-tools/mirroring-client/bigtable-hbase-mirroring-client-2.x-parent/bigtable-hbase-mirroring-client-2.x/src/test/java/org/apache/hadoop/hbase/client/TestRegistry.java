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
package org.apache.hadoop.hbase.client;

/**
 * It's necessary for unit testing of {@link
 * com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection} as {@link
 * org.apache.hadoop.hbase.client.ConnectionFactory#createAsyncConnection()} checks cluster id
 * during {@link org.apache.hadoop.hbase.client.AsyncConnection} creation.
 *
 * <p>AsyncRegistry is private in org.apache.hadoop.hbase.client so this mock must be in the same
 * package.
 */
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;

// TODO (Vermas2012) The upstream AsyncRegistry is deleted in latest hbase version (2.3+), we need
// to understand if we need this registry now. There is just 1 test depending on it.
public class TestRegistry {
  TestRegistry(Configuration conf) {}

  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<String> getClusterId() {
    CompletableFuture<String> future = new CompletableFuture();
    future.complete("clusterId");
    return future;
  }

  public CompletableFuture<ServerName> getMasterAddress() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    throw new UnsupportedOperationException();
  }
}
