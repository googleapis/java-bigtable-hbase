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

public class TestRegistry implements AsyncRegistry {
  TestRegistry(Configuration conf) {}

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    CompletableFuture<String> future = new CompletableFuture();
    future.complete("clusterId");
    return future;
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }
}
