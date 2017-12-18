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

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;

/**
 * Bigtable implementation of {@link AsyncRegistry}}. Hard codes required values as corresponding Bigtable
 * components do not exist
 * 
 * @author spollapally
 */
public class BigtableAsyncRegistry implements AsyncRegistry {

  public BigtableAsyncRegistry(Configuration conf) {}

  @Override
  public void close() {}

  /**
   * getClusterId() is required for creating and asyncConnection successfully. see
   * {@link ConnectionFactory#createAsyncConnection()}
   */
  @Override
  public CompletableFuture<String> getClusterId() {
    return CompletableFuture.completedFuture("TestClusterID");
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    throw new UnsupportedOperationException("getCurrentNrHRS");
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    throw new UnsupportedOperationException("getMasterAddress");
  }

  @Override
  public CompletableFuture<Integer> getMasterInfoPort() {
    throw new UnsupportedOperationException("getMasterInfoPort");
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    throw new UnsupportedOperationException("getMetaRegionLocation");
  }

}
