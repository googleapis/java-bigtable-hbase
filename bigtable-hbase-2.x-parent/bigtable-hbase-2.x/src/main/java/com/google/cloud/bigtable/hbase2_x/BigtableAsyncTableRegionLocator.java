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
package com.google.cloud.bigtable.hbase2_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.AbstractBigtableRegionLocator;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Bigtable implementation of {@link AsyncTableRegionLocator}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAsyncTableRegionLocator extends AbstractBigtableRegionLocator
    implements AsyncTableRegionLocator {

  public BigtableAsyncTableRegionLocator(
      TableName tableName, BigtableHBaseSettings settings, DataClientWrapper client) {
    super(tableName, settings, client);
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, boolean reload) {
    return ApiFutureUtils.toCompletableFuture(getRegionsAsync(reload))
        .thenApplyAsync(result -> findRegion(result, row));
  }

  private HRegionLocation findRegion(List<HRegionLocation> regions, byte[] row) {
    int low = 0;
    int high = regions.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      HRegionLocation regionLocation = regions.get(mid);
      HRegionInfo regionInfo = regionLocation.getRegionInfo();

      // This isn't the last region (endKey != "") and row key is greater than the current bound
      if (regionInfo.getEndKey().length > 0 && Bytes.compareTo(row, regionInfo.getEndKey()) >= 0) {
        low = mid + 1;
      } else if (Bytes.compareTo(row, regionInfo.getStartKey()) < 0) {
        high = mid - 1;
      } else {
        return regionLocation;
      }
    }
    return null;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(
      byte[] row, int replicaId, boolean reload) {
    return getRegionLocation(row, reload);
  }

  @Override
  public CompletableFuture<List<HRegionLocation>> getRegionLocations(byte[] row, boolean reload) {
    throw new UnsupportedOperationException("getRegionLocations"); // TODO
  }

  @Override
  public CompletableFuture<List<HRegionLocation>> getAllRegionLocations() {
    throw new UnsupportedOperationException("getAllRegionLocations"); // TODO
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException("clearRegionLocationCache");
  }

  @Override
  public SampledRowKeysAdapter getSampledRowKeysAdapter(
      TableName tableName, ServerName serverName) {
    return new SampledRowKeysAdapter(tableName, serverName) {
      @Override
      protected HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey) {
        RegionInfo regionInfo =
            RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
        return new HRegionLocation(regionInfo, serverName);
      }
    };
  }
}
