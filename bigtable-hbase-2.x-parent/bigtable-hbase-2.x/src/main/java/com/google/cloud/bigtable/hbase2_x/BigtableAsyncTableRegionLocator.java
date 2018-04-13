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
package com.google.cloud.bigtable.hbase2_x;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.hbase.AbstractBigtbleRegionLocator;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * Bigtable implementation of {@link AsyncTableRegionLocator}
 * 
 * @author spollapally
 */
public class BigtableAsyncTableRegionLocator extends AbstractBigtbleRegionLocator implements AsyncTableRegionLocator {
  private final Logger LOG = new Logger(getClass());
  HRegionLocation hRegionLocation = null;

  public BigtableAsyncTableRegionLocator(TableName tableName, BigtableOptions options,
      BigtableDataClient client) {
    super(tableName,options,client);
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, boolean reload){
    try {
      this.regionsFuture = getRegionsAsync(reload);
    } catch (IOException e) {
      //As getRegionLocation() method is not throwing any exception hence needs to handle following way.
      e.getCause();
    }
    Futures.addCallback(this.regionsFuture, new FutureCallback<List<HRegionLocation>>() {
      @Override 
      public void onSuccess(@Nullable List<HRegionLocation> result) {
        for(HRegionLocation region : result) {
          if (region.getRegion().containsRow(row)) {
            hRegionLocation = region;
          } 
        }
      }
      @Override 
      public void onFailure(Throwable t) {
        synchronized (BigtableAsyncTableRegionLocator.this) {
          regionsFuture = null;
        }
      }
    });
    if(hRegionLocation == null) {
      LOG.info("Region not found for row: ", Bytes.toStringBinary(row));
    }
    return CompletableFuture.completedFuture(hRegionLocation);
  }

  @Override
  public SampledRowKeysAdapter getSampledRowKeysAdapter(TableName tableName,
      ServerName serverName) {
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
