/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.v2.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.common.collect.ImmutableList;

public class BigtableRegionLocator implements RegionLocator {
  // Reuse the results from previous calls during this time.
  public static long MAX_REGION_AGE_MILLIS = 60 * 1000;

  protected static final Logger LOG = new Logger(BigtableRegionLocator.class);

  private final TableName tableName;
  private final BigtableDataClient client;
  private final SampledRowKeysAdapter adapter;
  private final BigtableTableName bigtableTableName;
  private List<HRegionLocation> regions;
  private long regionsFetchTimeMillis;

  public BigtableRegionLocator(TableName tableName, BigtableOptions options, BigtableDataClient client) {
    this.tableName = tableName;
    this.client = client;
    this.bigtableTableName = options.getInstanceName().toTableName(tableName.getNameAsString());
    ServerName serverName = ServerName.valueOf(options.getDataHost(), options.getPort(), 0);
    this.adapter = new SampledRowKeysAdapter(tableName, serverName);
  }

  /**
   * The list of regions will be sorted and cover all the possible rows.
   */
  private synchronized List<HRegionLocation> getRegions(boolean reload) throws IOException {
    // If we don't need to refresh and we have a recent enough version, just use that.
    if (!reload && regions != null &&
        regionsFetchTimeMillis + MAX_REGION_AGE_MILLIS > System.currentTimeMillis()) {
      return regions;
    }

    SampleRowKeysRequest.Builder request = SampleRowKeysRequest.newBuilder();
    request.setTableName(bigtableTableName.toString());
    LOG.debug("Sampling rowkeys for table %s", request.getTableName());

    try {
      ImmutableList<SampleRowKeysResponse> responses = client.sampleRowKeys(request.build());
      regions = adapter.adaptResponse(responses);
      regionsFetchTimeMillis = System.currentTimeMillis();
      return regions;
    } catch(Throwable throwable) {
      regions = null;
      throw new IOException("Error sampling rowkeys.", throwable);
    }
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row) throws IOException {
    return getRegionLocation(row, false);
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row, boolean reload) throws IOException {
    for(HRegionLocation region : getRegions(reload)) {
      if (region.getRegionInfo().containsRow(row)) {
        return region;
      }
    }
    throw new IOException("Region not found for row: " + Bytes.toStringBinary(row));
  }

  @Override
  public List<HRegionLocation> getAllRegionLocations() throws IOException {
    return getRegions(false);
  }

  @Override
  public byte[][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  @Override
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  @Override
  public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    List<HRegionLocation> regions = getAllRegionLocations();
    byte[][] startKeys = new byte[regions.size()][];
    byte[][] endKeys = new byte[regions.size()][];
    int i = 0;
    for(HRegionLocation region : regions) {
      startKeys[i] = region.getRegionInfo().getStartKey();
      endKeys[i] = region.getRegionInfo().getEndKey();
      i++;
    }
    return Pair.newPair(startKeys, endKeys);
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public void close() throws IOException {
  }
}
