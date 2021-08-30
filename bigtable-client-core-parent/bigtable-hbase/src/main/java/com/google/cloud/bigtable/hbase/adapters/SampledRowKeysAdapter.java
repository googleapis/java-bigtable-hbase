/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.util.Logger;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * SampledRowKeysAdapter class.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class SampledRowKeysAdapter {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(SampledRowKeysAdapter.class);

  protected final TableName tableName;
  protected final ServerName serverName;

  /**
   * Constructor for SampledRowKeysAdapter.
   *
   * @param tableName a {@link org.apache.hadoop.hbase.TableName} object.
   * @param serverName a {@link org.apache.hadoop.hbase.ServerName} object.
   */
  public SampledRowKeysAdapter(TableName tableName, ServerName serverName) {
    this.tableName = tableName;
    this.serverName = serverName;
  }

  /**
   * adaptResponse.
   *
   * @param responses a {@link java.util.List} object.
   * @return a {@link java.util.List} object.
   */
  public List<HRegionLocation> adaptResponse(List<KeyOffset> responses) {

    List<HRegionLocation> regions = new ArrayList<>();

    // Starting by the first possible row, iterate over the sorted sampled row keys and create
    // regions.
    byte[] startKey = HConstants.EMPTY_START_ROW;

    for (KeyOffset response : responses) {
      byte[] endKey = response.getKey().toByteArray();

      // Avoid empty regions.
      if (Bytes.equals(startKey, endKey)) {
        continue;
      }
      regions.add(createRegionLocation(startKey, endKey));
      startKey = endKey;
    }

    // Create one last region if the last region doesn't reach the end or there are no regions.
    byte[] endKey = HConstants.EMPTY_END_ROW;
    if (regions.isEmpty() || !Bytes.equals(startKey, endKey)) {
      regions.add(createRegionLocation(startKey, endKey));
    }
    return regions;
  }

  /**
   * HBase 1.x and 2.x have non compatible {@link HRegionInfo} classes. HBase 2.x introduces a
   * RegionInfo interface, which makes the two hbase classes with the same name binary incompatible.
   * {@link HRegionLocation} uses RegionInfo instead of {@link HRegionInfo}, causing confusion and
   * delay. {@link AbstractBigtableConnection#getRegionLocator(TableName)} calls an abstract method
   * which subclasses will construct appropriate {@link SampledRowKeysAdapter} implementations.
   *
   * @param startKey a byte array.
   * @param endKey a byte array.
   * @return a {@link HRegionLocation} object.
   */
  protected abstract HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey);
}
