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
package com.google.cloud.bigtable.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;

/**
 * HBase 1.x specific implementation of {@link AbstractBigtableConnection}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableConnection extends AbstractBigtableConnection {

  /**
   * Constructor for BigtableConnection.
   *
   * @param conf a {@link Configuration} object.
   * @throws IOException if any.
   */
  public BigtableConnection(Configuration conf) throws IOException {
    super(conf);
  }

  BigtableConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    super(conf, managed, pool, user);
  }

  /** {@inheritDoc} */
  @Override
  public Admin getAdmin() throws IOException {
    return BigtableAdmin.createInstance(this);
  }

  @Override
  public String getClusterId() throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  protected SampledRowKeysAdapter createSampledRowKeysAdapter(
      TableName tableName, ServerName serverName) {
    return new SampledRowKeysAdapter(tableName, serverName) {
      @Override
      protected HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey) {
        HRegionInfo hRegionInfo = new HRegionInfo(tableName, startKey, endKey);
        return new HRegionLocation(hRegionInfo, serverName);
      }
    };
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService ignored) throws IOException {
    return BigtableTable.create(this, createAdapter(tableName));
  }

  @Override
  public List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException {
    ImmutableList.Builder<HRegionInfo> regionInfos = ImmutableList.builder();
    for (HRegionLocation location : getRegionLocator(tableName).getAllRegionLocations()) {
      regionInfos.add(location.getRegionInfo());
    }
    return regionInfos.build();
  }
}
