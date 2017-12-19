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

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;

/**
 * Bigtable implementation of {@link AsyncTableRegionLocator}
 * 
 * @author spollapally
 */
public class BigtableAsyncTableRegionLocator implements AsyncTableRegionLocator {
  private final Logger LOG = new Logger(getClass());

  private final TableName tableName;
  private final BigtableDataClient client;
  private final SampledRowKeysAdapter adapter;
  private final BigtableTableName bigtableTableName;

  public BigtableAsyncTableRegionLocator(TableName tableName, BigtableOptions options,
      BigtableDataClient client) {
    this.tableName = tableName;
    this.client = client;
    this.bigtableTableName = options.getInstanceName().toTableName(tableName.getNameAsString());
    ServerName serverName = ServerName.valueOf(options.getDataHost(), options.getPort(), 0);
    this.adapter = new SampledRowKeysAdapter(tableName, serverName) {

      @Override
      protected HRegionLocation createHRegionLocation(HRegionInfo hRegionInfo,
          ServerName serverName) {
        return new HRegionLocation(hRegionInfo, serverName);
      }
    };
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] arg0, boolean arg1) {
    throw new UnsupportedOperationException("getRegionLocation"); // TODO
  }
}
