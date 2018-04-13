/*
 * Copyright 2018 Google Inc. All Rights Reserved.

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

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;

import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p> AbstractBigtbleRegionLocator class. </p>
 * 
 * @author rupeshit
 *
 */
public abstract class AbstractBigtbleRegionLocator {
  // Reuse the results from previous calls during this time.
  /** Constant <code>MAX_REGION_AGE_MILLIS=60 * 1000</code> */
  public static long MAX_REGION_AGE_MILLIS = 60 * 1000;

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractBigtbleRegionLocator.class);

  protected final TableName tableName;
  protected final BigtableDataClient client;
  protected final SampledRowKeysAdapter adapter;
  protected final BigtableTableName bigtableTableName;
  protected List<HRegionLocation> regions;
  protected long regionsFetchTimeMillis;
  protected ListenableFuture<List<HRegionLocation>> regionsFuture;
  
  public AbstractBigtbleRegionLocator (TableName tableName, BigtableOptions options, BigtableDataClient client) {
    this.tableName = tableName;
    this.client = client;
    this.bigtableTableName = options.getInstanceName().toTableName(tableName.getNameAsString());
    ServerName serverName = ServerName.valueOf(options.getDataHost(), options.getPort(), 0);
    this.adapter = getSampledRowKeysAdapter(tableName, serverName);
  }
  
  public abstract SampledRowKeysAdapter getSampledRowKeysAdapter(TableName tableName,
      ServerName serverName);
 
  /**
   * The list of regions will be sorted and cover all the possible rows.
   */
  protected synchronized ListenableFuture<List<HRegionLocation>> getRegionsAsync(boolean reload) throws IOException {
    // If we don't need to refresh and we have a recent enough version, just use that.
    if (!reload && regionsFuture != null &&
        regionsFetchTimeMillis + MAX_REGION_AGE_MILLIS > System.currentTimeMillis()) {
      return this.regionsFuture;
    }

    SampleRowKeysRequest.Builder request = SampleRowKeysRequest.newBuilder();
    request.setTableName(bigtableTableName.toString());
    LOG.debug("Sampling rowkeys for table %s", request.getTableName());

    try {
      ListenableFuture<List<SampleRowKeysResponse>> future = client.sampleRowKeysAsync(request.build());
      this.regionsFuture = Futures
          .transform(future, new Function<List<SampleRowKeysResponse>, List<HRegionLocation>>() {
            @Override
            public List<HRegionLocation> apply(@Nullable List<SampleRowKeysResponse> input) {
              return adapter.adaptResponse(input);
            }
          });
      Futures.addCallback(this.regionsFuture, new FutureCallback<List<HRegionLocation>>() {
        @Override public void onSuccess(@Nullable List<HRegionLocation> result) {
        }
        @Override public void onFailure(Throwable t) {
          synchronized (AbstractBigtbleRegionLocator.this) {
            regionsFuture = null;
          }
        }
      });
      regionsFetchTimeMillis = System.currentTimeMillis();
      return this.regionsFuture;
    } catch(Throwable throwable) {
      regionsFuture = null;
      throw new IOException("Error sampling rowkeys.", throwable);
    }
  }
}
