/*
 * Copyright 2018 Google LLC
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

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;

/**
 * AbstractBigtableRegionLocator class.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public abstract class AbstractBigtableRegionLocator {
  // Reuse the results from previous calls during this time.
  /** Constant <code>MAX_REGION_AGE_MILLIS=60 * 1000</code> */
  public static long MAX_REGION_AGE_MILLIS = 60 * 1000;

  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(AbstractBigtableRegionLocator.class);

  protected final TableName tableName;
  private ApiFuture<List<HRegionLocation>> regionsFuture;
  private final DataClientWrapper client;
  private final SampledRowKeysAdapter adapter;
  private final String fullTableName;
  private long regionsFetchTimeMillis;

  public AbstractBigtableRegionLocator(
      TableName tableName, BigtableHBaseSettings settings, DataClientWrapper client) {
    this.tableName = tableName;
    this.client = client;
    this.fullTableName =
        NameUtil.formatTableName(
            settings.getProjectId(), settings.getInstanceId(), tableName.getNameAsString());
    ServerName serverName = ServerName.valueOf(settings.getDataHost(), settings.getPort(), 0);
    this.adapter = getSampledRowKeysAdapter(tableName, serverName);
  }

  public abstract SampledRowKeysAdapter getSampledRowKeysAdapter(
      TableName tableName, ServerName serverName);

  /**
   * The list of regions will be sorted and cover all the possible rows.
   *
   * @param reload a boolean field.
   * @return a {@link List} object.
   */
  protected synchronized ApiFuture<List<HRegionLocation>> getRegionsAsync(boolean reload) {
    // If we don't need to refresh and we have a recent enough version, just use that.
    if (!reload
        && regionsFuture != null
        && regionsFetchTimeMillis + MAX_REGION_AGE_MILLIS > System.currentTimeMillis()) {
      return this.regionsFuture;
    }

    LOG.debug("Sampling rowkeys for table %s", fullTableName);

    try {
      ApiFuture<List<KeyOffset>> future = client.sampleRowKeysAsync(tableName.getNameAsString());
      this.regionsFuture =
          ApiFutures.transform(
              future,
              new ApiFunction<List<KeyOffset>, List<HRegionLocation>>() {
                @Override
                public List<HRegionLocation> apply(@Nullable List<KeyOffset> input) {
                  return adapter.adaptResponse(input);
                }
              },
              MoreExecutors.directExecutor());
      ApiFutures.addCallback(
          this.regionsFuture,
          new ApiFutureCallback<List<HRegionLocation>>() {
            @Override
            public void onSuccess(@Nullable List<HRegionLocation> result) {}

            @Override
            public void onFailure(Throwable t) {
              synchronized (AbstractBigtableRegionLocator.this) {
                regionsFuture = null;
              }
            }
          },
          MoreExecutors.directExecutor());
      regionsFetchTimeMillis = System.currentTimeMillis();
      return this.regionsFuture;
    } catch (Throwable throwable) {
      regionsFuture = null;
      Futures.immediateFailedFuture(throwable);
    }
    return regionsFuture;
  }
}
