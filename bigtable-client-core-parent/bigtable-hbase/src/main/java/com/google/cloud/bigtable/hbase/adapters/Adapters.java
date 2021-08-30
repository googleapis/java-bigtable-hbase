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
import com.google.cloud.bigtable.hbase.adapters.filters.BigtableWhileMatchResultScannerAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.FlatRowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowRangeAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ScanAdapter;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;

/**
 * Manages all Adapters
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public final class Adapters {
  /** Constant <code>ROW_ADAPTER</code> */
  public static final RowAdapter ROW_ADAPTER = new RowAdapter();
  /** Constant <code>FLAT_ROW_ADAPTER</code> */
  public static final FlatRowAdapter FLAT_ROW_ADAPTER = new FlatRowAdapter();
  /** Constant <code>APPEND_ADAPTER</code> */
  public static final AppendAdapter APPEND_ADAPTER = new AppendAdapter();
  /** Constant <code>INCREMENT_ADAPTER</code> */
  public static final IncrementAdapter INCREMENT_ADAPTER = new IncrementAdapter();
  /** Constant <code>DELETE_ADAPTER</code> */
  public static final DeleteAdapter DELETE_ADAPTER = new DeleteAdapter();
  /** Constant <code>FILTER_ADAPTER</code> */
  public static final FilterAdapter FILTER_ADAPTER = FilterAdapter.buildAdapter();
  /** Constant <code>ROW_RANGE_ADAPTER</code> */
  public static final RowRangeAdapter ROW_RANGE_ADAPTER = new RowRangeAdapter();
  /** Constant <code>SCAN_ADAPTER</code> */
  public static final ScanAdapter SCAN_ADAPTER = new ScanAdapter(FILTER_ADAPTER, ROW_RANGE_ADAPTER);
  /** Constant <code>BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER</code> */
  public static final BigtableWhileMatchResultScannerAdapter
      BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER =
          new BigtableWhileMatchResultScannerAdapter();
  /** Constant <code>GET_ADAPTER</code> */
  public static final GetAdapter GET_ADAPTER = new GetAdapter(SCAN_ADAPTER);

  /**
   * createMutationsAdapter.
   *
   * @param putAdapter a {@link com.google.cloud.bigtable.hbase.adapters.PutAdapter} object.
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.HBaseMutationAdapter} object.
   */
  public static HBaseMutationAdapter createMutationsAdapter(PutAdapter putAdapter) {
    return new HBaseMutationAdapter(
        DELETE_ADAPTER,
        putAdapter,
        new UnsupportedMutationAdapter<Increment>("increment"),
        new UnsupportedMutationAdapter<Append>("append"));
  }

  /**
   * createPutAdapter.
   *
   * @param settings a {@link org.apache.hadoop.conf.Configuration} object.
   * @return a {@link com.google.cloud.bigtable.hbase.adapters.PutAdapter} object.
   */
  public static PutAdapter createPutAdapter(BigtableHBaseSettings settings) {
    Configuration config = settings.getConfiguration();
    boolean setClientTimestamp = !settings.isRetriesWithoutTimestampAllowed();
    return new PutAdapter(config.getInt("hbase.client.keyvalue.maxsize", -1), setClientTimestamp);
  }

  private Adapters() {}
}
