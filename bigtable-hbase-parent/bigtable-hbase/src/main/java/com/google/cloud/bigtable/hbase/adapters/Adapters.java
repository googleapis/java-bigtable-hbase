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
package com.google.cloud.bigtable.hbase.adapters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.adapters.filters.BigtableWhileMatchResultScannerAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.BigtableResultScannerAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ScanAdapter;

/**
 * Manages all Adapters
 */
public final class Adapters {
  public static final RowAdapter ROW_ADAPTER = new RowAdapter();
  public static final AppendAdapter APPEND_ADAPTER = new AppendAdapter();
  public static final IncrementAdapter INCREMENT_ADAPTER = new IncrementAdapter();
  public static final DeleteAdapter DELETE_ADAPTER = new DeleteAdapter();
  public static final FilterAdapter FILTER_ADAPTER = FilterAdapter.buildAdapter();
  public static final ScanAdapter SCAN_ADAPTER =  new ScanAdapter(FILTER_ADAPTER);
  public static final BigtableResultScannerAdapter BIGTABLE_RESULT_SCAN_ADAPTER =
      new BigtableResultScannerAdapter(ROW_ADAPTER);
  public static final BigtableWhileMatchResultScannerAdapter
      BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER =
      new BigtableWhileMatchResultScannerAdapter(ROW_ADAPTER);
  public static final GetAdapter GET_ADAPTER = new GetAdapter(SCAN_ADAPTER);

  public static MutationAdapter createMutationsAdapter(PutAdapter putAdapter) {
    return new MutationAdapter(
      DELETE_ADAPTER,
      putAdapter,
      new UnsupportedOperationAdapter<Increment>("increment"),
      new UnsupportedOperationAdapter<Append>("append"));
  }

  public static PutAdapter createPutAdapter(Configuration config, BigtableOptions options) {
    boolean setClientTimestamp = !options.getRetryOptions().allowRetriesWithoutTimestamp();
    return new PutAdapter(config.getInt("hbase.client.keyvalue.maxsize", -1), setClientTimestamp);
  }

  private Adapters() {
  }
}
