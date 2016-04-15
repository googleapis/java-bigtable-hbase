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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableTableName;

 /**
 * Adapts HBase Deletes, Gets, Scans, Puts, RowMutations, Appends and Increments to Bigtable requests.
 *
 */
public class HBaseRequestAdapter {

  protected final TableName tableName;
  protected final BigtableTableName bigtableTableName;
  protected final PutAdapter putAdapter;
  protected final MutationAdapter mutationAdapter;
  protected final RowMutationsAdapter rowMutationsAdapter;

  public HBaseRequestAdapter(BigtableOptions options, TableName tableName, Configuration config) {
    this.tableName = tableName;
    this.bigtableTableName = options.getClusterName().toTableName(tableName.getQualifierAsString());
    this.putAdapter = Adapters.createPutAdapter(config, options);
    this.mutationAdapter = Adapters.createMutationsAdapter(putAdapter);
    this.rowMutationsAdapter = new RowMutationsAdapter(mutationAdapter);
  }

  public MutateRowRequest adapt(Delete delete) {
    MutateRowRequest.Builder requestBuilder = Adapters.DELETE_ADAPTER.adapt(delete);
    requestBuilder.setTableName(bigtableTableName.toString());
    return requestBuilder.build();
  }

  public ReadRowsRequest adapt(Get get) {
    ReadHooks readHooks = new DefaultReadHooks();
    ReadRowsRequest.Builder builder = Adapters.GET_ADAPTER.adapt(get, readHooks);
    builder.setTableName(bigtableTableName.toString());
    return readHooks.applyPreSendHook(builder.build());
  }

  public ReadRowsRequest adapt(Scan scan) {
    ReadHooks readHooks = new DefaultReadHooks();
    ReadRowsRequest.Builder builder = Adapters.SCAN_ADAPTER.adapt(scan, readHooks);
    builder.setTableName(bigtableTableName.toString());
    return readHooks.applyPreSendHook(builder.build());
  }

  public ReadModifyWriteRowRequest adapt(Append append) {
    ReadModifyWriteRowRequest.Builder builder = Adapters.APPEND_ADAPTER.adapt(append);
    builder.setTableName(bigtableTableName.toString());
    return builder.build();
  }

  public ReadModifyWriteRowRequest adapt(Increment increment) {
    ReadModifyWriteRowRequest.Builder builder = Adapters.INCREMENT_ADAPTER.adapt(increment);
    builder.setTableName(bigtableTableName.toString());
    return builder.build();
  }

  public MutateRowRequest adapt(Put put) {
    MutateRowRequest.Builder builder = putAdapter.adapt(put);
    builder.setTableName(bigtableTableName.toString());
    return builder.build();
  }

  public MutateRowRequest adapt(RowMutations mutations) {
    MutateRowRequest.Builder builder = rowMutationsAdapter.adapt(mutations);
    builder.setTableName(bigtableTableName.toString());
    return builder.build();
  }

  public MutateRowRequest adapt(org.apache.hadoop.hbase.client.Mutation mutation) {
    MutateRowRequest.Builder builder = mutationAdapter.adapt(mutation);
    builder.setTableName(bigtableTableName.toString());
    return builder.build();
  }

  public BigtableTableName getBigtableTableName() {
    return bigtableTableName;
  }
  
  public TableName getTableName() {
    return tableName;
  }
}
