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

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;

/**
 * Adapts HBase Deletes, Gets, Scans, Puts, RowMutations, Appends and Increments to Bigtable requests.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class HBaseRequestAdapter {

  private final static RequestContext REQUEST_CONTEXT = RequestContext.create(
      InstanceName.of("", ""),
      ""
  );

  public static class MutationAdapters {
    protected final PutAdapter putAdapter;
    protected final HBaseMutationAdapter hbaseMutationAdapter;
    protected final RowMutationsAdapter rowMutationsAdapter;

    public MutationAdapters(BigtableOptions options, Configuration config) {
      this(Adapters.createPutAdapter(config, options));
    }

    @VisibleForTesting
    MutationAdapters(PutAdapter putAdapter) {
      this.putAdapter = putAdapter;
      this.hbaseMutationAdapter = Adapters.createMutationsAdapter(putAdapter);
      this.rowMutationsAdapter = new RowMutationsAdapter(hbaseMutationAdapter);
    }

    public MutationAdapters withServerSideTimestamps() {
      return new MutationAdapters(putAdapter.withServerSideTimestamps());
    }
  }

  protected final MutationAdapters mutationAdapters;
  protected final TableName tableName;
  protected final BigtableTableName bigtableTableName;

  /**
   * <p>Constructor for HBaseRequestAdapter.</p>
   *
   * @param options a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @param tableName a {@link org.apache.hadoop.hbase.TableName} object.
   * @param config a {@link org.apache.hadoop.conf.Configuration} object.
   */
  public HBaseRequestAdapter(BigtableOptions options, TableName tableName, Configuration config) {
    this(options, tableName, new MutationAdapters(options, config));
  }

  /**
   * <p>Constructor for HBaseRequestAdapter.</p>
   *
   * @param options a {@link BigtableOptions} object.
   * @param tableName a {@link TableName} object.
   * @param mutationAdapters a {@link MutationAdapters} object.
   */
  public HBaseRequestAdapter(BigtableOptions options,
                             TableName tableName,
                             MutationAdapters mutationAdapters) {
    this(tableName,
        options.getInstanceName().toTableName(tableName.getQualifierAsString()),
        mutationAdapters);
  }


  /**
   * <p>Constructor for HBaseRequestAdapter.</p>
   *
   * @param tableName a {@link TableName} object.
   * @param bigtableTableName a {@link BigtableTableName} object.
   * @param mutationAdapters a {@link MutationAdapters} object.
   */
  @VisibleForTesting
  HBaseRequestAdapter(TableName tableName,
                              BigtableTableName bigtableTableName,
                              MutationAdapters mutationAdapters) {
    this.tableName = tableName;
    this.bigtableTableName = bigtableTableName;
    this.mutationAdapters = mutationAdapters;
  }

  public HBaseRequestAdapter withServerSideTimestamps(){
    return new HBaseRequestAdapter(tableName, bigtableTableName, mutationAdapters.withServerSideTimestamps());
  }

  /**
   * <p>adapt.</p>
   *
   * @param delete a {@link org.apache.hadoop.hbase.client.Delete} object.
   * @return a {@link com.google.bigtable.v2.MutateRowRequest} object.
   */
  public MutateRowRequest adapt(Delete delete) {
    Mutation mutation = Mutation.create();
    Adapters.DELETE_ADAPTER.adapt(delete, mutation);
    return toMutateRowRequest(delete.getRow(), mutation);
  }

  /**
   * <p>adapt.</p>
   *
   * @param delete a {@link org.apache.hadoop.hbase.client.Delete} object.
   * @return a {@link com.google.bigtable.v2.MutateRowsRequest.Entry} object.
   */
  public MutateRowsRequest.Entry adaptEntry(Delete delete) {
    Mutation mutation = Mutation.create();
    Adapters.DELETE_ADAPTER.toEntry(delete, mutation);
    return toEntry(delete.getRow(), mutation);
  }

  /**
   * <p>adapt.</p>
   *
   * @param get a {@link org.apache.hadoop.hbase.client.Get} object.
   * @return a {@link com.google.bigtable.v2.ReadRowsRequest} object.
   */
  public ReadRowsRequest adapt(Get get) {
    ReadHooks readHooks = new DefaultReadHooks();
    ReadRowsRequest.Builder builder = Adapters.GET_ADAPTER.adapt(get, readHooks);
    builder.setTableName(getTableNameString());
    return readHooks.applyPreSendHook(builder.build());
  }

  /**
   * <p>adapt.</p>
   *
   * @param scan a {@link org.apache.hadoop.hbase.client.Scan} object.
   * @return a {@link com.google.bigtable.v2.ReadRowsRequest} object.
   */
  public ReadRowsRequest adapt(Scan scan) {
    ReadHooks readHooks = new DefaultReadHooks();
    ReadRowsRequest.Builder builder = Adapters.SCAN_ADAPTER.adapt(scan, readHooks);
    builder.setTableName(getTableNameString());
    return readHooks.applyPreSendHook(builder.build());
  }

  /**
   * <p>adapt.</p>
   *
   * @param append a {@link org.apache.hadoop.hbase.client.Append} object.
   * @return a {@link com.google.bigtable.v2.ReadModifyWriteRowRequest} object.
   */
  public ReadModifyWriteRowRequest adapt(Append append) {
    ReadModifyWriteRowRequest.Builder builder = ReadModifyWriteRowRequest.newBuilder();
    Adapters.APPEND_ADAPTER.adapt(append, builder);
    builder.setTableName(getTableNameString());
    return builder.build();
  }

  /**
   * <p>adapt.</p>
   *
   * @param increment a {@link org.apache.hadoop.hbase.client.Increment} object.
   * @return a {@link com.google.bigtable.v2.ReadModifyWriteRowRequest} object.
   */
  public ReadModifyWriteRowRequest adapt(Increment increment) {
    ReadModifyWriteRowRequest.Builder builder = ReadModifyWriteRowRequest.newBuilder();
    Adapters.INCREMENT_ADAPTER.adapt(increment, builder);
    builder.setTableName(getTableNameString());
    return builder.build();
  }

  /**
   * <p>adapt.</p>
   *
   * @param put a {@link org.apache.hadoop.hbase.client.Put} object.
   * @return a {@link com.google.bigtable.v2.MutateRowRequest} object.
   */
  public MutateRowRequest adapt(Put put) {
    Mutation mutation = Mutation.create();
    mutationAdapters.putAdapter.adapt(put, mutation);
    return toMutateRowRequest(put.getRow(), mutation);
  }


  /**
   * <p>adaptEntry.</p>
   *
   * @param put a {@link org.apache.hadoop.hbase.client.Put} object.
   * @return a {@link com.google.bigtable.v2.MutateRowsRequest.Entry} object.
   */
  public MutateRowsRequest.Entry adaptEntry(Put put) {
    Mutation mutation = Mutation.create();
    mutationAdapters.putAdapter.toEntry(put, mutation);
    return toEntry(put.getRow(), mutation);
  }

  /**
   * <p>adapt.</p>
   *
   * @param mutations a {@link org.apache.hadoop.hbase.client.RowMutations} object.
   * @return a {@link com.google.bigtable.v2.MutateRowRequest} object.
   */
  public MutateRowRequest adapt(RowMutations mutations) {
    Mutation mutation = Mutation.create();
    mutationAdapters.rowMutationsAdapter.adapt(mutations, mutation);
    return toMutateRowRequest(mutations.getRow(), mutation);
  }

  /**
   * <p>adaptEntry.</p>
   *
   * @param mutations a {@link org.apache.hadoop.hbase.client.RowMutations} object.
   * @return a {@link com.google.bigtable.v2.MutateRowsRequest.Entry} object.
   */
  public MutateRowsRequest.Entry adaptEntry(RowMutations mutations) {
    Mutation mutation = Mutation.create();
    mutationAdapters.rowMutationsAdapter.toEntry(mutations, mutation);
    return toEntry(mutations.getRow(), mutation);
  }

  /**
   * <p>adapt.</p>
   *
   * @param mutation a {@link org.apache.hadoop.hbase.client.Mutation} object.
   * @return a {@link com.google.bigtable.v2.MutateRowRequest} object.
   */
  public MutateRowRequest adapt(org.apache.hadoop.hbase.client.Mutation mutation) {
    Mutation mutationModel = Mutation.create();
    mutationAdapters.hbaseMutationAdapter.adapt(mutation, mutationModel);
    return toMutateRowRequest(mutation.getRow(), mutationModel);
  }

  /**
   * <p>Getter for the field <code>bigtableTableName</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   */
  public BigtableTableName getBigtableTableName() {
    return bigtableTableName;
  }
  
  /**
   * <p>Getter for the field <code>tableName</code>.</p>
   *
   * @return a {@link org.apache.hadoop.hbase.TableName} object.
   */
  public TableName getTableName() {
    return tableName;
  }

  /**
   * <p>getTableNameString.</p>
   *
   * @return a {@link java.lang.String} object.
   */
  protected String getTableNameString() {
    return getBigtableTableName().toString();
  }

  private MutateRowRequest toMutateRowRequest(byte[] rowKey, Mutation mutation) {
    RowMutation rowMutation = toRowMutationModel(rowKey, mutation);
    MutateRowRequest.Builder builder = rowMutation.toProto(REQUEST_CONTEXT).toBuilder();
    builder.setTableName(getTableNameString());
    return builder.build();
  }

  private MutateRowsRequest.Entry toEntry(byte[] rowKey, Mutation mutation) {
    RowMutation rowMutation = toRowMutationModel(rowKey, mutation);
    return rowMutation.toBulkProto(REQUEST_CONTEXT).getEntries(0);
  }

  private RowMutation toRowMutationModel(byte [] rowKey, Mutation mutation) {
    return RowMutation.create("", ByteString.copyFrom(rowKey), mutation);
  }

}
