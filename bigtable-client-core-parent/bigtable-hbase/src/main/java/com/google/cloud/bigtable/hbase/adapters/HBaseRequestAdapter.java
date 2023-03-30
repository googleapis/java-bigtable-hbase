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
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.MutationApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Adapts HBase Deletes, Gets, Scans, Puts, RowMutations, Appends and Increments to Bigtable
 * requests.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class HBaseRequestAdapter {

  public static class MutationAdapters {

    protected final PutAdapter putAdapter;
    protected final HBaseMutationAdapter hbaseMutationAdapter;
    protected final RowMutationsAdapter rowMutationsAdapter;

    public MutationAdapters(BigtableHBaseSettings settings) {
      this(Adapters.createPutAdapter(settings));
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

  /**
   * Constructor for HBaseRequestAdapter.
   *
   * @param settings a {@link BigtableHBaseSettings} object.
   * @param tableName a {@link org.apache.hadoop.hbase.TableName} object.
   */
  public HBaseRequestAdapter(BigtableHBaseSettings settings, TableName tableName) {
    this(settings, tableName, new MutationAdapters(settings));
  }

  /**
   * Constructor for HBaseRequestAdapter.
   *
   * @param settings a {@link BigtableHBaseSettings} object.
   * @param tableName a {@link TableName} object.
   * @param mutationAdapters a {@link MutationAdapters} object.
   */
  public HBaseRequestAdapter(
      BigtableHBaseSettings settings, TableName tableName, MutationAdapters mutationAdapters) {
    this(tableName, mutationAdapters);
  }

  /**
   * Constructor for HBaseRequestAdapter.
   *
   * @param tableName a {@link TableName} object.
   * @param mutationAdapters a {@link MutationAdapters} object.
   */
  @VisibleForTesting
  HBaseRequestAdapter(TableName tableName, MutationAdapters mutationAdapters) {
    this.tableName = tableName;
    this.mutationAdapters = mutationAdapters;
  }

  public HBaseRequestAdapter withServerSideTimestamps() {
    return new HBaseRequestAdapter(tableName, mutationAdapters.withServerSideTimestamps());
  }

  /**
   * adapt.
   *
   * @param delete a {@link org.apache.hadoop.hbase.client.Delete} object.
   * @return a {@link RowMutation} object.
   */
  public RowMutation adapt(Delete delete) {
    RowMutation rowMutation = newRowMutationModel(delete.getRow());
    adapt(delete, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param delete a {@link org.apache.hadoop.hbase.client.Delete} object.
   * @param mutationApi a {@link com.google.cloud.bigtable.data.v2.models.MutationApi} object.
   */
  @InternalApi
  public void adapt(Delete delete, MutationApi<?> mutationApi) {
    Adapters.DELETE_ADAPTER.adapt(delete, mutationApi);
  }

  /**
   * adapt.
   *
   * @param delete a {@link Delete} object.
   * @return a {@link RowMutationEntry} object.
   */
  public RowMutationEntry adaptEntry(Delete delete) {
    RowMutationEntry rowMutation = buildRowMutationEntry(delete.getRow());
    adapt(delete, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param get a {@link Get} object.
   * @return a {@link Query} object.
   */
  public Query adapt(Get get) {
    ReadHooks readHooks = new DefaultReadHooks();
    Query query = Query.create(getTableId());
    query = Adapters.GET_ADAPTER.adapt(get, readHooks, query);
    readHooks.applyPreSendHook(query);
    return query;
  }

  /**
   * adapt.
   *
   * @param scan a {@link Scan} object.
   * @return a {@link Query} object.
   */
  public Query adapt(Scan scan) {
    ReadHooks readHooks = new DefaultReadHooks();
    Query query = Query.create(getTableId());
    query = Adapters.SCAN_ADAPTER.adapt(scan, readHooks, query);
    readHooks.applyPreSendHook(query);
    return query;
  }

  /**
   * adapt.
   *
   * @param append a {@link Append} object.
   * @return a {@link ReadModifyWriteRow} object.
   */
  public ReadModifyWriteRow adapt(Append append) {
    ReadModifyWriteRow readModifyWriteRow =
        ReadModifyWriteRow.create(getTableId(), ByteString.copyFrom(append.getRow()));
    Adapters.APPEND_ADAPTER.adapt(append, readModifyWriteRow);
    return readModifyWriteRow;
  }

  /**
   * adapt.
   *
   * @param increment a {@link Increment} object.
   * @return a {@link ReadModifyWriteRow} object.
   */
  public ReadModifyWriteRow adapt(Increment increment) {
    ReadModifyWriteRow readModifyWriteRow =
        ReadModifyWriteRow.create(getTableId(), ByteString.copyFrom(increment.getRow()));
    Adapters.INCREMENT_ADAPTER.adapt(increment, readModifyWriteRow);
    return readModifyWriteRow;
  }

  /**
   * adapt.
   *
   * @param put a {@link org.apache.hadoop.hbase.client.Put} object.
   * @return a {@link RowMutation} object.
   */
  public RowMutation adapt(Put put) {
    RowMutation rowMutation = newRowMutationModel(put.getRow());
    adapt(put, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param put a {@link org.apache.hadoop.hbase.client.Put} object.
   * @param mutationApi a {@link com.google.cloud.bigtable.data.v2.models.MutationApi} object.
   */
  @InternalApi
  public void adapt(Put put, MutationApi<?> mutationApi) {
    mutationAdapters.putAdapter.adapt(put, mutationApi);
  }
  /**
   * adaptEntry.
   *
   * @param put a {@link Put} object.
   * @return a {@link RowMutationEntry} object.
   */
  public RowMutationEntry adaptEntry(Put put) {
    RowMutationEntry rowMutation = buildRowMutationEntry(put.getRow());
    adapt(put, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param mutations a {@link org.apache.hadoop.hbase.client.RowMutations} object.
   * @return a {@link RowMutation} object.
   */
  public RowMutation adapt(RowMutations mutations) {
    RowMutation rowMutation = newRowMutationModel(mutations.getRow());
    adapt(mutations, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param mutations a {@link org.apache.hadoop.hbase.client.RowMutations} object.
   * @param mutationApi a {@link MutationApi} object.
   */
  @InternalApi
  public void adapt(RowMutations mutations, MutationApi<?> mutationApi) {
    mutationAdapters.rowMutationsAdapter.adapt(mutations, mutationApi);
  }

  /**
   * adaptEntry.
   *
   * @param mutations a {@link org.apache.hadoop.hbase.client.RowMutations} object.
   * @return a {@link RowMutationEntry} object.
   */
  public RowMutationEntry adaptEntry(RowMutations mutations) {
    RowMutationEntry rowMutation = buildRowMutationEntry(mutations.getRow());
    adapt(mutations, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param mutation a {@link org.apache.hadoop.hbase.client.Mutation} object.
   * @return a {@link RowMutation} object.
   */
  public RowMutation adapt(org.apache.hadoop.hbase.client.Mutation mutation) {
    RowMutation rowMutation = newRowMutationModel(mutation.getRow());
    adapt(mutation, rowMutation);
    return rowMutation;
  }

  /**
   * adapt.
   *
   * @param mutation a {@link org.apache.hadoop.hbase.client.Mutation} object.
   * @param mutationApi a {@link MutationApi} object.
   */
  @InternalApi
  private void adapt(org.apache.hadoop.hbase.client.Mutation mutation, MutationApi<?> mutationApi) {
    mutationAdapters.hbaseMutationAdapter.adapt(mutation, mutationApi);
  }

  /** Returns the tableId for which request is being transformed. */
  public String getTableId() {
    return tableName.getNameAsString();
  }

  /**
   * Getter for the field <code>tableName</code>.
   *
   * @return a {@link org.apache.hadoop.hbase.TableName} object.
   */
  public TableName getTableName() {
    return tableName;
  }

  private RowMutation newRowMutationModel(byte[] rowKey) {
    if (!mutationAdapters.putAdapter.isSetClientTimestamp()) {
      return RowMutation.create(getTableId(), ByteString.copyFrom(rowKey), Mutation.createUnsafe());
    }
    return RowMutation.create(getTableId(), ByteString.copyFrom(rowKey));
  }

  private RowMutationEntry buildRowMutationEntry(byte[] rowKey) {
    if (!mutationAdapters.putAdapter.isSetClientTimestamp()) {
      return RowMutationEntry.createUnsafe(ByteString.copyFrom(rowKey));
    }

    return RowMutationEntry.create(ByteString.copyFrom(rowKey));
  }
}
