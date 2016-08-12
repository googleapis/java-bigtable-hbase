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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.common.base.Function;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * <p>BigtableTable class.</p>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableTable implements Table {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableTable.class);

  private static class TableMetrics {
    Timer putTimer = BigtableClientMetrics.timer(MetricLevel.Info, "google-cloud-bigtable.table.put.timer");
    Timer getTimer = BigtableClientMetrics.timer(MetricLevel.Info, "google-cloud-bigtable.table.get.timer");
  }

  // ReadHooks don't make sense from conditional mutations. If any filter attempts to make use of
  // them (which they shouldn't since we built the filter), throw an exception.
  private static final ReadHooks UNSUPPORTED_READ_HOOKS = new ReadHooks() {
    @Override
    public void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook) {
      throw new IllegalStateException(
          "We built a bad Filter for conditional mutation.");
    }

    @Override
    public ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest) {
      throw new UnsupportedOperationException(
          "We built a bad Filter for conditional mutation.");
    }
  };

  protected final TableName tableName;
  protected final BigtableOptions options;
  protected final HBaseRequestAdapter hbaseAdapter;

  protected final BigtableDataClient client;
  private BatchExecutor batchExecutor;
  protected final AbstractBigtableConnection bigtableConnection;
  private TableMetrics metrics = new TableMetrics();

  /**
   * Constructed by BigtableConnection
   *
   * @param bigtableConnection a {@link org.apache.hadoop.hbase.client.AbstractBigtableConnection} object.
   * @param hbaseAdapter a {@link com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter} object.
   */
  public BigtableTable(AbstractBigtableConnection bigtableConnection,
      HBaseRequestAdapter hbaseAdapter) {
    this.bigtableConnection = bigtableConnection;
    BigtableSession session = bigtableConnection.getSession();
    this.options = session.getOptions();
    this.client = session.getDataClient();
    this.hbaseAdapter = hbaseAdapter;
    this.tableName = hbaseAdapter.getTableName();
  }

  /** {@inheritDoc} */
  @Override
  public TableName getName() {
    return this.tableName;
  }

  /** {@inheritDoc} */
  @Override
  public final Configuration getConfiguration() {
    return this.bigtableConnection.getConfiguration();
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    try (Admin admin = this.bigtableConnection.getAdmin()) {
      return admin.getTableDescriptor(tableName);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean exists(Get get) throws IOException {
    LOG.trace("exists(Get)");
    // TODO: make use of strip_value() or count to hopefully return no extra data
    Result result = get(get);
    return !result.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    LOG.trace("existsAll(Get)");
    Boolean[] existsObjects = getBatchExecutor().exists(gets);
    boolean[] exists = new boolean[existsObjects.length];
    for (int i = 0; i < existsObjects.length; i++) {
      exists[i] = existsObjects[i];
    }
    return exists;
  }

  /** {@inheritDoc} */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    LOG.trace("batch(List<>, Object[])");
    getBatchExecutor().batch(actions, results);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    LOG.trace("batch(List<>)");
    return getBatchExecutor().batch(actions);
  }

  /** {@inheritDoc} */
  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Object[], Batch.Callback)");
    getBatchExecutor().batchCallback(actions, results, callback);
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Batch.Callback)");
    Object[] results = new Object[actions.size()];
    getBatchExecutor().batchCallback(actions, results, callback);
    return results;
  }

  /** {@inheritDoc} */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    LOG.trace("get(List<>)");
    return getBatchExecutor().batch(gets);
  }

  /** {@inheritDoc} */
  @Override
  public Result get(Get get) throws IOException {
    LOG.trace("get(Get)");
    Timer.Context timerContext = metrics.getTimer.time();
    try (com.google.cloud.bigtable.grpc.scanner.ResultScanner<com.google.bigtable.v2.Row> scanner =
        client.readRows(hbaseAdapter.adapt(get))) {
      return Adapters.ROW_ADAPTER.adaptResponse(scanner.next());
    } catch (Throwable t) {
      throw logAndCreateIOException("get", get.getRow(), t);
    } finally {
      timerContext.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    LOG.trace("getScanner(Scan)");
    try {
      com.google.cloud.bigtable.grpc.scanner.ResultScanner<com.google.bigtable.v2.Row> scanner =
          client.readRows(hbaseAdapter.adapt(scan));
      if (hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner);
      }
      return Adapters.BIGTABLE_RESULT_SCAN_ADAPTER.adapt(scanner);
    } catch (Throwable throwable) {
      LOG.error("Encountered exception when executing getScanner.", throwable);
      throw new IOException(
          makeGenericExceptionMessage(
              "getScanner",
              options.getProjectId(),
              tableName.getQualifierAsString()),
          throwable);
    }
  }

  @VisibleForTesting
  static boolean hasWhileMatchFilter(Filter filter) {
    if (filter instanceof WhileMatchFilter) {
      return true;
    }
 
    if (filter instanceof FilterList) {
      FilterList list = (FilterList) filter;
      for (Filter subFilter : list.getFilters()) {
        if (hasWhileMatchFilter(subFilter)) {
          return true;
        }
      }
    }

    return false;
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    LOG.trace("getScanner(byte[])");
    return getScanner(new Scan().addFamily(family));
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    LOG.trace("getScanner(byte[], byte[])");
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  /** {@inheritDoc} */
  @Override
  public void put(Put put) throws IOException {
    LOG.trace("put(Put)");
    Timer.Context timerContext = metrics.putTimer.time();
    MutateRowRequest request = hbaseAdapter.adapt(put);
    try {
      client.mutateRow(request);
    } catch (Throwable t) {
      throw logAndCreateIOException("put", put.getRow(), t);
    } finally {
      timerContext.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void put(List<Put> puts) throws IOException {
    LOG.trace("put(List<Put>)");
    getBatchExecutor().batch(puts);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    return checkAndPut(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, put);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {

    CheckAndMutateRowRequest.Builder requestBuilder =
        makeConditionalMutationRequestBuilder(
            row,
            family,
            qualifier,
            compareOp,
            value,
            put.getRow(),
            hbaseAdapter.adapt(put).getMutationsList());

    try {
      CheckAndMutateRowResponse response =
          client.checkAndMutateRow(requestBuilder.build());
      return wasMutationApplied(requestBuilder, response);
    } catch (Throwable t) {
      throw logAndCreateIOException("checkAndPut", row, t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void delete(Delete delete) throws IOException {
    LOG.trace("delete(Delete)");
    MutateRowRequest request = hbaseAdapter.adapt(delete);
    try {
      client.mutateRow(request);
    } catch (Throwable t) {
      throw logAndCreateIOException("delete", delete.getRow(), t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    LOG.trace("delete(List<Delete>)");
    getBatchExecutor().batch(deletes);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    return checkAndDelete(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, delete);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
    CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {

    CheckAndMutateRowRequest.Builder requestBuilder =
        makeConditionalMutationRequestBuilder(
            row,
            family,
            qualifier,
            compareOp,
            value,
            delete.getRow(),
            hbaseAdapter.adapt(delete).getMutationsList());

    try {
      CheckAndMutateRowResponse response =
          client.checkAndMutateRow(requestBuilder.build());
      return wasMutationApplied(requestBuilder, response);
    } catch (Throwable t) {
      throw logAndCreateIOException("checkAndDelete", row, t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndMutate(
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp, final byte [] value, final RowMutations rm)
      throws IOException {
    List<Mutation> adaptedMutations = new ArrayList<>();
    for (org.apache.hadoop.hbase.client.Mutation mut : rm.getMutations()) {
      adaptedMutations.addAll(hbaseAdapter.adapt(mut).getMutationsList());
    }

    CheckAndMutateRowRequest.Builder requestBuilder =
        makeConditionalMutationRequestBuilder(
            row,
            family,
            qualifier,
            compareOp,
            value,
            rm.getRow(),
            adaptedMutations);

    try {
      CheckAndMutateRowResponse response =
          client.checkAndMutateRow(requestBuilder.build());
      return wasMutationApplied(requestBuilder, response);
    } catch (Throwable t) {
      throw logAndCreateIOException("checkAndMutate", row, t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    LOG.trace("mutateRow(RowMutation)");
    MutateRowRequest request = hbaseAdapter.adapt(rm);
    try {
      client.mutateRow(request);
    } catch (Throwable t) {
      throw logAndCreateIOException("mutateRow", rm.getRow(), t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result append(Append append) throws IOException {
    LOG.trace("append(Append)");

    ReadModifyWriteRowRequest request = hbaseAdapter.adapt(append);
    try {
      ReadModifyWriteRowResponse response = client.readModifyWriteRow(request);
      // The bigtable API will always return the mutated results. In order to maintain
      // compatibility, simply return null when results were not requested.
      if (append.isReturnResults()) {
        return Adapters.ROW_ADAPTER.adaptResponse(response.getRow());
      } else {
        return null;
      }
    } catch (Throwable t) {
      throw logAndCreateIOException("append", append.getRow(), t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result increment(Increment increment) throws IOException {
    LOG.trace("increment(Increment)");

    ReadModifyWriteRowRequest request = hbaseAdapter.adapt(increment);
    try {
      return Adapters.ROW_ADAPTER.adaptResponse(client.readModifyWriteRow(request).getRow());
    } catch (Throwable t) {
      throw logAndCreateIOException("increment", increment.getRow(), t);
    }
  }

  private IOException logAndCreateIOException(String type, byte[] row, Throwable t) {
    LOG.error("Encountered exception when executing " + type + ".", t);
    return new IOException(
        makeGenericExceptionMessage(
            type,
            options.getProjectId(),
            tableName.getQualifierAsString(),
            row),
        t);
  }

  /** {@inheritDoc} */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    LOG.trace("incrementColumnValue(byte[], byte[], byte[], long)");
    Increment incr = new Increment(row);
    incr.addColumn(family, qualifier, amount);
    Result result = increment(incr);

    Cell cell = result.getColumnLatestCell(family, qualifier);
    if (cell == null) {
      LOG.error("Failed to find a incremented value in result of increment");
      throw new IOException(
          makeGenericExceptionMessage(
              "increment",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              row));
    }
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  /** {@inheritDoc} */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    LOG.trace("incrementColumnValue(byte[], byte[], byte[], long, Durability)");
    return incrementColumnValue(row, family, qualifier, amount);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // TODO: shutdown the executor.
  }

  /** {@inheritDoc} */
  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    LOG.error("Unsupported coprocessorService(byte[]) called.");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
    LOG.error("Unsupported coprocessorService(Class, byte[], byte[], Batch.Call) called.");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    LOG.error("Unsupported coprocessorService("
        + "Class, byte[], byte[], Batch.Call, Batch.Callback) called.");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public long getWriteBufferSize() {
    LOG.error("Unsupported getWriteBufferSize() called");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    LOG.error("Unsupported getWriteBufferSize() called");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2,
      R r) throws ServiceException, Throwable {
    LOG.error("Unsupported batchCoprocessorService("
        + "MethodDescriptor, Message, byte[], byte[], R) called.");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2,
      R r, Batch.Callback<R> rCallback) throws ServiceException, Throwable {
    LOG.error("Unsupported batchCoprocessorService("
        + "MethodDescriptor, Message, byte[], byte[], R, Batch.Callback<R>) called.");
    throw new UnsupportedOperationException();  // TODO
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(BigtableTable.class)
        .add("hashCode", "0x" + Integer.toHexString(hashCode()))
        .add("project", options.getProjectId())
        .add("instance", options.getInstanceId())
        .add("table", tableName.getNameAsString())
        .add("host", options.getDataHost())
        .toString();
  }

  /**
   * <p>wasMutationApplied.</p>
   *
   * @param requestBuilder a {@link com.google.bigtable.v2.CheckAndMutateRowRequest.Builder} object.
   * @param response a {@link com.google.bigtable.v2.CheckAndMutateRowResponse} object.
   * @return a boolean.
   */
  protected boolean wasMutationApplied(
      CheckAndMutateRowRequest.Builder requestBuilder,
      CheckAndMutateRowResponse response) {

    // If we have true mods, we want the predicate to have matched.
    // If we have false mods, we did not want the predicate to have matched.
    return (requestBuilder.getTrueMutationsCount() > 0
        && response.getPredicateMatched())
        || (requestBuilder.getFalseMutationsCount() > 0
        && !response.getPredicateMatched());
  }

  /**
   * <p>makeConditionalMutationRequestBuilder.</p>
   *
   * @param row an array of byte.
   * @param family an array of byte.
   * @param qualifier an array of byte.
   * @param compareOp a {@link org.apache.hadoop.hbase.filter.CompareFilter.CompareOp} object.
   * @param value an array of byte.
   * @param actionRow an array of byte.
   * @param mutations a {@link java.util.List} object.
   * @return a {@link com.google.bigtable.v2.CheckAndMutateRowRequest.Builder} object.
   * @throws java.io.IOException if any.
   */
  protected CheckAndMutateRowRequest.Builder makeConditionalMutationRequestBuilder(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareFilter.CompareOp compareOp,
      byte[] value,
      byte[] actionRow,
      List<com.google.bigtable.v2.Mutation> mutations) throws IOException {

    if (!Arrays.equals(actionRow, row)) {
      // The following odd exception message is for compatibility with HBase.
      throw new DoNotRetryIOException("Action's getRow must match the passed row");
    }

    CheckAndMutateRowRequest.Builder requestBuilder =
        CheckAndMutateRowRequest.newBuilder();

    requestBuilder.setTableName(hbaseAdapter.getBigtableTableName().toString());

    requestBuilder.setRowKey(ByteString.copyFrom(row));
    Scan scan = new Scan().addColumn(family, qualifier);
    scan.setMaxVersions(1);
    if (value == null) {
      // If we don't have a value and we are doing CompareOp.EQUAL, we want to mutate if there
      // is no cell with the qualifier. If we are doing CompareOp.NOT_EQUAL, we want to mutate
      // if there is any cell. We don't actually want an extra filter for either of these cases,
      // but we do need to invert the compare op.
      if (CompareFilter.CompareOp.EQUAL.equals(compareOp)) {
        requestBuilder.addAllFalseMutations(mutations);
      } else if (CompareFilter.CompareOp.NOT_EQUAL.equals(compareOp)) {
        requestBuilder.addAllTrueMutations(mutations);
      }
    } else {
      ValueFilter valueFilter =
          new ValueFilter(compareOp, new BinaryComparator(value));
      scan.setFilter(valueFilter);
      requestBuilder.addAllTrueMutations(mutations);
    }
    requestBuilder.setPredicateFilter(Adapters.SCAN_ADAPTER.buildFilter(scan,
      UNSUPPORTED_READ_HOOKS));
    return requestBuilder;
  }

  static String makeGenericExceptionMessage(String operation, String projectId, String tableName) {
    return String.format(
        "Failed to perform operation. Operation='%s', projectId='%s', tableName='%s'",
        operation,
        projectId,
        tableName);
  }

  static String makeGenericExceptionMessage(
      String operation,
      String projectId,
      String tableName,
      byte[] rowKey) {
    return String.format(
        "Failed to perform operation. Operation='%s', projectId='%s', tableName='%s', rowKey='%s'",
        operation,
        projectId,
        tableName,
        Bytes.toStringBinary(rowKey));
  }

  /**
   * <p>Getter for the field <code>batchExecutor</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.hbase.BatchExecutor} object.
   */
  protected synchronized BatchExecutor getBatchExecutor() {
    if (batchExecutor == null) {
      batchExecutor = new BatchExecutor(this.bigtableConnection.getSession(), hbaseAdapter);
    }
    return batchExecutor;
  }
}
