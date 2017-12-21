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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Service;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;

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
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * <p>BigtableTable class.</p>
 *
 * Scan methods return rows in key order.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public abstract class AbstractBigtableTable implements Table {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractBigtableTable.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private static class TableMetrics {
    Timer putTimer = BigtableClientMetrics.timer(MetricLevel.Info, "table.put.latency");
    Timer getTimer = BigtableClientMetrics.timer(MetricLevel.Info, "table.get.latency");
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

  private static void addBatchSizeAnnotation(Collection<?> c) {
    TRACER.getCurrentSpan().addAnnotation("batchSize",
      ImmutableMap.of("size", AttributeValue.longAttributeValue(c.size())));
  }

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
  public AbstractBigtableTable(AbstractBigtableConnection bigtableConnection,
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
    try (
        Closeable ss =
            TRACER.spanBuilder("BigtableTable.getTableDescriptor").startScopedSpan();
        Admin admin = this.bigtableConnection.getAdmin()) {
      return admin.getTableDescriptor(tableName);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean exists(Get get) throws IOException {
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.exists").startScopedSpan()) {
      LOG.trace("exists(Get)");
      return !convertToResult(getResults(addKeyOnlyFilter(get), "exists")).isEmpty();
    }
  }

  private Get addKeyOnlyFilter(Get get) {
    Get existsGet = new Get(get);
    if (get.getFilter() == null) {
      existsGet.setFilter(new KeyOnlyFilter());
    } else {
      existsGet.setFilter(new FilterList(get.getFilter(), new KeyOnlyFilter()));
    }
    return existsGet;
  }

  /** {@inheritDoc} */
  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    LOG.trace("existsAll(Get)");
    try (
        Closeable ss = TRACER.spanBuilder("BigtableTable.existsAll").startScopedSpan()) {
      addBatchSizeAnnotation(gets);
      List<Get> existGets = new ArrayList<>(gets.size());
      for(Get get : gets ){
        existGets.add(addKeyOnlyFilter(get));
      }
      return getBatchExecutor().exists(existGets);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    LOG.trace("batch(List<>, Object[])");
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.batch").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      getBatchExecutor().batch(actions, results);
    }
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    LOG.trace("batch(List<>)");
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.batch").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      return getBatchExecutor().batch(actions);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Object[], Batch.Callback)");
    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.batchCallback").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      getBatchExecutor().batchCallback(actions, results, callback);
    }
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Batch.Callback)");
    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.batchCallback").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      Object[] results = new Object[actions.size()];
      getBatchExecutor().batchCallback(actions, results, callback);
      return results;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    LOG.trace("get(List<>)");
    if (gets == null || gets.isEmpty()) {
      return new Result[0];
    } else if (gets.size() == 1) {
      try {
        return new Result[] { get(gets.get(0)) };
      } catch(IOException e) {
        throw createRetriesExhaustedWithDetailsException(e, gets.get(0));
      }
    } else {
      try (Closeable ss = TRACER.spanBuilder("BigtableTable.get").startScopedSpan()) {
        addBatchSizeAnnotation(gets);
        return getBatchExecutor().batch(gets);
      }
    }
  }

  private RetriesExhaustedWithDetailsException
      createRetriesExhaustedWithDetailsException(Throwable e, Row action) {
    return new RetriesExhaustedWithDetailsException(Arrays.asList(e), Arrays.asList(action),
        Arrays.asList(options.getDataHost().toString()));
  }

  /** {@inheritDoc} */
  @Override
  public Result get(Get get) throws IOException {
    LOG.trace("get(Get)");
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.get").startScopedSpan()) {
      return convertToResult(getResults(get, "get"));
    }
  }

  private FlatRow getResults(Get get, String method) throws IOException {
    try (Timer.Context ignored = metrics.getTimer.time()) {
      List<FlatRow> list = client.readFlatRowsList(hbaseAdapter.adapt(get));
      switch(list.size()) {
      case 0:
        return null;
      case 1:
        return list.get(0);
      default:
        throw new IllegalStateException("Multiple responses found for " + method);
      }
    }
  }

  protected Result convertToResult(FlatRow row) {
    if (row == null) {
      return Adapters.FLAT_ROW_ADAPTER.adaptResponse(null);
    } else {
      return Adapters.FLAT_ROW_ADAPTER.adaptResponse(row);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    LOG.trace("getScanner(Scan)");
    Span span = TRACER.spanBuilder("BigtableTable.scan").startSpan();
    try (Closeable c = TRACER.withSpan(span)) {
      com.google.cloud.bigtable.grpc.scanner.ResultScanner<FlatRow> scanner =
          client.readFlatRows(hbaseAdapter.adapt(scan));
      if (hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner, span);
      }
      return Adapters.BIGTABLE_RESULT_SCAN_ADAPTER.adapt(scanner, span);
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
    MutateRowRequest request = hbaseAdapter.adapt(put);
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.put").startScopedSpan();
        Timer.Context timerContext = metrics.putTimer.time()) {
      client.mutateRow(request);
    } catch (Throwable t) {
      throw logAndCreateIOException("put", put.getRow(), t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void put(List<Put> puts) throws IOException {
    LOG.trace("put(List<Put>)");
    if (puts == null || puts.isEmpty()) {
      return;
    } else if (puts.size() == 1) {
      try {
        put(puts.get(0));
      } catch (IOException e) {
        throw createRetriesExhaustedWithDetailsException(e, puts.get(0));
      }
    } else {
      getBatchExecutor().batch(puts);
    }
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
    LOG.trace("checkAndPut(byte[], byte[], byte[], CompareOp, value, Put)");
    CheckAndMutateRowRequest.Builder requestBuilder =
        makeConditionalMutationRequestBuilder(
            row,
            family,
            qualifier,
            compareOp,
            value,
            put.getRow(),
            hbaseAdapter.adapt(put).getMutationsList());

    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.checkAndPut").startScopedSpan()) {
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
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.delete").startScopedSpan()) {
      MutateRowRequest request = hbaseAdapter.adapt(delete);
      client.mutateRow(request);
    } catch (Throwable t) {
      throw logAndCreateIOException("delete", delete.getRow(), t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    LOG.trace("delete(List<Delete>)");
    try (Closeable ss = TRACER.spanBuilder("BigtableTable.delete").startScopedSpan()) {
      getBatchExecutor().batch(deletes);
    }
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
    LOG.trace("checkAndDelete(byte[], byte[], byte[], CompareOp, byte[], Delete)");
    CheckAndMutateRowRequest.Builder requestBuilder =
        makeConditionalMutationRequestBuilder(
            row,
            family,
            qualifier,
            compareOp,
            value,
            delete.getRow(),
            hbaseAdapter.adapt(delete).getMutationsList());

    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.checkAndDelete").startScopedSpan()) {
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
    LOG.trace("checkAndMutate(byte[], byte[], byte[], CompareOp, byte[], RowMutations)");

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

    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.checkAndMutate").startScopedSpan()) {
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
    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.mutateRow").startScopedSpan()) {
      MutateRowRequest request = hbaseAdapter.adapt(rm);
      client.mutateRow(request);
    } catch (Throwable t) {
      throw logAndCreateIOException("mutateRow", rm.getRow(), t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result append(Append append) throws IOException {
    LOG.trace("append(Append)");
    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.append").startScopedSpan()) {
      ReadModifyWriteRowRequest request = hbaseAdapter.adapt(append);
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
    try (Closeable ss =
        TRACER.spanBuilder("BigtableTable.increment").startScopedSpan()) {
      ReadModifyWriteRowRequest request = hbaseAdapter.adapt(increment);
      return Adapters.ROW_ADAPTER.adaptResponse(client.readModifyWriteRow(request).getRow());
    } catch (Throwable t) {
      throw logAndCreateIOException("increment", increment.getRow(), t);
    }
  }

  private IOException logAndCreateIOException(String type, byte[] row, Throwable t) {
    LOG.error("Encountered exception when executing " + type + ".", t);
    return new DoNotRetryIOException(
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
    try (
        Closeable ss = TRACER.spanBuilder("BigtableTable.incrementColumnValue").startScopedSpan()) {
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
    return MoreObjects.toStringHelper(AbstractBigtableTable.class)
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
          new ValueFilter(reverseCompareOp(compareOp), new BinaryComparator(value));
      scan.setFilter(valueFilter);
      requestBuilder.addAllTrueMutations(mutations);
    }
    requestBuilder.setPredicateFilter(Adapters.SCAN_ADAPTER.buildFilter(scan,
      UNSUPPORTED_READ_HOOKS));
    return requestBuilder;
  }

  /**
   * For some reason, the ordering of CheckAndMutate operations is the inverse order of normal
   * {@link ValueFilter} operations.
   *
   * @param compareOp
   * @return the inverse of compareOp
   */
  private static CompareOp reverseCompareOp(CompareOp compareOp) {
    switch (compareOp) {
    case EQUAL:
    case NOT_EQUAL:
    case NO_OP:
      return compareOp;
    case LESS:
      return CompareOp.GREATER;
    case LESS_OR_EQUAL:
      return CompareOp.GREATER_OR_EQUAL;
    case GREATER:
      return CompareOp.LESS;
    case GREATER_OR_EQUAL:
      return CompareOp.LESS_OR_EQUAL;
    default:
      return CompareOp.NO_OP;
    }
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
      batchExecutor = new BatchExecutor(bigtableConnection.getSession(), hbaseAdapter);
    }
    return batchExecutor;
  }


  @Override
  public void setOperationTimeout(int i) {
    throw new UnsupportedOperationException("setOperationTimeout");
  }

  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException("getOperationTimeout");
  }

  @Override
  public void setRpcTimeout(int i) {
    throw new UnsupportedOperationException("setRpcTimeout");
  }

  @Override
  public int getRpcTimeout() {
    throw new UnsupportedOperationException("getRpcTimeout");
  }
}
