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

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import io.opencensus.common.Scope;
import io.opencensus.trace.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
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
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
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
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.CheckAndMutateUtil;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

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
@SuppressWarnings("deprecation")
public abstract class AbstractBigtableTable implements Table {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractBigtableTable.class);

  private static final Tracer TRACER = Tracing.getTracer();

  private static class TableMetrics {
    Timer putTimer = BigtableClientMetrics.timer(MetricLevel.Info, "table.put.latency");
    Timer getTimer = BigtableClientMetrics.timer(MetricLevel.Info, "table.get.latency");
  }

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
  protected final RequestContext requestContext;

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
    this.requestContext = RequestContext.create(
        InstanceName.of(options.getProjectId(), options.getInstanceId()),
        options.getAppProfileId());
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
        Scope scope = TRACER.spanBuilder("BigtableTable.getTableDescriptor").startScopedSpan();
        Admin admin = this.bigtableConnection.getAdmin()) {
      return admin.getTableDescriptor(tableName);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean exists(Get get) throws IOException {
    try (Scope scope = TRACER.spanBuilder("BigtableTable.exists").startScopedSpan()) {
      LOG.trace("exists(Get)");
      return !convertToResult(getResults(GetAdapter.setCheckExistenceOnly(get), "exists")).isEmpty();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    LOG.trace("existsAll(Get)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.existsAll").startScopedSpan()) {
      addBatchSizeAnnotation(gets);
      List<Get> existGets = new ArrayList<>(gets.size());
      for(Get get : gets ){
        existGets.add(GetAdapter.setCheckExistenceOnly(get));
      }
      return getBatchExecutor().exists(existGets);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    LOG.trace("batch(List<>, Object[])");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batch").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      getBatchExecutor().batch(actions, results);
    }
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    LOG.trace("batch(List<>)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batch").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      return getBatchExecutor().batch(actions);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Object[], Batch.Callback)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batchCallback").startScopedSpan()) {
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
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batchCallback").startScopedSpan()) {
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
      try (Scope scope = TRACER.spanBuilder("BigtableTable.get").startScopedSpan()) {
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
    try (Scope scope = TRACER.spanBuilder("BigtableTable.get").startScopedSpan()) {
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
    try (Scope scope = TRACER.withSpan(span)) {
      com.google.cloud.bigtable.grpc.scanner.ResultScanner<FlatRow> scanner =
          client.readFlatRows(hbaseAdapter.adapt(scan));
      if (hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner, span);
      }
      return Adapters.BIGTABLE_RESULT_SCAN_ADAPTER.adapt(scanner, span);
    } catch (Throwable throwable) {
      LOG.error("Encountered exception when executing getScanner.", throwable);
      span.setStatus(Status.UNKNOWN);
      // Close the span only when throw an exception and not on finally because if no exception
      // the span will be ended by the adapter.
      span.end();
      throw new IOException(
          makeGenericExceptionMessage(
              "getScanner",
              options.getProjectId(),
              tableName.getQualifierAsString()),
          throwable);
    }
  }

  public static boolean hasWhileMatchFilter(Filter filter) {
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
    Span span = TRACER.spanBuilder("BigtableTable.put").startSpan();
    try (Scope scope = TRACER.withSpan(span);
        Timer.Context timerContext = metrics.putTimer.time()) {
      client.mutateRow(request);
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException("put", put.getRow(), t);
    } finally{
      span.end();
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
    ConditionalRowMutation conditionalRowMutation =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family)
            .qualifier(qualifier)
            .ifMatches(compareOp, value)
            .withPut(put)
            .build();

    CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    return checkAndMutate(row, request, "checkAndPut");
  }

  /** {@inheritDoc} */
  @Override
  public void delete(Delete delete) throws IOException {
    LOG.trace("delete(Delete)");
    Span span = TRACER.spanBuilder("BigtableTable.delete").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      MutateRowRequest request = hbaseAdapter.adapt(delete);
      client.mutateRow(request);
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException("delete", delete.getRow(), t);
    } finally {
      span.end();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    LOG.trace("delete(List<Delete>)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.delete").startScopedSpan()) {
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
    ConditionalRowMutation conditionalRowMutation =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family)
            .qualifier(qualifier)
            .ifMatches(compareOp, value)
            .withDelete(delete)
            .build();

    CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    return checkAndMutate(row, request, "checkAndDelete");
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndMutate(
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp, final byte [] value, final RowMutations rm)
      throws IOException {
    LOG.trace("checkAndMutate(byte[], byte[], byte[], CompareOp, byte[], RowMutations)");

    ConditionalRowMutation conditionalRowMutation =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family)
            .qualifier(qualifier)
            .ifMatches(compareOp, value)
            .withMutations(rm)
            .build();

    CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    return checkAndMutate(row, request, "checkAndMutate");
  }

  private boolean checkAndMutate(final byte[] row, CheckAndMutateRowRequest request, String type)
      throws IOException {
    Span span = TRACER.spanBuilder("BigtableTable." + type).startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      CheckAndMutateRowResponse response = client.checkAndMutateRow(request);
      return CheckAndMutateUtil.wasMutationApplied(request, response);
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException(type, row, t);
    } finally {
      span.end();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    LOG.trace("mutateRow(RowMutation)");
    Span span = TRACER.spanBuilder("BigtableTable.mutateRow").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      MutateRowRequest request = hbaseAdapter.adapt(rm);
      client.mutateRow(request);
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException("mutateRow", rm.getRow(), t);
    } finally {
      span.end();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result append(Append append) throws IOException {
    LOG.trace("append(Append)");
    Span span = TRACER.spanBuilder("BigtableTable.append").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
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
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException("append", append.getRow(), t);
    } finally {
      span.end();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result increment(Increment increment) throws IOException {
    LOG.trace("increment(Increment)");
    Span span = TRACER.spanBuilder("BigtableTable.increment").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      ReadModifyWriteRowRequest request = hbaseAdapter.adapt(increment);
      return Adapters.ROW_ADAPTER.adaptResponse(client.readModifyWriteRow(request).getRow());
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException("increment", increment.getRow(), t);
    } finally {
      span.end();
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
        Scope scope = TRACER.spanBuilder("BigtableTable.incrementColumnValue").startScopedSpan()) {
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
  public int getReadRpcTimeout() {
    throw new UnsupportedOperationException("getReadRpcTimeout");
  }

  @Override
  public void setReadRpcTimeout(int i) {
    throw new UnsupportedOperationException("setReadRpcTimeout");
  }

  @Override
  public int getWriteRpcTimeout() {
    throw new UnsupportedOperationException("getWriteRpcTimeout");
  }

  @Override
  public void setWriteRpcTimeout(int i) {
    throw new UnsupportedOperationException("setWriteRpcTimeout");
  }

  @Override
  public int getRpcTimeout() {
    throw new UnsupportedOperationException("getRpcTimeout");
  }
}
