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
package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.CheckAndMutateUtil;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.FutureUtil;
import com.google.cloud.bigtable.hbase.util.Logger;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
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
import org.apache.hadoop.hbase.client.Mutation;
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
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Service;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * BigtableTable class. Scan methods return rows in key order.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
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
    TRACER
        .getCurrentSpan()
        .addAnnotation(
            "batchSize", ImmutableMap.of("size", AttributeValue.longAttributeValue(c.size())));
  }

  protected final TableName tableName;
  protected final BigtableHBaseSettings settings;
  protected final HBaseRequestAdapter hbaseAdapter;

  protected final DataClientWrapper clientWrapper;
  protected final AbstractBigtableConnection bigtableConnection;
  private TableMetrics metrics = new TableMetrics();

  /**
   * Constructed by BigtableConnection
   *
   * @param bigtableConnection a {@link AbstractBigtableConnection} object.
   * @param hbaseAdapter a {@link HBaseRequestAdapter} object.
   */
  public AbstractBigtableTable(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter) {
    this.bigtableConnection = bigtableConnection;
    this.settings = bigtableConnection.getBigtableSettings();
    BigtableApi bigtableApi = bigtableConnection.getBigtableApi();
    this.clientWrapper = bigtableApi.getDataClient();
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
    return this.settings.getConfiguration();
  }

  /** {@inheritDoc} */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    try (Scope scope = TRACER.spanBuilder("BigtableTable.getTableDescriptor").startScopedSpan();
        Admin admin = this.bigtableConnection.getAdmin()) {
      return admin.getTableDescriptor(tableName);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean exists(Get get) throws IOException {
    try (Scope scope = TRACER.spanBuilder("BigtableTable.exists").startScopedSpan();
        Timer.Context ignored = metrics.getTimer.time()) {
      LOG.trace("exists(Get)");
      Filters.Filter filter =
          Adapters.GET_ADAPTER.buildFilter(GetAdapter.setCheckExistenceOnly(get));

      try {
        return !FutureUtil.unwrap(
                clientWrapper.readRowAsync(
                    tableName.getNameAsString(), ByteStringer.wrap(get.getRow()), filter))
            .isEmpty();
      } catch (Exception e) {
        throw createRetriesExhaustedWithDetailsException(e, get);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    LOG.trace("existsAll(Get)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.existsAll").startScopedSpan()) {
      addBatchSizeAnnotation(gets);
      List<Get> existGets = new ArrayList<>(gets.size());
      for (Get get : gets) {
        existGets.add(GetAdapter.setCheckExistenceOnly(get));
      }
      try (BatchExecutor executor = createBatchExecutor()) {
        return executor.exists(existGets);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    LOG.trace("batch(List<>, Object[])");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batch").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      try (BatchExecutor executor = createBatchExecutor()) {
        executor.batch(actions, results);
      }
    }
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    LOG.trace("batch(List<>)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batch").startScopedSpan()) {
      addBatchSizeAnnotation(actions);

      try (BatchExecutor executor = createBatchExecutor()) {
        return executor.batch(actions);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public <R> void batchCallback(
      List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    LOG.trace("batchCallback(List<>, Object[], Batch.Callback)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.batchCallback").startScopedSpan()) {
      addBatchSizeAnnotation(actions);
      try (BatchExecutor executor = createBatchExecutor()) {
        executor.batchCallback(actions, results, callback);
      }
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
      try (BatchExecutor executor = createBatchExecutor()) {
        executor.batchCallback(actions, results, callback);
      }
      return results;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    LOG.trace("get(List<>)");
    Preconditions.checkNotNull(gets);
    if (gets.isEmpty()) {
      return new Result[0];
    } else if (gets.size() == 1) {
      try {
        return new Result[] {get(gets.get(0))};
      } catch (IOException e) {
        throw createRetriesExhaustedWithDetailsException(e, gets.get(0));
      }
    } else {
      try (Scope scope = TRACER.spanBuilder("BigtableTable.get").startScopedSpan()) {
        addBatchSizeAnnotation(gets);
        try (BatchExecutor executor = createBatchExecutor()) {
          return executor.batch(gets);
        }
      }
    }
  }

  private RetriesExhaustedWithDetailsException createRetriesExhaustedWithDetailsException(
      Throwable e, Row action) {
    return new RetriesExhaustedWithDetailsException(
        Arrays.asList(e), Arrays.asList(action), Arrays.asList(settings.getDataHost()));
  }

  /** {@inheritDoc} */
  @Override
  public Result get(Get get) throws IOException {
    LOG.trace("get(Get)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.get").startScopedSpan();
        Timer.Context ignored = metrics.getTimer.time()) {

      Filters.Filter filter = Adapters.GET_ADAPTER.buildFilter(get);
      try {
        return FutureUtil.unwrap(
            clientWrapper.readRowAsync(
                tableName.getNameAsString(), ByteStringer.wrap(get.getRow()), filter));
      } catch (Exception e) {
        throw createRetriesExhaustedWithDetailsException(e, get);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    LOG.trace("getScanner(Scan)");
    Span span = TRACER.spanBuilder("BigtableTable.scan").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {

      final ResultScanner scanner = clientWrapper.readRows(hbaseAdapter.adapt(scan));
      if (hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner, span);
      }
      // TODO: need to end the span when stream ends
      return scanner;
    } catch (Throwable throwable) {
      LOG.error("Encountered exception when executing getScanner.", throwable);
      span.setStatus(Status.UNKNOWN);
      // Close the span only when throw an exception and not on finally because if no exception
      // the span will be ended by the adapter.
      span.end();
      throw new IOException(
          makeGenericExceptionMessage(
              "getScanner", settings.getProjectId(), tableName.getQualifierAsString()),
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
    RowMutation rowMutation = hbaseAdapter.adapt(put);
    mutateRow(put, rowMutation, "put");
  }

  /** {@inheritDoc} */
  @Override
  public void put(List<Put> puts) throws IOException {
    LOG.trace("put(List<Put>)");
    Preconditions.checkNotNull(puts);
    if (puts.isEmpty()) {
      return;
    } else if (puts.size() == 1) {
      try {
        put(puts.get(0));
      } catch (IOException e) {
        throw createRetriesExhaustedWithDetailsException(e, puts.get(0));
      }
    } else {
      try (BatchExecutor executor = createBatchExecutor()) {
        executor.batch(puts);
      }
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
  public boolean checkAndPut(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareFilter.CompareOp compareOp,
      byte[] value,
      Put put)
      throws IOException {
    LOG.trace("checkAndPut(byte[], byte[], byte[], CompareOp, value, Put)");
    ConditionalRowMutation request =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family)
            .qualifier(qualifier)
            .ifMatches(compareOp, value)
            .withPut(put)
            .build();

    return checkAndMutate(row, request, "checkAndPut");
  }

  /** {@inheritDoc} */
  @Override
  public void delete(Delete delete) throws IOException {
    LOG.trace("delete(Delete)");
    RowMutation rowMutation = hbaseAdapter.adapt(delete);
    mutateRow(delete, rowMutation, "delete");
  }

  /** {@inheritDoc} */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    LOG.trace("delete(List<Delete>)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.delete").startScopedSpan();
        BatchExecutor executor = createBatchExecutor()) {
      executor.batch(deletes, true);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    return checkAndDelete(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, delete);
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndDelete(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareFilter.CompareOp compareOp,
      byte[] value,
      Delete delete)
      throws IOException {
    LOG.trace("checkAndDelete(byte[], byte[], byte[], CompareOp, byte[], Delete)");
    ConditionalRowMutation request =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family)
            .qualifier(qualifier)
            .ifMatches(compareOp, value)
            .withDelete(delete)
            .build();

    return checkAndMutate(row, request, "checkAndDelete");
  }

  /** {@inheritDoc} */
  @Override
  public boolean checkAndMutate(
      final byte[] row,
      final byte[] family,
      final byte[] qualifier,
      final CompareFilter.CompareOp compareOp,
      final byte[] value,
      final RowMutations rm)
      throws IOException {
    LOG.trace("checkAndMutate(byte[], byte[], byte[], CompareOp, byte[], RowMutations)");

    ConditionalRowMutation request =
        new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family)
            .qualifier(qualifier)
            .ifMatches(compareOp, value)
            .withMutations(rm)
            .build();

    return checkAndMutate(row, request, "checkAndMutate");
  }

  private boolean checkAndMutate(final byte[] row, ConditionalRowMutation request, String type)
      throws IOException {
    Span span = TRACER.spanBuilder("BigtableTable." + type).startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      Boolean wasApplied = FutureUtil.unwrap(clientWrapper.checkAndMutateRowAsync(request));
      return CheckAndMutateUtil.wasMutationApplied(request, wasApplied);
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException(type, row, t);
    } finally {
      span.end();
    }
  }

  private void mutateRow(Mutation mutation, RowMutation rowMutation, String type)
      throws IOException {
    Span span = TRACER.spanBuilder("BigtableTable." + type).startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      FutureUtil.unwrap(clientWrapper.mutateRowAsync(rowMutation));
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException(type, mutation.getRow(), t);
    } finally {
      span.end();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    LOG.trace("mutateRow(RowMutation)");
    if (rowMutations.getMutations().isEmpty()) {
      return;
    }
    Span span = TRACER.spanBuilder("BigtableTable.mutateRow").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      FutureUtil.unwrap(clientWrapper.mutateRowAsync(hbaseAdapter.adapt(rowMutations)));
    } catch (Throwable t) {
      span.setStatus(Status.UNKNOWN);
      throw logAndCreateIOException("mutateRow", rowMutations.getRow(), t);
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
      Result response =
          FutureUtil.unwrap(clientWrapper.readModifyWriteRowAsync(hbaseAdapter.adapt(append)));
      // The bigtable API will always return the mutated results. In order to maintain
      // compatibility, simply return null when results were not requested.
      if (append.isReturnResults()) {
        return response;
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
      ReadModifyWriteRow request = hbaseAdapter.adapt(increment);
      return FutureUtil.unwrap(clientWrapper.readModifyWriteRowAsync(request));
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
            type, settings.getProjectId(), tableName.getQualifierAsString(), row),
        t);
  }

  /** {@inheritDoc} */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    LOG.trace("incrementColumnValue(byte[], byte[], byte[], long)");
    try (Scope scope = TRACER.spanBuilder("BigtableTable.incrementColumnValue").startScopedSpan()) {
      Increment incr = new Increment(row);
      incr.addColumn(family, qualifier, amount);
      Result result = increment(incr);

      Cell cell = result.getColumnLatestCell(family, qualifier);
      if (cell == null) {
        LOG.error("Failed to find a incremented value in result of increment");
        throw new IOException(
            makeGenericExceptionMessage(
                "increment", settings.getProjectId(), tableName.getQualifierAsString(), row));
      }
      return Bytes.toLong(CellUtil.cloneValue(cell));
    }
  }

  /** {@inheritDoc} */
  @Override
  public long incrementColumnValue(
      byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
      throws IOException {
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
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable)
      throws ServiceException, Throwable {
    LOG.error("Unsupported coprocessorService(Class, byte[], byte[], Batch.Call) called.");
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <T extends Service, R> void coprocessorService(
      Class<T> service,
      byte[] startKey,
      byte[] endKey,
      Batch.Call<T, R> callable,
      Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    LOG.error(
        "Unsupported coprocessorService("
            + "Class, byte[], byte[], Batch.Call, Batch.Callback) called.");
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public long getWriteBufferSize() {
    LOG.error("Unsupported getWriteBufferSize() called");
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Deprecated
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    LOG.error("Unsupported getWriteBufferSize() called");
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes2,
      R r)
      throws ServiceException, Throwable {
    LOG.error(
        "Unsupported batchCoprocessorService("
            + "MethodDescriptor, Message, byte[], byte[], R) called.");
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes2,
      R r,
      Batch.Callback<R> rCallback)
      throws ServiceException, Throwable {
    LOG.error(
        "Unsupported batchCoprocessorService("
            + "MethodDescriptor, Message, byte[], byte[], R, Batch.Callback<R>) called.");
    throw new UnsupportedOperationException(); // TODO
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(AbstractBigtableTable.class)
        .add("hashCode", "0x" + Integer.toHexString(hashCode()))
        .add("project", settings.getProjectId())
        .add("instance", settings.getInstanceId())
        .add("table", tableName.getNameAsString())
        .add("host", settings.getDataHost())
        .toString();
  }

  static String makeGenericExceptionMessage(String operation, String projectId, String tableName) {
    return String.format(
        "Failed to perform operation. Operation='%s', projectId='%s', tableName='%s'",
        operation, projectId, tableName);
  }

  static String makeGenericExceptionMessage(
      String operation, String projectId, String tableName, byte[] rowKey) {
    return String.format(
        "Failed to perform operation. Operation='%s', projectId='%s', tableName='%s', rowKey='%s'",
        operation, projectId, tableName, Bytes.toStringBinary(rowKey));
  }

  /**
   * Getter for the field <code>batchExecutor</code>.
   *
   * @return a {@link com.google.cloud.bigtable.hbase.BatchExecutor} object.
   */
  protected BatchExecutor createBatchExecutor() {
    return new BatchExecutor(bigtableConnection.getBigtableApi(), settings, hbaseAdapter);
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
