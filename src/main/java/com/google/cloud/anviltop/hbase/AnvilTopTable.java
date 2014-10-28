/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServices;
import com.google.cloud.anviltop.hbase.adapters.AnviltopResultScannerAdapter;
import com.google.cloud.anviltop.hbase.adapters.AppendAdapter;
import com.google.cloud.anviltop.hbase.adapters.AppendResponseAdapter;
import com.google.cloud.anviltop.hbase.adapters.DeleteAdapter;
import com.google.cloud.anviltop.hbase.adapters.GetAdapter;
import com.google.cloud.anviltop.hbase.adapters.GetRowResponseAdapter;
import com.google.cloud.anviltop.hbase.adapters.IncrementAdapter;
import com.google.cloud.anviltop.hbase.adapters.IncrementRowResponseAdapter;
import com.google.cloud.anviltop.hbase.adapters.MutationAdapter;
import com.google.cloud.anviltop.hbase.adapters.PutAdapter;
import com.google.cloud.anviltop.hbase.adapters.RowAdapter;
import com.google.cloud.anviltop.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.anviltop.hbase.adapters.ScanAdapter;
import com.google.cloud.anviltop.hbase.adapters.UnsupportedOperationAdapter;
import com.google.cloud.hadoop.hbase.AnviltopClient;
import com.google.cloud.hadoop.hbase.AnviltopResultScanner;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.cloud.hadoop.hbase.repackaged.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class AnvilTopTable implements HTableInterface {
  protected final TableName tableName;
  protected final AnviltopOptions options;
  protected final AnviltopClient client;
  protected final RowAdapter rowAdapter = new RowAdapter();
  protected final PutAdapter putAdapter;
  protected final AppendAdapter appendAdapter = new AppendAdapter();
  protected final AppendResponseAdapter appendRespAdapter = new AppendResponseAdapter(rowAdapter);
  protected final IncrementAdapter incrementAdapter = new IncrementAdapter();
  protected final IncrementRowResponseAdapter incrRespAdapter = new IncrementRowResponseAdapter(rowAdapter);
  protected final DeleteAdapter deleteAdapter = new DeleteAdapter();
  protected final MutationAdapter mutationAdapter;
  protected final RowMutationsAdapter rowMutationsAdapter;
  protected final GetAdapter getAdapter = new GetAdapter();
  protected final GetRowResponseAdapter getRowResponseAdapter = new GetRowResponseAdapter(rowAdapter);
  protected final ScanAdapter scanAdapter = new ScanAdapter();
  protected final AnviltopResultScannerAdapter anviltopResultScannerAdapter =
      new AnviltopResultScannerAdapter(rowAdapter);
  protected final Configuration configuration;
  protected final BatchExecutor batchExecutor;
  private final ExecutorService executorService;

  /**
   * Constructed by AnvilTopConnection
   *
   * @param tableName
   * @param client
   */
  public AnvilTopTable(TableName tableName,
      AnviltopOptions options,
      Configuration configuration,
      AnviltopClient client,
      ExecutorService executorService) {
    this.tableName = tableName;
    this.options = options;
    this.client = client;
    this.configuration = configuration;
    putAdapter = new PutAdapter(configuration);
    mutationAdapter = new MutationAdapter(
        deleteAdapter,
        putAdapter,
        new UnsupportedOperationAdapter<Increment>("increment"),
        new UnsupportedOperationAdapter<Append>("append"));
    rowMutationsAdapter = new RowMutationsAdapter(mutationAdapter);
    this.executorService = executorService;
    this.batchExecutor = new BatchExecutor(
        client,
        options,
        tableName,
        executorService,
        getAdapter,
        getRowResponseAdapter,
        putAdapter,
        deleteAdapter,
        rowMutationsAdapter,
        appendAdapter,
        appendRespAdapter,
        incrementAdapter,
        incrRespAdapter);

  }

  @Override
  public byte[] getTableName() {
    return this.tableName.getName();
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean exists(Get get) throws IOException {
    // TODO: make use of strip_value() or count to hopefully return no extra data
    Result result = get(get);
    return !result.isEmpty();
  }

  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    Boolean[] existsObjects = exists(gets);
    boolean[] exists = new boolean[existsObjects.length];
    for (int i = 0; i < existsObjects.length; i++) {
      exists[i] = existsObjects[i];
    }
    return exists;
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    return batchExecutor.exists(gets);
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    batchExecutor.batch(actions, results);
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return batchExecutor.batch(actions);
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    batchExecutor.batchCallback(actions, results, callback);
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    return batchExecutor.batchCallback(actions, callback);
  }

  @Override
  public Result get(Get get) throws IOException {
    AnviltopServices.GetRowRequest.Builder getRowRequest = getAdapter.adapt(get);
    getRowRequest
        .setProjectId(options.getProjectId())
        .setTableName(tableName.getQualifierAsString());

    try {
      AnviltopServices.GetRowResponse response = client.getRow(getRowRequest.build());

      return getRowResponseAdapter.adaptResponse(response);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "get",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              getRowRequest.getRowKey().toByteArray()),
          e);
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return batchExecutor.get(gets);
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    AnviltopServices.ReadTableRequest.Builder request = scanAdapter.adapt(scan);
    request.setProjectId(options.getProjectId());
    request.setTableName(tableName.getQualifierAsString());

    try {
      AnviltopResultScanner scanner = client.readTable(request.build());
      return anviltopResultScannerAdapter.adapt(scanner);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "getScanner",
              options.getProjectId(),
              tableName.getQualifierAsString()),
          e);
    }
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getScanner(new Scan().addFamily(family));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  @Override
  public void put(Put put) throws IOException {
    AnviltopData.RowMutation.Builder rowMutation = putAdapter.adapt(put);
    AnviltopServices.MutateRowRequest.Builder request = makeMutateRowRequest(rowMutation);

    try {
      client.mutateAtomic(request.build());
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "put",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              rowMutation.getRowKey().toByteArray()),
          e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    batchExecutor.put(puts);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean checkAndPut(byte[] bytes, byte[] bytes2, byte[] bytes3,
      CompareFilter.CompareOp compareOp, byte[] bytes4, Put put) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void delete(Delete delete) throws IOException {
    AnviltopData.RowMutation.Builder rowMutation = deleteAdapter.adapt(delete);
    AnviltopServices.MutateRowRequest.Builder request = makeMutateRowRequest(rowMutation);

    try {
      client.mutateAtomic(request.build());
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "delete",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              rowMutation.getRowKey().toByteArray()),
          e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    batchExecutor.delete(deletes);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public boolean checkAndDelete(byte[] bytes, byte[] bytes2, byte[] bytes3,
    CompareFilter.CompareOp compareOp, byte[] bytes4, Delete delete) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    AnviltopData.RowMutation.Builder rowMutation = rowMutationsAdapter.adapt(rm);
    AnviltopServices.MutateRowRequest.Builder request = makeMutateRowRequest(rowMutation);

    try {
      client.mutateAtomic(request.build());
    } catch (ServiceException e) {
      throw new IOException("Failed to mutate.", e);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    AnviltopServices.AppendRowRequest.Builder appendRowRequest = appendAdapter.adapt(append);
    appendRowRequest
        .setProjectId(options.getProjectId())
        .setTableName(tableName.getQualifierAsString());

    try {
      AnviltopServices.AppendRowResponse response = client.appendRow(appendRowRequest.build());
      return appendRespAdapter.adaptResponse(response);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "append",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              append.getRow()),
          e);
    }
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    AnviltopServices.IncrementRowRequest.Builder incrementRowRequest = incrementAdapter.adapt(
        increment);
    incrementRowRequest
        .setProjectId(options.getProjectId())
        .setTableName(tableName.getQualifierAsString());

    try {
      AnviltopServices.IncrementRowResponse response = client.incrementRow(
          incrementRowRequest.build());
      return incrRespAdapter.adaptResponse(response);
    } catch (ServiceException e) {
      throw new IOException(
          makeGenericExceptionMessage(
              "increment",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              increment.getRow()),
          e);
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    Increment incr = new Increment(row);
    incr.addColumn(family, qualifier, amount);
    Result result = increment(incr);

    Cell cell = result.getColumnLatestCell(family, qualifier);
    if (cell == null) {
      throw new IOException(
          makeGenericExceptionMessage(
              "increment",
              options.getProjectId(),
              tableName.getQualifierAsString(),
              row));
    }
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    return incrementColumnValue(row, family, qualifier, amount);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      boolean writeToWAL) throws IOException {
    return incrementColumnValue(row, family, qualifier, amount);
  }

  @Override
  public boolean isAutoFlush() {
    return true;
  }

  @Override
  public void flushCommits() throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2,
      R r) throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2,
      R r, Batch.Callback<R> rCallback) throws ServiceException, Throwable {
    throw new UnsupportedOperationException();  // TODO
  }

  private AnviltopServices.MutateRowRequest.Builder makeMutateRowRequest(
      AnviltopData.RowMutation.Builder rowMutation) {
    return AnviltopServices.MutateRowRequest.newBuilder()
        .setProjectId(options.getProjectId())
        .setTableName(tableName.getQualifierAsString())
        .setMutation(rowMutation);
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
}
