/*
 * Copyright (c) 2014 Google Inc.
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

package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase.adapters.AppendAdapter;
import com.google.cloud.bigtable.hbase.adapters.BigtableRowAdapter;
import com.google.cloud.bigtable.hbase.adapters.DeleteAdapter;
import com.google.cloud.bigtable.hbase.adapters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.IncrementAdapter;
import com.google.cloud.bigtable.hbase.adapters.MutationAdapter;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.adapters.RowMutationsAdapter;
import com.google.cloud.bigtable.hbase.adapters.ScanAdapter;
import com.google.cloud.bigtable.hbase.adapters.UnsupportedOperationAdapter;
import com.google.cloud.hadoop.hbase.BigtableClient;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class BigtableBufferedMutator implements BufferedMutator {

  protected static final Logger LOG = new Logger(BigtableBufferedMutator.class);

  private final Configuration configuration;
  private final TableName tableName;
  private List<Mutation> writeBuffer = new ArrayList<>();
  private boolean closed = false;
  private final int maxInflightRpcCount;
  private final long writeBufferSize;
  protected long currentWriteBufferSize = 0;

  protected final BatchExecutor batchExecutor;

  protected final ResponseAdapter<com.google.bigtable.v1.Row, Result> bigtableRowAdapter =
      new BigtableRowAdapter();
  protected final ScanAdapter scanAdapter = new ScanAdapter(new FilterAdapter());
  protected final GetAdapter getAdapter = new GetAdapter(scanAdapter);
  protected final DeleteAdapter deleteAdapter = new DeleteAdapter();

  protected final PutAdapter putAdapter;
  protected final MutationAdapter mutationAdapter;
  protected final RowMutationsAdapter rowMutationsAdapter;

  protected final ExceptionListener listener;

  /*
   * TODO(sduskis): These shouldn't be needed here. Let's separate out the functionality in
   * batchExecutor so that we can remove these.
   */
  protected final AppendAdapter appendAdapter = new AppendAdapter();
  protected final IncrementAdapter incrementAdapter = new IncrementAdapter();

  public BigtableBufferedMutator(Configuration configuration,
      TableName tableName,
      int maxInflightRpcs,
      long writeBufferSize,
      BigtableClient client,
      BigtableOptions options,
      ExecutorService executorService,
      BufferedMutator.ExceptionListener listener) {
    super();
    LOG.trace("CREATED with max in flight rpc count " + maxInflightRpcs);
    this.maxInflightRpcCount = maxInflightRpcs;
    this.writeBufferSize = writeBufferSize;
    this.configuration = configuration;
    this.tableName = tableName;
    this.listener = listener;

    putAdapter = new PutAdapter(configuration);
    mutationAdapter = new MutationAdapter(
        deleteAdapter,
        putAdapter,
        new UnsupportedOperationAdapter<Increment>("increment"),
        new UnsupportedOperationAdapter<Append>("append"));
    rowMutationsAdapter = new RowMutationsAdapter(mutationAdapter);

    this.batchExecutor = new BatchExecutor(client,
        options,
        TableMetadataSetter.from(tableName, options),
        MoreExecutors.listeningDecorator(executorService),
        getAdapter,
        putAdapter,
        deleteAdapter,
        rowMutationsAdapter,
        appendAdapter,
        incrementAdapter,
        bigtableRowAdapter);
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      flush();
      closed = true;
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (closed) {
      throw new IllegalStateException("Cannot flush when the BufferedMutator is closed.");
    }
    if (!writeBuffer.isEmpty()) {
      List<Row> copy = Lists.<Row> newArrayList(writeBuffer);
      for (List<Row> partition : Lists.partition(copy, this.maxInflightRpcCount)) {
        writeBuffer.removeAll(partition);
        try {
          batchExecutor.batch(partition);
        } catch (RetriesExhaustedWithDetailsException e) {
          listener.onException(e, this);
        } catch (IOException e) {
          List<Throwable> problems = Lists.<Throwable> newArrayList(e);
          ArrayList<String> hostnames = new ArrayList<String>(1);
          listener.onException(
              new RetriesExhaustedWithDetailsException(problems, partition, hostnames), this);
        }
      }
      currentWriteBufferSize = 0;
      for (Mutation m : writeBuffer) {
        currentWriteBufferSize += m.heapSize();
      }
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public synchronized long getWriteBufferSize() {
    return currentWriteBufferSize;
  }

  @Override
  public synchronized void mutate(Mutation mutation) throws IOException {
    if (closed) {
      throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
    }
    writeBuffer.add(mutation);
    currentWriteBufferSize += mutation.heapSize();
    flushIfNecessary();
  }

  @Override
  public synchronized void mutate(List<? extends Mutation> mutations) throws IOException {
    if (closed) {
      throw new IllegalStateException("Cannot mutate when the BufferedMutator is closed.");
    }
    writeBuffer.addAll(mutations);
    for (Mutation m : mutations) {
      currentWriteBufferSize += m.heapSize();
    }
    flushIfNecessary();
  }

  private void flushIfNecessary() throws IOException {
    if (writeBuffer.size() > maxInflightRpcCount ||
        currentWriteBufferSize > writeBufferSize) {
      flush();
    }
  }

}
