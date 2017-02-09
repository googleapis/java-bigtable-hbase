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
package com.google.cloud.bigtable.grpc.async;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.util.concurrent.ExecutorService;

/**
 * This class combines a collection of {@link com.google.bigtable.v2.ReadRowsRequest}s with a single row key into a single
 * {@link com.google.bigtable.v2.ReadRowsRequest} with a {@link com.google.bigtable.v2.RowSet} which will result in fewer round trips. This class
 * is not thread safe, and requires calling classes to make it thread safe.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BulkRead {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BulkRead.class);

  private final BigtableDataClient client;
  private final ExecutorService threadPool;
  private final String tableName;

  private final Map<RowFilter, Batch> batches;


  /**
   * <p>Constructor for BulkRead.</p>
   *  @param client a {@link BigtableDataClient} object.
   * @param tableName a {@link BigtableTableName} object.
   * @param threadPool the {@link ExecutorService} to execute the batched reads on
   */
  public BulkRead(BigtableDataClient client, BigtableTableName tableName,
      ExecutorService threadPool) {
    this.client = client;
    this.threadPool = threadPool;
    this.tableName = tableName.toString();
    this.batches = new HashMap<>();
  }

  /**
   * Adds the key in the request to a batch read. The future will be resolved when the batch response
   * is received.
   *
   * @param request a {@link com.google.bigtable.v2.ReadRowsRequest} with a single row key.
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will be populated
   *     with the {@link FlatRow} that corresponds to the request
   */
  public ListenableFuture<FlatRow> add(ReadRowsRequest request) {
    Preconditions.checkNotNull(request);
    Preconditions.checkArgument(request.getRows().getRowKeysCount() == 1);
    ByteString rowKey = request.getRows().getRowKeysList().get(0);
    Preconditions.checkArgument(!rowKey.equals(ByteString.EMPTY));

    final RowFilter filter = request.getFilter();
    Batch batch = batches.get(filter);
    if (batch == null) {
      batch = new Batch(filter);
      batches.put(filter, batch);
    }
    return batch.addKey(rowKey);
  }

  /**
   * Sends all remaining requests to the server. This method does not wait for the method to
   * complete.
   */
  public void flush() {
    for (Batch batch : batches.values()) {
      threadPool.submit(batch);
    }
    batches.clear();
  }


  /**
   * ReadRowRequests have to be batched based on the {@link RowFilter} since {@link ReadRowsRequest}
   * only support a single RowFilter. A batch represents this grouping.
   */
  class Batch implements Runnable {
    private final RowFilter filter;
    /**
     * Maps row keys to a collection of {@link SettableFuture}s that will be populated once the batch
     * operation is complete. The value of the {@link Multimap} is a {@link SettableFuture} of
     * a {@link List} of {@link FlatRow}s.  The {@link Multimap} is used because a user could request
     * the same key multiple times in the same batch. The {@link List} of {@link FlatRow}s mimics the
     * interface of {@link BigtableDataClient#readRowsAsync(ReadRowsRequest)}.
     */
    private final Multimap<ByteString, SettableFuture<FlatRow>> futures;

    public Batch(RowFilter filter) {
      this.filter = filter;
      this.futures = HashMultimap.create();
    }

    public SettableFuture<FlatRow> addKey(ByteString rowKey) {
      SettableFuture<FlatRow> future = SettableFuture.create();
      futures.put(rowKey, future);
      return future;
    }

    /**
     * Sends the requests and resolves the futures using the response.
     */
    public void run() {
      try {
        ResultScanner<FlatRow> scanner = client.readFlatRows(ReadRowsRequest.newBuilder()
            .setTableName(tableName)
            .setFilter(filter)
            .setRows(RowSet.newBuilder().addAllRowKeys(futures.keys()).build())
            .build()
        );
        while (true) {
          FlatRow row = scanner.next();
          if (row == null) {
            break;
          }
          Collection<SettableFuture<FlatRow>> rowFutures = futures.get(row.getRowKey());
          if (rowFutures != null) {
            for (SettableFuture<FlatRow> rowFuture : rowFutures) {
              rowFuture.set(row);
            }
            futures.removeAll(row.getRowKey());
          } else {
            LOG.warn("Found key: %s, but it was not in the original request.", row.getRowKey());
          }
        }
        // Deal with remaining/missing keys
        for (Entry<ByteString, SettableFuture<FlatRow>> entry : futures.entries()) {
          entry.getValue().set(null);
        }
      } catch (Throwable e) {
        for (Entry<ByteString, SettableFuture<FlatRow>> entry : futures.entries()) {
          entry.getValue().setException(e);
        }
      }
    }
  }
}
