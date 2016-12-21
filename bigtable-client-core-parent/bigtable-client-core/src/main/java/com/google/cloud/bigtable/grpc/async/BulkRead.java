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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

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
  private final String tableName;

  /**
   * ReadRowRequests have to be batched based on the {@link RowFilter} since {@link ReadRowsRequest}
   * only support a single RowFilter.
   */
  private RowFilter currentFilter;

  /**
   * Maps row keys to a collection of {@link SettableFuture}s that will be populated once the batch
   * operation is complete. The value of the {@link Multimap} is a {@link SettableFuture} of
   * a {@link List} of {@link FlatRow}s.  The {@link Multimap} is used because a user could request
   * the same key multiple times in the same batch. The {@link List} of {@link FlatRow}s mimics the
   * interface of {@link BigtableDataClient#readRowsAsync(ReadRowsRequest)}.
   */
  private Multimap<ByteString, SettableFuture<List<FlatRow>>> futures;

  /**
   * <p>Constructor for BulkRead.</p>
   *
   * @param client a {@link com.google.cloud.bigtable.grpc.BigtableDataClient} object.
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   */
  public BulkRead(BigtableDataClient client, BigtableTableName tableName) {
    this.client = client;
    this.tableName = tableName.toString();
  }

  /**
   * Adds the key in the request to a list of to look up in a batch read.
   *
   * @param request a {@link com.google.bigtable.v2.ReadRowsRequest} with a single row key.
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will be populated
   *     with the {@link FlatRow} that corresponds to the request
   */
  public ListenableFuture<List<FlatRow>> add(ReadRowsRequest request) {
    Preconditions.checkNotNull(request);
    Preconditions.checkArgument(request.getRows().getRowKeysCount() == 1);
    ByteString rowKey = request.getRows().getRowKeysList().get(0);
    Preconditions.checkArgument(!rowKey.equals(ByteString.EMPTY));

    RowFilter filter = request.getFilter();
    if (currentFilter == null) {
      currentFilter = filter;
    } else if (!filter.equals(currentFilter)) {
      // TODO: this should probably also happen if there is some maximum number of
      flush();
      currentFilter = filter;
    }
    if (futures == null) {
      futures = HashMultimap.create();
    }
    SettableFuture<List<FlatRow>> future = SettableFuture.create();
    futures.put(rowKey, future);
    return future;
  }

  /**
   * Sends all remaining requests to the server. This method does not wait for the method to
   * complete.
   */
  public void flush() {
    if (futures != null && !futures.isEmpty()) {
      try {
        ResultScanner<FlatRow> scanner = client.readFlatRows(ReadRowsRequest.newBuilder()
          .setTableName(tableName)
          .setFilter(currentFilter)
          .setRows(RowSet.newBuilder().addAllRowKeys(futures.keys()).build())
          .build());
        while (true) {
          FlatRow row = scanner.next();
          if (row == null) {
            break;
          }
          Collection<SettableFuture<List<FlatRow>>> rowFutures = futures.get(row.getRowKey());
          if (rowFutures != null) {
            // TODO: What about missing keys?
            for (SettableFuture<List<FlatRow>> rowFuture : rowFutures) {
              rowFuture.set(ImmutableList.of(row));
            }
            futures.removeAll(row.getRowKey());
          } else {
            LOG.warn("Found key: %s, but it was not in the original request.", row.getRowKey());
          }
        }
        for (Entry<ByteString, SettableFuture<List<FlatRow>>> entry : futures.entries()) {
          entry.getValue().set(ImmutableList.<FlatRow> of());
        }
      } catch (IOException e) {
        for (Entry<ByteString, SettableFuture<List<FlatRow>>> entry : futures.entries()) {
          entry.getValue().setException(e);
        }
      }
    }
    futures = null;
    currentFilter = null;
  }
}
