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
import java.util.Collections;
import java.util.List;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
   * a {@link List} of {@link Row}s.  The {@link Multimap} is used because a user could request
   * the same key multiple times in the same batch. The {@link List} of {@link Row}s mimics the
   * interface of {@link BigtableDataClient#readRowsAsync(ReadRowsRequest)}.
   */
  private Multimap<ByteString, SettableFuture<List<Row>>> rowFutures;

  /**
   * Maps row keys to a collection of {@link SettableFuture}s that will be populated once the batch
   * operation is complete. The value of the {@link Multimap} is a {@link SettableFuture} of
   * a {@link List} of {@link Row}s.  The {@link Multimap} is used because a user could request
   * the same key multiple times in the same batch. The {@link List} of {@link Row}s mimics the
   * interface of {@link BigtableDataClient#readRowsAsync(ReadRowsRequest)}.
   */
  private Multimap<ByteString, SettableFuture<List<FlatRow>>> flatRowFutures;

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
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will be populated with the {@link com.google.bigtable.v2.Row} that
   *    corresponds to the request
   * @throws java.lang.InterruptedException if any.
   */
  @Deprecated
  public ListenableFuture<List<Row>> add(ReadRowsRequest request) throws InterruptedException {
    ByteString rowKey = getRowKey(request);
    if (rowFutures == null) {
      rowFutures = HashMultimap.create();
    }
    SettableFuture<List<Row>> future = SettableFuture.create();
    rowFutures.put(rowKey, future);
    return future;
  }

  /**
   * Adds the key in the request to a list of to look up in a batch read.
   *
   * @param request a {@link com.google.bigtable.v2.ReadRowsRequest} with a single row key.
   * @return a {@link com.google.common.util.concurrent.ListenableFuture} that will be populated with the {@link com.google.bigtable.v2.Row} that
   *    corresponds to the request
   * @throws java.lang.InterruptedException if any.
   */
  public ListenableFuture<List<FlatRow>> addFlatRow(ReadRowsRequest request) throws InterruptedException {
    ByteString rowKey = getRowKey(request);
    if (flatRowFutures == null) {
      flatRowFutures = HashMultimap.create();
    }
    SettableFuture<List<FlatRow>> future = SettableFuture.create();
    flatRowFutures.put(rowKey, future);
    return future;
  }

  private ByteString getRowKey(ReadRowsRequest request) {
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
    return rowKey;
  }


  /**
   * Sends all remaining requests to the server. This method does not wait for the method to
   * complete.
   */
  public void flush() {
    if (!isEmpty(rowFutures) || !isEmpty(flatRowFutures)) {
      try {
        ResultScanner<FlatRow> scanner = client.readFlatRows(ReadRowsRequest.newBuilder()
          .setTableName(tableName)
          .setFilter(currentFilter)
          .setRows(RowSet.newBuilder().addAllRowKeys(getKeys(rowFutures, flatRowFutures)).build())
          .build());
        while (true) {
          FlatRow row = scanner.next();
          if (row == null) {
            break;
          }
          if (flatRowFutures != null) {
            Collection<SettableFuture<List<FlatRow>>> rowFuturesCollection =
                flatRowFutures.get(row.getRowKey());
            if (rowFuturesCollection != null) {
              // TODO: What about missing keys?
              setValue(row, rowFuturesCollection);
              flatRowFutures.removeAll(row.getRowKey());
            } else {
              LOG.warn("Found key: %s, but it was not in the original request.", row.getRowKey());
            }
          }
          if (rowFutures != null) {
            Collection<SettableFuture<List<Row>>> rowFuturesCollection = rowFutures.get(row.getRowKey());
            if (rowFuturesCollection != null) {
              // TODO: What about missing keys?
              setValue(FlatRowConverter.convert(row), rowFuturesCollection);
              rowFutures.removeAll(row.getRowKey());
            } else {
              LOG.warn("Found key: %s, but it was not in the original request.", row.getRowKey());
            }
          }
        }
        if (rowFutures != null) {
          ImmutableList<Row> emptyList = ImmutableList.<Row> of();
          for (SettableFuture<List<Row>> entry : rowFutures.values()) {
            entry.set(emptyList);
          }
        }
        if (flatRowFutures != null) {
          ImmutableList<FlatRow> emptyList = ImmutableList.<FlatRow> of();
          for (SettableFuture<List<FlatRow>> entry : flatRowFutures.values()) {
            entry.set(emptyList);
          }
        }
      } catch (IOException e) {
        if (rowFutures != null) {
          for (SettableFuture<List<Row>> entry : rowFutures.values()) {
            entry.setException(e);
          }
        }
        if (flatRowFutures != null) {
          for (SettableFuture<List<FlatRow>> entry : flatRowFutures.values()) {
            entry.setException(e);
          }
        }
      }
    }
    rowFutures = null;
    flatRowFutures = null;
    currentFilter = null;
  }

  private <T> void setValue(T value, Collection<SettableFuture<List<T>>> rowFuturesCollection) {
    final ImmutableList<T> list = ImmutableList.of(value);
    for (SettableFuture<List<T>> rowFuture : rowFuturesCollection) {
      rowFuture.set(list);
    }
  }

  private Iterable<ByteString> getKeys(
      Multimap<ByteString, ?> futures1,
      Multimap<ByteString, ?> futures2) {
    if (isEmpty(futures1)) {
      if (isEmpty(futures2)) {
        return Collections.emptyList();
      } else {
        return futures2.keySet();
      }
    } else {
      if (isEmpty(futures2)) {
        return futures1.keySet();
      } else {
        return ImmutableSet.<ByteString> builder()
            .addAll(futures1.keySet())
            .addAll(futures2.keySet())
            .build();
      }
    }
  }

  private boolean isEmpty(Multimap<?, ?> multimap) {
    return multimap == null || multimap.isEmpty();
  }
}
