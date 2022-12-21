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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

/**
 * A {@link Get} adapter that transform the Get into a {@link Query} using the proto-based filter
 * language.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class GetAdapter implements ReadOperationAdapter<Get> {

  public static Get setCheckExistenceOnly(Get get) {
    if (get.isCheckExistenceOnly()) {
      return get;
    } else {
      Get existsGet = new Get(get);
      existsGet.setCheckExistenceOnly(true);
      return existsGet;
    }
  }

  protected final ScanAdapter scanAdapter;
  /**
   * Constructor for GetAdapter.
   *
   * @param scanAdapter a {@link ScanAdapter} object.
   */
  public GetAdapter(ScanAdapter scanAdapter) {
    this.scanAdapter = scanAdapter;
  }

  /** {@inheritDoc} */
  @Override
  public Query adapt(Get operation, ReadHooks readHooks, Query query) {
    Scan operationAsScan = new Scan(addKeyOnlyFilter(operation));
    scanAdapter.throwIfUnsupportedScan(operationAsScan);

    return query
        .filter(scanAdapter.buildFilter(operationAsScan, readHooks))
        .rowKey(ByteString.copyFrom(operation.getRow()));
  }

  /**
   * creates filter based on user provided conditions in {@link Get} request.
   *
   * @param operation a {@link Get} object.
   * @return a {@link Filters.Filter} object.
   */
  public Filters.Filter buildFilter(Get operation) {
    Scan operationAsScan = new Scan(addKeyOnlyFilter(operation));
    scanAdapter.throwIfUnsupportedScan(operationAsScan);

    return scanAdapter.buildFilter(operationAsScan, new DefaultReadHooks());
  }

  private Get addKeyOnlyFilter(Get get) {
    if (get.isCheckExistenceOnly()) {
      Get existsGet = new Get(get);
      if (get.getFilter() == null) {
        existsGet.setFilter(new KeyOnlyFilter());
      } else {
        existsGet.setFilter(new FilterList(get.getFilter(), new KeyOnlyFilter()));
      }
      return existsGet;
    } else {
      return get;
    }
  }
}
