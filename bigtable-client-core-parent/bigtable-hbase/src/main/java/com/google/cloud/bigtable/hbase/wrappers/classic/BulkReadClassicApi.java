/*
 * Copyright 2020 Google LLC.
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.hbase.client.Result;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BulkReadClassicApi implements BulkReadWrapper {

  private final BulkRead delegate;
  private boolean isClosed = false;

  BulkReadClassicApi(BulkRead delegate) {
    this.delegate = delegate;
  }

  @Override
  public ApiFuture<Result> add(Query query) {
    Preconditions.checkState(!isClosed, "can't mutate when the bulk mutation is closed.");
    return ApiFutures.transform(
        delegate.add(query),
        new ApiFunction<FlatRow, Result>() {
          @Override
          public Result apply(FlatRow flatRow) {
            return Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  @Override
  public void close() {
    isClosed = true;
  }
}
