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
package com.google.cloud.bigtable.grpc.async;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.core.IBulkRead;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * This class wraps existing {@link BulkRead} with Google-cloud-java's model.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * <p>See {@link com.google.cloud.bigtable.grpc.BigtableSession#createBulkRead(BigtableTableName)}
 * as a public alternative.
 */
@InternalApi("For internal usage only - please use BulkRead")
public class BulkReadWrapper implements IBulkRead {

  private final BulkRead delegate;

  public BulkReadWrapper(BulkRead delegate) {
    this.delegate = delegate;
  }

  @Override
  public ApiFuture<Row> add(Query request) {
    return ApiFutures.transform(
        delegate.add(request),
        new ApiFunction<FlatRow, Row>() {
          @Override
          public Row apply(FlatRow flatRow) {
            return FlatRowConverter.convertToModelRow(flatRow);
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
    delegate.flush();
  }
}
