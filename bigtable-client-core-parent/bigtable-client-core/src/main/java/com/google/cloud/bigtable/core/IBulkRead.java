/*
 * Copyright 2020 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.grpc.BigtableTableName;

/**
 * Interface to support bulk read of {@link Query} request into a single grpc request.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * <p>See {@link com.google.cloud.bigtable.grpc.BigtableSession#createBulkRead(BigtableTableName)}
 * as a public alternative.
 */
@InternalApi("For internal usage only")
public interface IBulkRead extends AutoCloseable {

  ApiFuture<Row> add(Query request);

  void flush();
}
