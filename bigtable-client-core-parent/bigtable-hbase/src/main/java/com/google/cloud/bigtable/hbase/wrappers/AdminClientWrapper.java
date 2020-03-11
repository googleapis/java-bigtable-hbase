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
package com.google.cloud.bigtable.hbase.wrappers;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import java.util.List;

/**
 * Common API surface for admin operation.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface AdminClientWrapper extends AutoCloseable {

  /**
   * Creates a new table asynchronously. The table can be created with a full set of initial column
   * families, specified in the request.
   */
  ApiFuture<Table> createTableAsync(CreateTableRequest request);

  /** Gets the details of a table asynchronously. */
  ApiFuture<Table> getTableAsync(String tableId);

  /** Lists the names of all tables in an instance asynchronously. */
  ApiFuture<List<String>> listTablesAsync();

  /** Permanently deletes a specified table and all of its data. */
  ApiFuture<Void> deleteTableAsync(String tableId);

  /** Creates, modifies or deletes a new column family within a specified table. */
  ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request);

  /** Permanently deletes all rows in a range. */
  ApiFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix);

  /** Asynchronously drops all data in the table */
  ApiFuture<Void> dropAllRowsAsync(String tableId);
}
