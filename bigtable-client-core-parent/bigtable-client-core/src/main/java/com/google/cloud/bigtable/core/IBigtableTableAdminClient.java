/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.core;

import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;

/**
 * Interface to access Bigtable gax table admin API.
 */
public interface IBigtableTableAdminClient {

  /**
   * Creates a new table. The table can be created with a full set of initial column families,
   * specified in the request.
   * @param request a {@link CreateTableRequest} object.
   */
  Table createTable(CreateTableRequest request);

  /**
   * Creates a new table asynchronously. The table can be created with a full set of initial column
   * families, specified in the request.
   *
   * @param request a {@link CreateTableRequest} object.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Table> createTableAsync(CreateTableRequest request);

  /**
   * Gets the details of a table.
   *
   * @param tableId a String object.
   * @return a {@link Table} object.
   */
  Table getTable(String tableId);

  /**
   * Gets the details of a table asynchronously.
   *
   * @return a {@link ListenableFuture} that returns a {@link Table} object.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Table> getTableAsync(String tableId);

  /**
   * Lists the names of all tables in an instance.
   *
   * @return an immutable {@link List} object containing tableId.
   */
  List<String> listTables();

  /**
   * Lists the names of all tables in an instance asynchronously.
   *
   * @return a {@link ListenableFuture} of type {@link Void} will be set when request is
   *   successful otherwise exception will be thrown.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<List<String>> listTablesAsync();

  /**
   * Permanently deletes a specified table and all of its data.
   *
   */
  void deleteTable(String tableId);

  /**
   * Permanently deletes a specified table and all of its data.
   *
   * @return a {@link ListenableFuture} of type {@link Void} will be set when request is
   *  successful otherwise exception will be thrown.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Void> deleteTableAsync(String tableId);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return return {@link Table} object  that contains the updated table structure.
   */
  Table modifyFamilies(ModifyColumnFamiliesRequest request);

  /**
   * Creates, modifies or deletes a new column family within a specified table.
   *
   * @param request a {@link ModifyColumnFamiliesRequest} object.
   * @return a {@link ListenableFuture} that returns {@link Table} object that contains
   *  the updated table structure.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param tableId
   * @param rowKeyPrefix
   */
  void dropRowRange(String tableId, String rowKeyPrefix);

  /**
   * Permanently deletes all rows in a range.
   *
   * @param tableId
   * @param rowKeyPrefix
   * @return a {@link ListenableFuture} that returns {@link Void} object.
   */
  //TODO(rahulkql): Once it is adapted to v2.models, change the return type to ApiFuture.
  ListenableFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix);
}
