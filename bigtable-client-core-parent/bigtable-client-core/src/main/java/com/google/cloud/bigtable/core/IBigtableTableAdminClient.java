package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import java.util.List;

/**
 * Interface to access Bigtable data service api.
 */
public interface IBigtableTableAdminClient {

  Table createTable(CreateTableRequest request);

  ApiFuture<Table> createTableAsync(CreateTableRequest request);

  Table modifyFamilies(ModifyColumnFamiliesRequest request);

  ApiFuture<Table> modifyFamiliesAsync(ModifyColumnFamiliesRequest request);

  void deleteTable(String tableId);

  ApiFuture<Void> deleteTableAsync(String tableId);

  boolean exists(String tableId);

  ApiFuture<Boolean> existsAsync(String tableId);

  Table getTable(String tableId);

  ApiFuture<Table> getTableAsync(String tableId);

  List<String> listTables();

  ApiFuture<List<String>> listTablesAsync();

  void dropRowRange(String tableId, String rowKeyPrefix);

  ApiFuture<Void> dropRowRangeAsync(String tableId, String rowKeyPrefix);
}
