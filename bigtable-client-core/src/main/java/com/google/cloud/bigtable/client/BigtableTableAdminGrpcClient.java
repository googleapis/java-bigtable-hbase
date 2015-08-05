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
package com.google.cloud.bigtable.client;

import io.grpc.Channel;

import com.google.bigtable.admin.table.v1.BigtableTableServiceGrpc;
import com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest;
import com.google.bigtable.admin.table.v1.GetTableRequest;
import com.google.bigtable.admin.table.v1.ListTablesRequest;
import com.google.bigtable.admin.table.v1.ListTablesResponse;
import com.google.bigtable.admin.table.v1.RenameTableRequest;
import com.google.bigtable.admin.table.v1.Table;

/**
 * A gRPC client for accessing the Bigtable Table Admin API.
 */
public class BigtableTableAdminGrpcClient implements BigtableTableAdminClient {

  private final BigtableTableServiceGrpc.BigtableTableServiceBlockingStub blockingStub;

  public BigtableTableAdminGrpcClient(Channel channel) {
    blockingStub = BigtableTableServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    return blockingStub.listTables(request);
  }

  @Override
  public Table getTable(GetTableRequest request) {
    return blockingStub.getTable(request);
  }

  @Override
  public void createTable(CreateTableRequest request) {
    blockingStub.createTable(request);
  }

  @Override
  public void createColumnFamily(CreateColumnFamilyRequest request) {
    blockingStub.createColumnFamily(request);
  }

  @Override
  public void deleteTable(DeleteTableRequest request) {
    blockingStub.deleteTable(request);
  }

  @Override
  public void deleteColumnFamily(DeleteColumnFamilyRequest request) {
    blockingStub.deleteColumnFamily(request);
  }

  @Override
  public void renameTable(RenameTableRequest request) {
    blockingStub.renameTable(request);
  }
}
