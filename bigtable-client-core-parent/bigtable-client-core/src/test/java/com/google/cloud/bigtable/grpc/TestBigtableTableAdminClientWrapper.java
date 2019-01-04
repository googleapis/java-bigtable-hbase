/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
  public class TestBigtableTableAdminClientWrapper {
  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final String COLUMN_FAMILY = "myColumnFamily";
  private static final String ROW_KEY_PREFIX = "row-key-val";

  private BigtableOptions options = BigtableOptions.builder()
      .setProjectId(PROJECT_ID)
      .setInstanceId(INSTANCE_ID)
      .setAppProfileId(APP_PROFILE_ID)
      .build();
  private static final BigtableInstanceName INSTANCE_NAME = new BigtableInstanceName(PROJECT_ID,
      INSTANCE_ID);
  private static final String TABLE_NAME = INSTANCE_NAME.toTableNameStr(TABLE_ID);

  private BigtableTableAdminClient mockAdminClient;

  private BigtableTableAdminClientWrapper clientWrapper;


  @Before
  public void setUp(){
    mockAdminClient = Mockito.mock(BigtableTableAdminClient.class);
    clientWrapper = new BigtableTableAdminClientWrapper(mockAdminClient, options);
  }

  @Test
  public void testCreateTable(){
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);
    GetTableRequest getTableRequest = GetTableRequest.newBuilder().setName(TABLE_NAME).build();

    doNothing().when(mockAdminClient).createTable(request.toProto(INSTANCE_NAME.toAdminInstanceName()));
    when(mockAdminClient.getTable(getTableRequest)).thenReturn(createTableData());
    Table response = clientWrapper.createTable(request);

    assertEquals(Table.fromProto(createTableData()), response);
    verify(mockAdminClient).createTable(request.toProto(INSTANCE_NAME.toAdminInstanceName()));
    verify(mockAdminClient).getTable(getTableRequest);
  }

  @Test
  public void testCreateTableAsync() throws Exception{
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);

    when(mockAdminClient.createTableAsync(request.toProto(INSTANCE_NAME.toAdminInstanceName())))
        .thenReturn(immediateFuture(createTableData()));
    ListenableFuture<Table> response = clientWrapper.createTableAsync(request);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(mockAdminClient).createTableAsync(request.toProto(INSTANCE_NAME.toAdminInstanceName()));
  }

  @Test
  public void testGetTable(){
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();

    when(mockAdminClient.getTable(request)).thenReturn(createTableData());
    Table response = clientWrapper.getTable(TABLE_ID);

    assertEquals(Table.fromProto(createTableData()), response);
    verify(mockAdminClient).getTable(request);
  }

  @Test
  public void testGetTableAsync() throws Exception{
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();

    when(mockAdminClient.getTableAsync(request))
        .thenReturn(immediateFuture(createTableData()));
    ListenableFuture<Table> response = clientWrapper.getTableAsync(TABLE_ID);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(mockAdminClient).getTableAsync(request);
  }

  @Test
  public void testListTables(){
    ImmutableList<String> tableIdList = ImmutableList.of("test-table-1", "test-table-2",
        "test-table-3");
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(INSTANCE_NAME.toString()).build();
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-1")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-2")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-3")).build();

    when(mockAdminClient.listTables(request)).thenReturn(builder.build());
    List<String> actualResponse = clientWrapper.listTables();

    assertEquals(tableIdList, actualResponse);
    verify(mockAdminClient).listTables(request);
  }

  @Test
  public void testListTablesAsync() throws Exception{
    ImmutableList<String> tableIdList = ImmutableList.of("test-table-1", "test-table-2",
        "test-table-3");
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(INSTANCE_NAME.toString()).build();
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-1")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-2")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-3")).build();

    when(mockAdminClient.listTablesAsync(request)).thenReturn(immediateFuture(builder.build()));
    ListenableFuture<List<String>> actualResponse = clientWrapper.listTablesAsync();

    assertEquals(tableIdList, actualResponse.get());
    verify(mockAdminClient).listTablesAsync(request);
  }

  @Test
  public void testDeleteTable(){
    DeleteTableRequest request = DeleteTableRequest.newBuilder().setName(TABLE_NAME).build();

    doNothing().when(mockAdminClient).deleteTable(request);
    clientWrapper.deleteTable(TABLE_ID);

    verify(mockAdminClient).deleteTable(request);
  }

  @Test
  public void testDeleteTableAsync(){
    DeleteTableRequest request = DeleteTableRequest.newBuilder().setName(TABLE_NAME).build();

    when(mockAdminClient.deleteTableAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    clientWrapper.deleteTableAsync(TABLE_ID);

    verify(mockAdminClient).deleteTableAsync(request);
  }

  @Test
  public void testModifyFamilies(){
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest
            .of(TABLE_ID)
            .addFamily(COLUMN_FAMILY, GCRULES.maxVersions(1))
            .updateFamily("update-family", GCRULES.defaultRule());

    when(mockAdminClient.modifyColumnFamily(request.toProto(INSTANCE_NAME.toAdminInstanceName())))
        .thenReturn(createTableData());
    Table response = clientWrapper.modifyFamilies(request);

    assertEquals(Table.fromProto(createTableData()), response);
    verify(mockAdminClient).modifyColumnFamily(request.toProto(INSTANCE_NAME.toAdminInstanceName()));
  }

  @Test
  public void testModifyFamiliesAsync() throws Exception{
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest
            .of(TABLE_ID)
            .addFamily(COLUMN_FAMILY, GCRULES.maxVersions(1))
            .updateFamily("update-family", GCRULES.defaultRule());

    when(mockAdminClient.modifyColumnFamilyAsync(request.toProto(INSTANCE_NAME.toAdminInstanceName())))
        .thenReturn(immediateFuture(createTableData()));
    ListenableFuture<Table> response = clientWrapper.modifyFamiliesAsync(request);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(mockAdminClient).modifyColumnFamilyAsync(request.toProto(INSTANCE_NAME.toAdminInstanceName()));
  }

  @Test
  public void dropRowRange(){
    DropRowRangeRequest request = DropRowRangeRequest.newBuilder()
        .setName(TABLE_NAME)
        .setRowKeyPrefix(ByteString.copyFromUtf8(ROW_KEY_PREFIX))
        .build();

    doNothing().when(mockAdminClient).dropRowRange(request);
    clientWrapper.dropRowRange(TABLE_ID, ROW_KEY_PREFIX);

    verify(mockAdminClient).dropRowRange(request);
  }

  @Test
  public void dropRowRangeAsync(){
    DropRowRangeRequest request = DropRowRangeRequest.newBuilder()
        .setName(TABLE_NAME)
        .setRowKeyPrefix(ByteString.copyFromUtf8(ROW_KEY_PREFIX))
        .build();

    when(mockAdminClient.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    clientWrapper.dropRowRangeAsync(TABLE_ID, ROW_KEY_PREFIX);

    verify(mockAdminClient).dropRowRangeAsync(request);
  }

  private static com.google.bigtable.admin.v2.Table createTableData(){
    GCRules.GCRule gcRule = GCRULES.maxVersions(1);
    ColumnFamily columnFamily = ColumnFamily.newBuilder()
        .setGcRule(gcRule.toProto()).build();

    return com.google.bigtable.admin.v2.Table.newBuilder()
        .setName(TABLE_NAME)
        .putColumnFamilies(COLUMN_FAMILY, columnFamily)
        .build();
  }
}
