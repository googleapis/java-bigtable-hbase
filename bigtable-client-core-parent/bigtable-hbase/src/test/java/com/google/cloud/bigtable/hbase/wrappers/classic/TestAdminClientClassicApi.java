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

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestAdminClientClassicApi {

  private static final String PROJECT_ID = "fake-project-id";
  private static final String INSTANCE_ID = "fake-instance-id";
  private static final BigtableInstanceName INSTANCE_NAME =
      new BigtableInstanceName(PROJECT_ID, INSTANCE_ID);

  private static final String TABLE_ID = "fake-Table-id";
  private static final String TABLE_NAME =
      NameUtil.formatTableName(PROJECT_ID, INSTANCE_ID, TABLE_ID);

  private static final String COLUMN_FAMILY = "myColumnFamily";
  private static final String ROW_KEY_PREFIX = "row-key-val";
  private static final String UPDATE_FAMILY = "update-family";

  private BigtableTableAdminClient delegate;

  private AdminClientWrapper adminClientWrapper;

  @Before
  public void setUp() {
    delegate = Mockito.mock(BigtableTableAdminClient.class);
    adminClientWrapper = new AdminClientClassicApi(delegate, INSTANCE_NAME);
  }

  @Test
  public void testCreateTableAsync() throws Exception {
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);

    when(delegate.createTableAsync(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(immediateFuture(createTableData()));

    assertEquals(
        Table.fromProto(createTableData()), adminClientWrapper.createTableAsync(request).get());
    verify(delegate).createTableAsync(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void testGetTableAsync() throws Exception {
    GetTableRequest request = GetTableRequest.newBuilder().setName(TABLE_NAME).build();
    when(delegate.getTableAsync(request)).thenReturn(immediateFuture(createTableData()));

    assertEquals(
        Table.fromProto(createTableData()), adminClientWrapper.getTableAsync(TABLE_ID).get());
    verify(delegate).getTableAsync(request);
  }

  @Test
  public void testListTablesAsync() throws Exception {
    ImmutableList<String> tableIdList =
        ImmutableList.of("test-table-1", "test-table-2", "test-table-3");
    ListTablesRequest request =
        ListTablesRequest.newBuilder().setParent(INSTANCE_NAME.toString()).build();
    ListTablesResponse.Builder builder = ListTablesResponse.newBuilder();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-1")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-2")).build();
    builder.addTablesBuilder().setName(INSTANCE_NAME.toTableNameStr("test-table-3")).build();
    when(delegate.listTablesAsync(request)).thenReturn(immediateFuture(builder.build()));

    assertEquals(tableIdList, adminClientWrapper.listTablesAsync().get());
    verify(delegate).listTablesAsync(request);
  }

  @Test
  public void testDeleteTableAsync() throws ExecutionException, InterruptedException {
    DeleteTableRequest request = DeleteTableRequest.newBuilder().setName(TABLE_NAME).build();
    when(delegate.deleteTableAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    adminClientWrapper.deleteTableAsync(TABLE_ID).get();

    verify(delegate).deleteTableAsync(request);
  }

  @Test
  public void testModifyFamiliesAsync() throws Exception {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID)
            .addFamily(COLUMN_FAMILY, GCRULES.maxVersions(1))
            .updateFamily(UPDATE_FAMILY, GCRULES.maxAge(Duration.ofHours(100)));

    when(delegate.modifyColumnFamilyAsync(request.toProto(PROJECT_ID, INSTANCE_ID)))
        .thenReturn(immediateFuture(createTableData()));
    Future<Table> response = adminClientWrapper.modifyFamiliesAsync(request);

    assertEquals(Table.fromProto(createTableData()), response.get());
    verify(delegate).modifyColumnFamilyAsync(request.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void dropRowRangeAsyncForDeleteByPrefix() throws ExecutionException, InterruptedException {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(false)
            .setRowKeyPrefix(ByteString.copyFromUtf8(ROW_KEY_PREFIX))
            .build();

    when(delegate.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    adminClientWrapper.dropRowRangeAsync(TABLE_ID, ROW_KEY_PREFIX).get();

    verify(delegate).dropRowRangeAsync(request);
  }

  @Test
  public void dropRowRangeAsyncForTruncate() throws ExecutionException, InterruptedException {
    DropRowRangeRequest request =
        DropRowRangeRequest.newBuilder()
            .setName(TABLE_NAME)
            .setDeleteAllDataFromTable(true)
            .build();

    when(delegate.dropRowRangeAsync(request))
        .thenReturn(immediateFuture(Empty.newBuilder().build()));
    adminClientWrapper.dropAllRowsAsync(TABLE_ID).get();

    verify(delegate).dropRowRangeAsync(request);
  }

  private static com.google.bigtable.admin.v2.Table createTableData() {
    GCRules.GCRule gcRule = GCRULES.maxVersions(1);
    ColumnFamily columnFamily = ColumnFamily.newBuilder().setGcRule(gcRule.toProto()).build();

    return com.google.bigtable.admin.v2.Table.newBuilder()
        .setName(TABLE_NAME)
        .putColumnFamilies(COLUMN_FAMILY, columnFamily)
        .build();
  }
}
