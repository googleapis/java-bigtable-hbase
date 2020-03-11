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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.admin.v2.internal.NameUtil;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestAdminClientClassicApi {

  private static final String TABLE_ID = "fake-Table-id";

  private static final String TABLE_NAME =
      NameUtil.formatTableName("fake-project-id", "fake-instance-id", TABLE_ID);

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private IBigtableTableAdminClient delegate;

  private AdminClientWrapper adminClientWrapper;

  @Before
  public void setUp() {
    adminClientWrapper = new AdminClientClassicApi(delegate);
  }

  @Test
  public void testCreateTableAsync() throws Exception {
    CreateTableRequest req = CreateTableRequest.of(TABLE_ID);
    when(delegate.createTableAsync(req))
        .thenReturn(
            ApiFutures.immediateFuture(
                Table.fromProto(
                    com.google.bigtable.admin.v2.Table.newBuilder().setName(TABLE_NAME).build())));
    Future<Table> response = adminClientWrapper.createTableAsync(req);

    assertEquals(TABLE_ID, response.get().getId());
    verify(delegate).createTableAsync(req);
  }

  @Test
  public void testGetTableAsync() throws ExecutionException, InterruptedException {
    when(delegate.getTableAsync(TABLE_ID))
        .thenReturn(
            ApiFutures.immediateFuture(
                Table.fromProto(
                    com.google.bigtable.admin.v2.Table.newBuilder().setName(TABLE_NAME).build())));
    Future<Table> response = adminClientWrapper.getTableAsync(TABLE_ID);
    assertEquals(TABLE_ID, response.get().getId());
    verify(delegate).getTableAsync(TABLE_ID);
  }

  @Test
  public void testListTablesAsync() throws ExecutionException, InterruptedException {
    List<String> response = ImmutableList.of("a", "b");
    when(delegate.listTablesAsync()).thenReturn(ApiFutures.immediateFuture(response));
    assertEquals(response, adminClientWrapper.listTablesAsync().get());
    verify(delegate).listTablesAsync();
  }

  @Test
  public void testDeleteTablesAsync() throws ExecutionException, InterruptedException {
    when(delegate.deleteTableAsync(TABLE_ID)).thenReturn(ApiFutures.<Void>immediateFuture(null));
    adminClientWrapper.deleteTableAsync(TABLE_ID).get();
    verify(delegate).deleteTableAsync(TABLE_ID);
  }

  @Test
  public void testModifyFamiliesAsync() throws ExecutionException, InterruptedException {
    ModifyColumnFamiliesRequest request =
        ModifyColumnFamiliesRequest.of(TABLE_ID)
            .addFamily("cf1")
            .updateFamily("cf2", GCRules.GCRULES.maxVersions(5));
    Table table =
        Table.fromProto(
            com.google.bigtable.admin.v2.Table.newBuilder().setName(TABLE_NAME).build());
    when(delegate.modifyFamiliesAsync(request)).thenReturn(ApiFutures.immediateFuture(table));
    assertEquals(table, adminClientWrapper.modifyFamiliesAsync(request).get());
    verify(delegate).modifyFamiliesAsync(request);
  }

  @Test
  public void testDropRowRangeAsync() throws ExecutionException, InterruptedException {
    when(delegate.dropRowRangeAsync(TABLE_ID, "rowkey"))
        .thenReturn(ApiFutures.<Void>immediateFuture(null));
    adminClientWrapper.dropRowRangeAsync(TABLE_ID, "rowkey").get();
    verify(delegate).dropRowRangeAsync(TABLE_ID, "rowkey");
  }

  @Test
  public void testDropAllRowsAsync() throws ExecutionException, InterruptedException {
    when(delegate.dropAllRowsAsync(TABLE_ID)).thenReturn(ApiFutures.<Void>immediateFuture(null));
    adminClientWrapper.dropAllRowsAsync(TABLE_ID).get();
    verify(delegate).dropAllRowsAsync(TABLE_ID);
  }
}
