/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase2_x;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestBigtableAsyncBufferedMutator {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final String TABLE_ID = "fake-table";

  @Mock private BigtableApi mockBigtableApi;

  @Mock private BigtableHBaseSettings mockBigtableSettings;

  @Mock private HBaseRequestAdapter mockRequestAdapter;

  @Mock private DataClientWrapper mockDataClient;

  @Mock private BulkMutationWrapper mockBulkMutation;

  private BigtableAsyncBufferedMutator asyncBufferedMutator;

  @Before
  public void setUp() {
    when(mockBigtableApi.getDataClient()).thenReturn(mockDataClient);
    when(mockRequestAdapter.getTableId()).thenReturn(TABLE_ID);
    when(mockDataClient.createBulkMutation(eq(TABLE_ID), ArgumentMatchers.anyLong()))
        .thenReturn(mockBulkMutation);
    asyncBufferedMutator =
        new BigtableAsyncBufferedMutator(mockBigtableApi, mockBigtableSettings, mockRequestAdapter);
  }

  @After
  public void tearDown() {
    asyncBufferedMutator.close();
  }

  @Test
  public void testClose() throws Exception {
    doNothing().when(mockBulkMutation).sendUnsent();
    doNothing().when(mockBulkMutation).flush();
    doNothing().when(mockBulkMutation).close();

    asyncBufferedMutator.close();

    verify(mockBulkMutation).sendUnsent();
    verify(mockBulkMutation).close();
  }

  @Test
  public void testFlush() {
    doNothing().when(mockBulkMutation).sendUnsent();
    asyncBufferedMutator.flush();
    verify(mockBulkMutation).sendUnsent();
  }

  @Test
  public void testMutate() throws ExecutionException, InterruptedException {
    Put put = new Put(Bytes.toBytes("rowKey"));
    RowMutationEntry entry = RowMutationEntry.create("rowKey");

    when(mockRequestAdapter.adaptEntry(put)).thenReturn(entry);
    when(mockBulkMutation.add(entry)).thenReturn(ApiFutures.immediateFuture(null));
    asyncBufferedMutator.mutate(put).get();

    verify(mockRequestAdapter).adaptEntry(put);
    verify(mockBulkMutation).add(entry);
  }

  @Test
  public void testMutateWithList() {
    Put put = new Put(Bytes.toBytes("rowKey"));
    Delete delete = new Delete(Bytes.toBytes("to-be-deleted-row"));
    Append append = new Append(Bytes.toBytes("appended-row"));

    RowMutationEntry rowEntryForPut = RowMutationEntry.create("rowKey");
    RowMutationEntry rowEntryForDelete = RowMutationEntry.create("to-be-deleted-row");
    ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(TABLE_ID, "appended-row");

    when(mockRequestAdapter.adaptEntry(put)).thenReturn(rowEntryForPut);
    when(mockRequestAdapter.adaptEntry(delete)).thenReturn(rowEntryForDelete);
    when(mockRequestAdapter.adapt(append)).thenReturn(readModifyWriteRow);

    when(mockBulkMutation.add(rowEntryForPut)).thenReturn(ApiFutures.immediateFuture(null));
    when(mockBulkMutation.add(rowEntryForDelete)).thenReturn(ApiFutures.immediateFuture(null));
    when(mockDataClient.readModifyWriteRowAsync(readModifyWriteRow))
        .thenReturn(ApiFutures.immediateFuture(Result.EMPTY_RESULT));

    asyncBufferedMutator
        .mutate(Arrays.asList(put, delete, append))
        .forEach(
            future -> {
              try {
                future.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new AssertionError("BigtableAsyncBufferedMutator#mutate test failed");
              }
            });

    verify(mockRequestAdapter).adaptEntry(put);
    verify(mockRequestAdapter).adaptEntry(delete);
    verify(mockRequestAdapter).adapt(append);

    verify(mockBulkMutation).add(rowEntryForPut);
    verify(mockBulkMutation).add(rowEntryForDelete);
    verify(mockDataClient).readModifyWriteRowAsync(readModifyWriteRow);
  }
}
