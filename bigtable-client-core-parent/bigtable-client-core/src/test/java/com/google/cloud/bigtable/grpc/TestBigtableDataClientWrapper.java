/*
 * Copyright 2018 Google LLC.  All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestBigtableDataClientWrapper {

  private static final String PROJECT_ID = "projectId";
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE_ID = "tableId";
  private static final String APP_PROFILE_ID = "appProfileId";
  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(InstanceName.of(PROJECT_ID, INSTANCE_ID), APP_PROFILE_ID);
  private BigtableDataClient bigtableDataClient;
  private BigtableOptions options;
  private BigtableDataClientWrapper bigtableDataClientWrapper;

  @Mock
  private ResultScanner<FlatRow> mockFlatScanner;

  @Before
  public void setUp() {
    bigtableDataClient = Mockito.mock(BigtableDataClient.class);
    options = BigtableOptions.builder().setProjectId(PROJECT_ID).setInstanceId(INSTANCE_ID)
        .setAppProfileId(APP_PROFILE_ID).build();
    bigtableDataClientWrapper = new BigtableDataClientWrapper(bigtableDataClient, options);
  }

  @Test
  public void testMutateRow() {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "key");
    MutateRowRequest mutateRowRequest = rowMutation.toProto(REQUEST_CONTEXT);
    when(bigtableDataClient.mutateRow(mutateRowRequest))
        .thenReturn(MutateRowResponse.getDefaultInstance());
    bigtableDataClientWrapper.mutateRow(rowMutation);
    verify(bigtableDataClient).mutateRow(mutateRowRequest);
  }

  @Test
  public void testReadRowsWhenNext() throws IOException {
    FlatRow simpleRow = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("key"))
        .addCell("family", ByteString.copyFromUtf8("column"), 500, ByteString.copyFromUtf8("value"), null)
        .build();
    when(bigtableDataClient.readFlatRows(Mockito.isA(ReadRowsRequest.class))).thenReturn(mockFlatScanner);
    when(mockFlatScanner.next()).thenReturn(simpleRow);
    ResultScanner<Row> expected =
        bigtableDataClientWrapper.readRows(ReadRowsRequest.getDefaultInstance());
    Assert.assertEquals(expected.next(), FlatRowConverter.convertToModelRow(simpleRow));
    verify(bigtableDataClient).readFlatRows(Mockito.isA(ReadRowsRequest.class));
    verify(mockFlatScanner).next();
  }

  @Test
  public void testReadRowsWhenNextWithArgument() throws IOException {
    int count = 3;
    FlatRow.Cell cell = new FlatRow.Cell("family", ByteString.copyFromUtf8("column"), 500,
        ByteString.copyFromUtf8("value"), null);
    FlatRow[] flatRows = new FlatRow[count];
    flatRows[0] = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key1"))
            .addCell(cell).build();
    flatRows[1] = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key2"))
        .addCell(cell).build();
    flatRows[2] = FlatRow.newBuilder().withRowKey(ByteString.copyFromUtf8("key3"))
        .addCell(cell).build();

    when(bigtableDataClient.readFlatRows(Mockito.isA(ReadRowsRequest.class))).thenReturn(mockFlatScanner);
    when(mockFlatScanner.next(count)).thenReturn(flatRows);
    ResultScanner<Row> expected =
        bigtableDataClientWrapper.readRows(ReadRowsRequest.getDefaultInstance());

    Row[] modelRows = expected.next(count);
    Assert.assertEquals(modelRows.length, flatRows.length);
    Assert.assertEquals(modelRows[0].getKey(), flatRows[0].getRowKey());
    Assert.assertEquals(modelRows[1].getKey(), flatRows[1].getRowKey());
    Assert.assertEquals(modelRows[2].getKey(), flatRows[2].getRowKey());
    verify(bigtableDataClient).readFlatRows(Mockito.isA(ReadRowsRequest.class));
    verify(mockFlatScanner).next(count);
  }
}
