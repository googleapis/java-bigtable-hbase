/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.bigtable.hbase.replication;

import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF1;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF2;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.COL_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.COL_QUALIFIER_2;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.ROW_KEY;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.TABLE_NAME;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.TIMESTAMP;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.VALUE;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.assertEquals;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.getRowKey;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class CloudBigtableReplicationTaskTest {

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private Connection mockConnection;

  @Mock private Table mockTable;

  @Captor private ArgumentCaptor<List<RowMutations>> captor;

  @Before
  public void setUp() throws Exception {
    when(mockConnection.getTable(TABLE_NAME)).thenReturn(mockTable);
  }

  @After
  public void tearDown() throws Exception {
    reset(mockConnection, mockTable);
  }

  @Test
  public void batchCallFailsPartially() throws IOException, InterruptedException {
    doThrow(new RetriesExhaustedWithDetailsException("Placeholder error"))
        .when(mockTable)
        .batch(anyList(), any(Object[].class));
    Cell cell = new KeyValue(ROW_KEY, TIMESTAMP, KeyValue.Type.Delete);
    Map<ByteRange, List<Cell>> cellsToReplicate = new HashMap<>();
    cellsToReplicate.put(new SimpleByteRange(ROW_KEY), Arrays.asList(cell));
    CloudBigtableReplicationTask replicationTaskUnderTest =
        new CloudBigtableReplicationTask(
            TABLE_NAME.getNameAsString(), mockConnection, cellsToReplicate);

    assertFalse(replicationTaskUnderTest.call());

    verify(mockConnection).getTable(eq(TABLE_NAME));
    verify(mockTable).batch(any(List.class), any(Object[].class));
  }

  @Test
  public void batchCallFails() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              Object[] futures = (Object[]) args[1];
              futures[1] = new RetriesExhaustedWithDetailsException("Placeholder error");
              return null; // void method in a block-style lambda, so return null
            })
        .when(mockTable)
        .batch(anyList(), any(Object[].class));
    Cell cell = new KeyValue(getRowKey(0), TIMESTAMP, KeyValue.Type.Delete);
    Cell cell2 = new KeyValue(getRowKey(1), TIMESTAMP, KeyValue.Type.Delete);
    Map<ByteRange, List<Cell>> cellsToReplicate = new HashMap<>();
    cellsToReplicate.put(new SimpleByteRange(getRowKey(0)), Arrays.asList(cell));
    cellsToReplicate.put(new SimpleByteRange(getRowKey(1)), Arrays.asList(cell2));
    CloudBigtableReplicationTask replicationTaskUnderTest =
        new CloudBigtableReplicationTask(
            TABLE_NAME.getNameAsString(), mockConnection, cellsToReplicate);

    assertFalse(replicationTaskUnderTest.call());

    verify(mockConnection).getTable(eq(TABLE_NAME));
    verify(mockTable).batch(any(List.class), any(Object[].class));
  }

  @Test
  public void batchCallSucceeds() throws IOException, InterruptedException {
    Cell cell = new KeyValue(ROW_KEY, TIMESTAMP, KeyValue.Type.Delete);
    Map<ByteRange, List<Cell>> cellsToReplicate = new HashMap<>();
    cellsToReplicate.put(new SimpleByteRange(ROW_KEY), Arrays.asList(cell));

    RowMutations expectedRowMutations = new RowMutations(ROW_KEY);
    Put put = new Put(ROW_KEY);
    put.add(cell);
    expectedRowMutations.add(put);

    CloudBigtableReplicationTask replicationTaskUnderTest =
        new CloudBigtableReplicationTask(
            TABLE_NAME.getNameAsString(), mockConnection, cellsToReplicate);

    assertTrue(replicationTaskUnderTest.call());

    verify(mockConnection).getTable(eq(TABLE_NAME));
    verify(mockTable).batch(captor.capture(), any(Object[].class));
    Assert.assertEquals(1, captor.getValue().size());
    assertEquals(expectedRowMutations, captor.getValue().get(0));
  }

  @Test
  public void testCreateRowMutationOnlyPuts() throws IOException {
    Cell put1 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE);
    Cell put2 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER_2, TIMESTAMP, KeyValue.Type.Put, VALUE);
    Cell put3 = new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE);

    RowMutations expectedRowMutations = new RowMutations(ROW_KEY);
    Put put = new Put(ROW_KEY);
    put.add(put1);
    put.add(put2);
    put.add(put3);
    expectedRowMutations.add(put);

    assertEquals(
        expectedRowMutations,
        CloudBigtableReplicationTask.buildRowMutations(ROW_KEY, Arrays.asList(put1, put2, put3))
            .get(0));
  }

  @Test
  public void testCreateRowMutationsOnlyDeletes() throws IOException {
    Cell delete1 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete);
    Cell delete2 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER_2, TIMESTAMP, KeyValue.Type.Delete);
    Cell delete3 = new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteColumn);

    RowMutations expectedRowMutations = new RowMutations(ROW_KEY);
    Delete delete = new Delete(ROW_KEY);
    delete.addDeleteMarker(delete1);
    delete.addDeleteMarker(delete3);
    delete.addDeleteMarker(delete2);
    expectedRowMutations.add(delete);

    assertEquals(
        expectedRowMutations,
        CloudBigtableReplicationTask.buildRowMutations(
                ROW_KEY, Arrays.asList(delete1, delete3, delete2))
            .get(0));
  }

  @Test
  public void testCreateRowMutationsPutAndDeleteAlternate() throws IOException {
    Cell putCell1 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE);
    Cell putCell2 =
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER_2, TIMESTAMP, KeyValue.Type.Put, VALUE);
    Cell putCell3 = new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE);
    Cell deleteCell1 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete);
    Cell deleteCell2 = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER_2, TIMESTAMP, KeyValue.Type.Delete);
    Cell deleteCell3 =
        new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteColumn);

    // Alternate puts and deletes
    List<Cell> cellsToReplicate =
        Arrays.asList(putCell1, deleteCell1, putCell2, deleteCell2, putCell3, deleteCell3);

    // Created Expected RowMutations, Order doesn't change, each cell becomes a mutation(Put/Delete)
    RowMutations expectedRowMutations = new RowMutations(ROW_KEY);
    Put put = new Put(ROW_KEY);
    put.add(putCell1);
    expectedRowMutations.add(put);
    Delete delete = new Delete(ROW_KEY);
    delete.addDeleteMarker(deleteCell1);
    expectedRowMutations.add(delete);

    put = new Put(ROW_KEY);
    put.add(putCell2);
    expectedRowMutations.add(put);
    delete = new Delete(ROW_KEY);
    delete.addDeleteMarker(deleteCell2);
    expectedRowMutations.add(delete);

    put = new Put(ROW_KEY);
    put.add(putCell3);
    expectedRowMutations.add(put);
    delete = new Delete(ROW_KEY);
    delete.addDeleteMarker(deleteCell3);
    expectedRowMutations.add(delete);

    assertEquals(
        expectedRowMutations,
        CloudBigtableReplicationTask.buildRowMutations(ROW_KEY, cellsToReplicate).get(0));
  }
}
