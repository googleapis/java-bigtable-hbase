/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBatch extends AbstractTestBatch {
  protected void appendAdd(Append append, byte[] columnFamily, byte[] qualifier, byte[] value) {
    append.addColumn(columnFamily, qualifier, value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchWithMismatchedResultArray() throws Exception {
    Table table = getDefaultTable();
    Exception actualError = null;
    Batch.Callback mockCallBack = Mockito.mock(Batch.Callback.class);
    try {
      // This is accepted behaviour in HBase 2 API, It ignores the `new Object[1]` param.
      table.batchCallback(ImmutableList.of(), new Object[1], mockCallBack);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNull(actualError);
    verify(mockCallBack, Mockito.never()).update(Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testMutateRowWithEmptyElements() throws Exception {
    Table table = getDefaultTable();
    Exception actualError = null;
    try {
      table.mutateRow(null);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNotNull(actualError);
    actualError = null;

    try {
      table.mutateRow(new RowMutations(new byte[0]));
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNotNull(actualError);
    actualError = null;

    try {
      // Table#mutateRow ignores requests without Mutations.
      table.mutateRow(new RowMutations(new byte[1]));
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNull(actualError);

    try {
      byte[] rowKeyForPut = dataHelper.randomData("test-rowKey");
      RowMutations rowMutations = new RowMutations(rowKeyForPut);
      rowMutations.add(new Put(rowKeyForPut));

      table.mutateRow(rowMutations);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNotNull(actualError);
    actualError = null;

    try {
      byte[] rowKeyWithNullQual = dataHelper.randomData("test-rowKey");
      RowMutations rowMutations = new RowMutations(rowKeyWithNullQual);
      rowMutations.add(new Put(rowKeyWithNullQual).addColumn(COLUMN_FAMILY, null, null));

      // Table#mutateRow should add a row with null qualifier.
      table.mutateRow(rowMutations);

      assertTrue(table.exists(new Get(rowKeyWithNullQual)));
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNull(actualError);

    try {
      byte[] rowKeyWithEmptyQual = dataHelper.randomData("test-rowKey");
      RowMutations rowMutations = new RowMutations(rowKeyWithEmptyQual);
      rowMutations.add(new Put(rowKeyWithEmptyQual).addColumn(COLUMN_FAMILY, new byte[0], null));

      // Table#mutateRow should add a row with an empty qualifier.
      table.mutateRow(rowMutations);

      assertTrue(table.exists(new Get(rowKeyWithEmptyQual)));
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNull(actualError);

    try {
      byte[] rowKeyWithNullValue = dataHelper.randomData("test-rowKey");
      RowMutations rowMutations = new RowMutations(rowKeyWithNullValue);
      rowMutations.add(new Put(rowKeyWithNullValue).addColumn(COLUMN_FAMILY, "q".getBytes(), null));

      // Table#mutateRow should add a row with null values.
      table.mutateRow(rowMutations);

      assertTrue(table.exists(new Get(rowKeyWithNullValue)));
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNull(actualError);
  }

  /**
   * Requirement 8.3 - MutateRow performs a combination of Put and Delete operations for a single
   * row.
   */
  @Test
  public void testRowMutations() throws IOException {
    // Initialize data
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[][] quals = dataHelper.randomData("qual-", 3);
    byte[][] values = dataHelper.randomData("value-", 3);

    // Put a couple of values
    Put put0 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[0], values[0]);
    Put put1 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[1], values[1]);
    RowMutations rm = new RowMutations(rowKey);
    rm.add(put0);
    rm.add(put1);
    table.mutateRow(rm);

    // Check
    Result result = table.get(new Get(rowKey));
    Assert.assertEquals("Should have two values", 2, result.size());
    Assert.assertArrayEquals(
        "Value #0 should exist",
        values[0],
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[0])));
    Assert.assertArrayEquals(
        "Value #1 should exist",
        values[1],
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[1])));

    // Now delete the value #1 and insert value #3
    Delete delete = new Delete(rowKey).addColumns(COLUMN_FAMILY, quals[1]);
    Put put2 = new Put(rowKey).addColumn(COLUMN_FAMILY, quals[2], values[2]);
    rm = new RowMutations(rowKey);
    rm.add(delete);
    rm.add(put2);
    table.mutateRow(rm);

    // Check
    result = table.get(new Get(rowKey));
    Assert.assertEquals("Should have two values", 2, result.size());
    Assert.assertArrayEquals(
        "Value #0 should exist",
        values[0],
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[0])));
    Assert.assertArrayEquals(
        "Value #2 should exist",
        values[2],
        CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, quals[2])));

    table.close();
  }
}
