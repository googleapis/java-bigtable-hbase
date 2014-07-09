/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;


import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.TimeUnit;


@RunWith(JUnit4.class)
public class TestDeleteAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected DeleteAdapter deleteAdapter = new DeleteAdapter();
  protected QualifierTestHelper qualifierTestHelper = new QualifierTestHelper();
  protected DataGenerationHelper randomHelper = new DataGenerationHelper();

  @Test
  public void testFullRowDelete() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    AnviltopData.RowMutation.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getModsCount());

    Assert.assertTrue(rowMutation.getMods(0).getDeleteRow());

    Assert.assertFalse(rowMutation.getMods(0).hasDeleteCellRange());
    Assert.assertFalse(rowMutation.getMods(0).hasDeleteCellsInFamily());
  }

  @Test
  public void testDeleteRowAtTimestampIsUnsupported() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey, 1000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform row deletion at timestamp");

    deleteAdapter.adapt(delete);
  }

  @Test
  public void testColumnFamilyDelete() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    Delete delete = new Delete(rowKey);
    delete.deleteFamily(family);
    AnviltopData.RowMutation.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getModsCount());
    Assert.assertTrue(rowMutation.getMods(0).hasDeleteCellsInFamily());

    AnviltopData.RowMutation.Mod.DeleteCellsInFamily deleteCellsInFamily =
        rowMutation.getMods(0).getDeleteCellsInFamily();
    Assert.assertArrayEquals(family, deleteCellsInFamily.getFamilyNameBytes().toByteArray());

    Assert.assertFalse(rowMutation.getMods(0).getDeleteRow());
    Assert.assertFalse(rowMutation.getMods(0).hasDeleteCellRange());
  }

  @Test
  public void testColumnFamilyDeleteAtTimestampFails() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    delete.deleteFamily(Bytes.toBytes("family1"), 10000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion before timestamp");

    deleteAdapter.adapt(delete);
  }

  @Test
  public void testDeleteColumnAtTimestamp() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long anviltopTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp);
    byte[] fullColumnName = qualifierTestHelper.makeFullQualifier(family, qualifier);

    Delete delete = new Delete(rowKey);
    delete.deleteColumn(family, qualifier, hbaseTimestamp);
    AnviltopData.RowMutation.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getModsCount());
    Assert.assertTrue(rowMutation.getMods(0).hasDeleteCellRange());

    AnviltopData.RowMutation.Mod.DeleteCellRange deleteCellRange =
        rowMutation.getMods(0).getDeleteCellRange();
    Assert.assertArrayEquals(fullColumnName, deleteCellRange.getColumnName().toByteArray());
    Assert.assertTrue(rowMutation.getMods(0).getDeleteCellRange().hasTimeRange());

    AnviltopData.TimeRange timeRange = deleteCellRange.getTimeRange();
    Assert.assertEquals(anviltopTimestamp, timeRange.getStartTimestampMicros());
    Assert.assertEquals(anviltopTimestamp, timeRange.getEndTimestampMicros());

    Assert.assertFalse(rowMutation.getMods(0).getDeleteRow());
    Assert.assertFalse(rowMutation.getMods(0).hasDeleteCellsInFamily());
  }

  @Test
  public void testDeleteLatestColumnThrows() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");

    Delete delete = new Delete(rowKey);
    delete.deleteColumn(family, qualifier);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot delete single latest cell");

    deleteAdapter.adapt(delete);
  }

  @Test
  public void testDeleteColumnBeforeTimestamp() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long anviltopTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp);
    byte[] fullColumnName = qualifierTestHelper.makeFullQualifier(family, qualifier);

    Delete delete = new Delete(rowKey);
    delete.deleteColumns(family, qualifier, hbaseTimestamp);
    AnviltopData.RowMutation.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getModsCount());
    Assert.assertTrue(rowMutation.getMods(0).hasDeleteCellRange());

    AnviltopData.RowMutation.Mod.DeleteCellRange deleteCellRange =
        rowMutation.getMods(0).getDeleteCellRange();
    Assert.assertArrayEquals(fullColumnName, deleteCellRange.getColumnName().toByteArray());
    Assert.assertTrue(rowMutation.getMods(0).getDeleteCellRange().hasTimeRange());

    AnviltopData.TimeRange timeRange = deleteCellRange.getTimeRange();
    Assert.assertEquals(0L, timeRange.getStartTimestampMicros());
    Assert.assertEquals(anviltopTimestamp, timeRange.getEndTimestampMicros());

    Assert.assertFalse(rowMutation.getMods(0).getDeleteRow());
    Assert.assertFalse(rowMutation.getMods(0).hasDeleteCellsInFamily());
  }

  @Test
  public void testDeleteFamilyVersionIsUnsupported() {
    // Unexpected to see this:
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    long hbaseTimestamp = 1000L;

    Delete delete = new Delete(rowKey);
    delete.deleteFamilyVersion(family, hbaseTimestamp);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion at timestamp");

    deleteAdapter.adapt(delete);
  }
}
