package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

public abstract class AbstractTestAppend extends AbstractTest{
  /**
   * Requirement 5.1 - Append values to one or more columns within a single row.
   */
  @Test
  public void testAppend() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value1-");
    byte[] value1And2 = ArrayUtils.addAll(value1, value2);

    // Put then append
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier, value1);
    table.put(put);
    Append append = new Append(rowKey);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qualifier, value2);
    Result result = table.append(append);
    Cell cell = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cell));

    // Test result
    Get get = new Get(rowKey);
    getGetAddColumnVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    result = table.get(get);
    List<Cell> cells = result.getColumnCells(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect concatenated byte array", value1And2,
      CellUtil.cloneValue(cells.get(0)));
    if (result.size() == 2) {
      // TODO: This isn't always true with CBT.  Why is that?
      Assert.assertEquals("There should be two versions now", 2, result.size());
      Assert.assertArrayEquals("Expect original value still there", value1,
        CellUtil.cloneValue(cells.get(1)));
    }
  }

  @Test
  public void testAppendToEmptyCell() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier = dataHelper.randomData("qualifier-");
    byte[] value = dataHelper.randomData("value1-");

    // Put then append
    Append append = new Append(rowKey);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qualifier, value);
    table.append(append);

    // Test result
    Get get = new Get(rowKey);
    getGetAddColumnVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Result result = table.get(get);
    Assert.assertEquals("There should be one version now", 1, result.size());
    Cell cell = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    Assert.assertArrayEquals("Expect append value is entire value", value, CellUtil.cloneValue(cell));
  }

  @Test
  public void testAppendNoResult() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qual = dataHelper.randomData("qualifier-");
    byte[] value1 = dataHelper.randomData("value-");
    byte[] value2 = dataHelper.randomData("value-");

    // Put then append
    Put put = new Put(rowKey).addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value1);
    table.put(put);
    Append append = new Append(rowKey);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qual, value2);
    append.setReturnResults(false);
    Result result = table.append(append);
    if(result != null) {
      Assert.assertTrue("Should be empty", result.isEmpty());
    } else {
      Assert.assertNull("Should not return result", result);      
    }
  }

  @Test
  public void testAppendToMultipleColumns() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier1 = dataHelper.randomData("qualifier1-");
    byte[] qualifier2 = dataHelper.randomData("qualifier2-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value2-");

    // Put then append
    Append append = new Append(rowKey);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qualifier1, value1);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qualifier2, value2);
    table.append(append);

    // Test result
    Get get = new Get(rowKey);
    getGetAddFamilyVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY);
    Result result = table.get(get);
    Assert.assertEquals("There should be two cells", 2, result.size());
    Cell cell1 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier1);
    Assert.assertEquals(
        Bytes.toString(value1),
        Bytes.toString(CellUtil.cloneValue(cell1)));
    Cell cell2 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier2);
    Assert.assertEquals(
        Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(cell2)));
  }


  @Test
  public void testAppendToMultipleFamilies() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier1 = dataHelper.randomData("qualifier1-");
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value2-");

    // Put then append
    Append append = new Append(rowKey);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qualifier1, value1);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY2, qualifier1, value2);
    table.append(append);

    // Test result
    Get get = new Get(rowKey);
    getGetAddFamilyVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY);
    getGetAddFamilyVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY2);
    Result result = table.get(get);
    Assert.assertEquals("There should be two cells", 2, result.size());
    Cell cell1 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier1);
    Assert.assertNotNull(cell1);
    Assert.assertEquals(
        Bytes.toString(value1),
        Bytes.toString(CellUtil.cloneValue(cell1)));
    Cell cell2 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY2, qualifier1);
    Assert.assertNotNull(cell2);
    Assert.assertEquals(
        Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(cell2)));
  }

  @Test
  public void testAppendMultiFamilyEmptyQualifier() throws Exception {
    // Initialize
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("rowKey-");
    byte[] qualifier1 = new byte[0];
    byte[] value1 = dataHelper.randomData("value1-");
    byte[] value2 = dataHelper.randomData("value2-");

    // Put then append
    Append append = new Append(rowKey);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY, qualifier1, value1);
    appendAdd(append, SharedTestEnvRule.COLUMN_FAMILY2, qualifier1, value2);
    table.append(append);

    // Test result
    Get get = new Get(rowKey);
    getGetAddFamilyVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY);
    getGetAddFamilyVersion(get, 5, SharedTestEnvRule.COLUMN_FAMILY2);
    Result result = table.get(get);
    Assert.assertEquals("There should be two cells", 2, result.size());
    Cell cell1 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY, qualifier1);
    Assert.assertEquals(
        Bytes.toString(value1),
        Bytes.toString(CellUtil.cloneValue(cell1)));
    Cell cell2 = result.getColumnLatestCell(SharedTestEnvRule.COLUMN_FAMILY2, qualifier1);
    Assert.assertEquals(
        Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(cell2)));
  }
  public abstract void getGetAddColumnVersion(Get get, int version, byte[] rowKey, byte[] qualifier) throws IOException;
  public abstract void getGetAddFamilyVersion(Get get, int version, byte[] columnFamily) throws IOException;
  public abstract void appendAdd(Append append, byte[] columnFamily, byte[] qualifier, byte[] value);
  
}
