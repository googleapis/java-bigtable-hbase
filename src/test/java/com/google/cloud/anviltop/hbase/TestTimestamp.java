/*
 * Copyright (c) 2013 Google Inc.
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
package com.google.cloud.anviltop.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestTimestamp extends AbstractTest {
  /**
   * Requirement 1.6 - Custom, arbitrary timestamps are supported.
   *
   * Version numbers (1,2,3,...) are often used by users.  Just make sure we support this cleanly.
   */
  @Test
  public void testArbitraryTimestamp() throws IOException {
    // Initialize
    HTableInterface table = connection.getTable(TABLE_NAME);
    byte[] rowKey = randomData("testrow-");
    byte[] testQualifier = randomData("testQual-");
    int numVersions = 4;
    assert numVersions > 2;
    byte[][] values = randomData("testValue-", numVersions);
    long[] versions = sequentialTimestamps(numVersions, 1L);

    // Put several versions in the same row/column.
    Put put = new Put(rowKey);
    for (int i = 0; i < numVersions; ++i) {
      put.add(COLUMN_FAMILY, testQualifier, versions[i], values[i]);
    }
    table.put(put);

    // Confirm they are all here, in descending order by version number.
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);
    get.setMaxVersions(numVersions + 1);
    Result result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(numVersions, cells.size());
    for (int i = numVersions - 1, j = 0; j < numVersions; --i, ++j) {
      Assert.assertEquals(versions[i], cells.get(j).getTimestamp());
      Assert.assertArrayEquals(values[i], CellUtil.cloneValue(cells.get(j)));
    }

    // Now limit results to just two versions.
    get.setMaxVersions(2);
    result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(2, cells.size());
    for (int i = numVersions - 1, j = 0; j < 2; --i, ++j) {
      Assert.assertEquals(versions[i], cells.get(j).getTimestamp());
      Assert.assertArrayEquals(values[i], CellUtil.cloneValue(cells.get(j)));
    }

    // Delete the second-to-last version.
    Delete delete = new Delete(rowKey);
    delete.deleteColumn(COLUMN_FAMILY, testQualifier, versions[numVersions - 2]);
    table.delete(delete);

    // Now, the same get should return the last and third-to-last values.
    result = table.get(get);
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
    cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
    Assert.assertEquals(2, cells.size());
    Assert.assertEquals(versions[numVersions - 1], cells.get(0).getTimestamp());
    Assert.assertArrayEquals(values[numVersions - 1], CellUtil.cloneValue(cells.get(0)));
    Assert.assertEquals(versions[numVersions - 3], cells.get(1).getTimestamp());
    Assert.assertArrayEquals(values[numVersions - 3], CellUtil.cloneValue(cells.get(1)));

    // Delete row
    delete = new Delete(rowKey);
    table.delete(delete);

    // Confirm it's gone
    Assert.assertFalse(table.exists(get));
    table.close();
  }
}
