/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link com.google.cloud.bigtable.hbase.adapters.CellDeduplicationHelper}
 */
@RunWith(JUnit4.class)
public class TestCellDeduplicationHelper {
  @Test
  public void testCellsAreDeduplicated() {
    Append append = new Append(Bytes.toBytes("Ignored"));

    append.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1.1"));
    append.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2.1"));
    append.add(Bytes.toBytes("fam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1.2"));
    // Different family, should not be in the results:
    append.add(Bytes.toBytes("fam2"), Bytes.toBytes("qual1"), Bytes.toBytes("val1.2"));

    List<Cell> deduplicatedCells =
        CellDeduplicationHelper.deduplicateFamily(append, Bytes.toBytes("fam1"));

    // Expect a single value for qual1 and qual2.
    Assert.assertEquals(2, deduplicatedCells.size());

    for (Cell cell : deduplicatedCells) {
      byte[] family = CellUtil.cloneFamily(cell);
      Assert.assertArrayEquals(Bytes.toBytes("fam1"), family);

      if (Arrays.equals(Bytes.toBytes("qual1"), CellUtil.cloneQualifier(cell))) {
        Assert.assertArrayEquals(Bytes.toBytes("val1.2"), CellUtil.cloneValue(cell));
      } else if (Arrays.equals(Bytes.toBytes("qual2"), CellUtil.cloneQualifier(cell))) {
        Assert.assertArrayEquals(Bytes.toBytes("val2.1"), CellUtil.cloneValue(cell));
      } else {
        Assert.fail("Unexpected qualifier encountered: " +
            Bytes.toString(CellUtil.cloneQualifier(cell)));
      }
    }
  }
}
