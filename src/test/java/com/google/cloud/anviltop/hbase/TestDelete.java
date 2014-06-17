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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestDelete extends AbstractTest {
    /**
     * Requirement 4.1 - Delete all data for a given rowkey.
     */
    @Test
    public void testDeleteRow() throws IOException {
        // Initialize data
        HTableInterface table = connection.getTable(TABLE_NAME);
        byte[] rowKey = dataGenerationHelper.randomData("testrow-");
        byte[] qual = dataGenerationHelper.randomData("qual-");
        byte[] value = dataGenerationHelper.randomData("value-");

        // Insert empty values.  Null and byte[0] are interchangeable for puts (but not gets).
        Put put = new Put(rowKey);
        put.add(COLUMN_FAMILY, qual, 1L, value);
        put.add(COLUMN_FAMILY, qual, 2L, value);
        table.put(put);

        // Check values
        Get get = new Get(rowKey);
        Assert.assertTrue(table.exists(get));

        // Delete row
        Delete delete = new Delete(rowKey);
        table.delete(delete);
        Assert.assertFalse(table.exists(get));

        table.close();
    }
}
