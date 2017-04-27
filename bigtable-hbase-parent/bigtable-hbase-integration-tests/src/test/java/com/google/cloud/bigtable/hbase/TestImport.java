/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.Export;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

public class TestImport extends AbstractTest {
  @Test
  @Category(KnownGap.class)
  public void testMapReduce() throws IOException, ClassNotFoundException, InterruptedException {
    Admin admin = getConnection().getAdmin();

    admin.disableTable(sharedTestEnv.getDefaultTableName());
    admin.deleteTable(sharedTestEnv.getDefaultTableName());
    sharedTestEnv.createTable(sharedTestEnv.getDefaultTableName());
    // Put a value.
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] qual = dataHelper.randomData("testQualifier-");
    byte[] value = dataHelper.randomData("testValue-");

    try (Table oldTable = getConnection().getTable(sharedTestEnv.getDefaultTableName())){
      Put put = new Put(rowKey);
      put.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qual, value);
      oldTable.put(put);

      // Assert the value is there.
      Get get = new Get(rowKey);
      Result result = oldTable.get(get);
      List<Cell> cells = result.listCells();
      Assert.assertEquals(1, cells.size());
      Assert.assertArrayEquals(CellUtil.cloneValue(cells.get(0)), value);
    }

    // Run the export.
    Configuration conf = getConnection().getConfiguration();

    //conf.set("fs.defaultFS", "file:///");
    String tempDir = "hdfs:///tmp/backup";

    String[] args = new String[]{
        sharedTestEnv.getDefaultTableName().getNameAsString(),
        tempDir
    };
    Job job = Export.createSubmittableJob(conf, args);
    // So it looks for jars in the local FS, not HDFS.
    job.getConfiguration().set("fs.defaultFS", "file:///");
    Assert.assertTrue(job.waitForCompletion(true));

    // Create new table.
    TableName newTableName = sharedTestEnv.newTestTableName();
    try (Table newTable = getConnection().getTable(newTableName)){
      // Change for method in IntegrationTests
      HColumnDescriptor hcd = new HColumnDescriptor(SharedTestEnvRule.COLUMN_FAMILY);
      HTableDescriptor htd = new HTableDescriptor(newTableName);
      htd.addFamily(hcd);
      admin.createTable(htd);

      // Run the import.
      args = new String[]{
          newTableName.getNameAsString(),
          tempDir
      };
      job = Import.createSubmittableJob(conf, args);
      job.getConfiguration().set("fs.defaultFS", "file:///");
      Assert.assertTrue(job.waitForCompletion(true));

      // Assert the value is there.
      Get get = new Get(rowKey);
      Result result = newTable.get(get);
      List<Cell> cells = result.listCells();
      Assert.assertEquals(1, cells.size());
      Assert.assertArrayEquals(CellUtil.cloneValue(cells.get(0)), value);
    } finally {
      admin.disableTable(newTableName);
      admin.deleteTable(newTableName);
    }
  }
}
