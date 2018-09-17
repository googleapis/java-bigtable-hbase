/*
 * Copyright 2018 Google Inc. All Rights Reserved.

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * <p>AbstractTestSnapshot class.</p>
 * 
 * @author rupeshit
 *
 */
@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public abstract class AbstractTestSnapshot extends AbstractTest {

  protected final byte[] QUALIFIER = dataHelper.randomData("TestSnapshots");
  
  protected TableName tableName = sharedTestEnv.newTestTableName();
  protected final TableName anotherTableName = sharedTestEnv.newTestTableName();
  // The maximum size of a table id or snapshot id is 50. newTestTableName().size() can approach 50.
  // Make sure that the snapshot name and cloned table are within the 50 character limit
  protected final String snapshotName = tableName.getNameAsString().substring(40) + "_snp";
  protected final String anotherSnapshotName = 
      anotherTableName.getNameAsString().substring(40) + "_snp";
  protected final TableName clonedTableName =
      TableName.valueOf(tableName.getNameAsString().substring(40) + "_clone");
  protected HTableDescriptor descriptor;
  
  @After
  public void cleanup() {
    if (sharedTestEnv.isBigtable() && !enableTestForBigtable()) {
      return;
    }
    try (Admin admin = getConnection().getAdmin()) {
      delete(admin, tableName);
      delete(admin, anotherTableName);
      delete(admin, clonedTableName);
      for (SnapshotDescription snapDesc : admin.listSnapshots(snapshotName + ".*")) {
        deleteSnapshot(snapDesc.getName());
      }
      for (SnapshotDescription snapDesc : admin.listSnapshots(anotherSnapshotName + ".*")) {
        deleteSnapshot(snapDesc.getName());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected boolean enableTestForBigtable() {
    return false;
  }

  private void delete(Admin admin, TableName tableName) throws IOException {
    if (tableExists(tableName)) {
      disableTable(tableName);
      deleteTable(tableName);
    }
  }

  @Test
  public void testSnapshot() throws IOException {
    if (sharedTestEnv.isBigtable() && !enableTestForBigtable()) {
      return;
    }
    try (Admin admin = getConnection().getAdmin()) {
      createTable(tableName);

      Map<String, Long> values = createAndPopulateTable(tableName);
      checkSnapshotCount(admin, 0);
      snapshot(snapshotName, tableName);
      checkSnapshotCount(admin, 1);
      cloneSnapshot(snapshotName, clonedTableName);
      validateClone(values);
      checkSnapshotCount(admin, 1);
      deleteSnapshot(snapshotName);
      checkSnapshotCount(admin, 0);
    }
  }
  
  @Test
  public void testListSnapshots() throws IOException {
    if (sharedTestEnv.isBigtable() && !enableTestForBigtable()) {
      return;
    }
    final Pattern allSnapshots = Pattern.compile(snapshotName + ".*");
      createTable(tableName);
      Map<String, Long> values = createAndPopulateTable(tableName);
      Assert.assertEquals(0, listSnapshotsSize(allSnapshots));
      snapshot(snapshotName, tableName);
      Assert.assertEquals(1, listSnapshotsSize(snapshotName));
      deleteSnapshot(snapshotName);
      Assert.assertEquals(0, listSnapshotsSize(snapshotName));
      snapshot(snapshotName + 1, tableName);
      snapshot(snapshotName + 2,tableName);
      Assert.assertEquals(2, listSnapshotsSize(allSnapshots));
      Assert.assertEquals(1, listSnapshotsSize(Pattern.compile(snapshotName + 1)));
      Assert.assertEquals(1, listSnapshotsSize(Pattern.compile(snapshotName + 2)));
      deleteSnapshots(allSnapshots);
      Assert.assertEquals(0, listSnapshotsSize(allSnapshots));
    
  }
  
  @Test
  public void testTableSnapshots() throws IOException {
    if (sharedTestEnv.isBigtable() && !enableTestForBigtable()) {
      return;
    }
    final Pattern matchAll =  Pattern.compile(".*");
      createTable(tableName);
      createTable(anotherTableName);
      
      createAndPopulateTable(tableName);
      createAndPopulateTable(anotherTableName);
      Assert.assertEquals(0, 
          listTableSnapshotsSize(Pattern.compile(tableName.getNameAsString()), matchAll));
      Assert.assertEquals(0, listTableSnapshotsSize(
              Pattern.compile(anotherTableName.getNameAsString()), matchAll));
      snapshot(snapshotName, tableName);
      Assert.assertEquals(1, 
          listTableSnapshotsSize(Pattern.compile(tableName.getNameAsString()), matchAll));
      Assert.assertEquals(0, listTableSnapshotsSize(
          Pattern.compile(anotherTableName.getNameAsString()), matchAll));
      snapshot(anotherSnapshotName, anotherTableName);
      Assert.assertEquals(1, 
          listTableSnapshotsSize(Pattern.compile(tableName.getNameAsString()), matchAll));
      Assert.assertEquals(1, listTableSnapshotsSize(
          Pattern.compile(anotherTableName.getNameAsString()), matchAll));
      deleteSnapshot(snapshotName);
      Assert.assertEquals(0, 
          listTableSnapshotsSize(tableName.getNameAsString(), snapshotName));
      Assert.assertEquals(0, 
          listTableSnapshotsSize(anotherTableName.getNameAsString(), snapshotName));
      Assert.assertEquals(1, 
          listTableSnapshotsSize(anotherTableName.getNameAsString(), anotherSnapshotName));
      deleteSnapshot(anotherSnapshotName);
      Assert.assertEquals(0, 
          listTableSnapshotsSize(tableName.getNameAsString(), snapshotName));
      Assert.assertEquals(0, 
          listTableSnapshotsSize(anotherTableName.getNameAsString(), snapshotName));
      Assert.assertEquals(0, 
          listTableSnapshotsSize(anotherTableName.getNameAsString(), anotherSnapshotName));
    
  }
  private void checkSnapshotCount(Admin admin, int count) throws IOException {
    Assert.assertEquals(count,listSnapshotsSize(snapshotName));
  }

  /**
   * Create a test table, and add a small number of rows to the table.
   *
   * @return A Map of the data that was added to the original table.
   * @throws IOException
   */
  protected Map<String, Long> createAndPopulateTable(TableName tableName) throws IOException {
    Map<String, Long> values = new HashMap<>();
    try (Table table = getConnection().getTable(tableName)) {
      values.clear();
      List<Put> puts = new ArrayList<>();
      for (long i = 0; i < 10; i++) {
        final UUID rowKey = UUID.randomUUID();
        byte[] row = Bytes.toBytes(rowKey.toString());
        values.put(rowKey.toString(), i);
        puts.add(new Put(row).addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes(i)));
      }
      table.put(puts);
    }
    return values;
  }

  protected void validateClone(Map<String, Long> values) throws IOException {
    try (Table clonedTable = getConnection().getTable(clonedTableName);
        ResultScanner scanner = clonedTable.getScanner(new Scan())){
      for (Result result : scanner) {
        String row = Bytes.toString(result.getRow());
        Long expected = values.get(row);
        Long found = Bytes.toLong(result.getValue(COLUMN_FAMILY, QUALIFIER));
        Assert.assertEquals("row " + row + " not equal", expected, found);
        values.remove(row);
      }
    }
    Assert.assertTrue("There were missing keys.", values.isEmpty());
  }
  

  protected abstract void createTable(TableName tableName) throws IOException;
  protected abstract void snapshot(String snapshotName, TableName tableName) throws IOException;
  protected abstract int listSnapshotsSize(String regEx) throws IOException;
  protected abstract int listSnapshotsSize(Pattern pattern) throws IOException;
  protected abstract void deleteSnapshot(String snapshotName) throws IOException;
  protected abstract boolean tableExists(final TableName tableName) throws IOException;
  protected abstract void disableTable(final TableName tableName) throws IOException;
  protected abstract void cloneSnapshot(final String snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException;
  protected abstract int listTableSnapshotsSize(String tableNameRegex,
      String snapshotNameRegex) throws IOException;
  protected abstract int listTableSnapshotsSize(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException;
  protected abstract void deleteSnapshots(final Pattern pattern) throws IOException;
  protected abstract void deleteTable(final TableName tableName) throws IOException;
}
