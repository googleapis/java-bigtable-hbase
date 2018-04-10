/*
 * Copyright 2018 Google LLC All Rights Reserved.
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

package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.Logger;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

@RunWith(JUnit4.class)
public class TestAsyncSnapshots  extends AbstractAsyncTest {
	protected Logger logger = new Logger(this.getClass());
	final byte[] QUALIFIER = dataHelper.randomData("TestAsyncSnapshots");

  private final TableName tableName = sharedTestEnv.newTestTableName();
  private final TableName anotherTableName = sharedTestEnv.newTestTableName();
  // The maximum size of a table id or snapshot id is 50. newTestTableName().size() can approach 50.
  // Make sure that the snapshot name and cloned table are within the 50 character limit
  private final String snapshotName = tableName.getNameAsString().substring(40) + "_snp";
  private final String anotherSnapshotName = 
      anotherTableName.getNameAsString().substring(40) + "_snp";
  private final TableName clonedTableName =
      TableName.valueOf(tableName.getNameAsString().substring(40) + "_clone");

  @After
  public void cleanup() {
    if (sharedTestEnv.isBigtable() && !enableTestForBigtable()) {
      return;
    }
    try{
      AsyncAdmin asyncAdmin = getAsyncAdmin();
      delete(asyncAdmin, tableName);
      delete(asyncAdmin, anotherTableName);
      delete(asyncAdmin, clonedTableName);
      asyncAdmin.listSnapshots().thenApply(r->{
        for (SnapshotDescription snapshotDescription : r) {
        	asyncAdmin.deleteSnapshot(snapshotDescription.getName());
        }
        return null;
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  protected boolean enableTestForBigtable() {
    return false;
  }
  
  private void delete(AsyncAdmin asyncAdmin, TableName tableName) throws IOException {
    if(asyncAdmin.tableExists(tableName) != null) {
    	asyncAdmin.disableTable(tableName);
    	asyncAdmin.deleteTable(tableName);
    }
  }
  
  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }
  
  @Test
  public void testDeleteSnapshots() throws InterruptedException, ExecutionException {
  	AsyncAdmin asyncAdmin = getAsyncAdmin();
  	asyncAdmin.deleteSnapshots();
  	
  	//To Test delete snapshot with Pattern
  	asyncAdmin.snapshot(anotherSnapshotName, anotherTableName);
  	final Pattern allSnapshots = Pattern.compile(anotherSnapshotName + ".*");
  	asyncAdmin.deleteSnapshots(allSnapshots);
  }
  
  @Test
  public void testDeleteTableSnapshots() throws InterruptedException, ExecutionException {
  	AsyncAdmin asyncAdmin = getAsyncAdmin();
  	//To Test delete snapshot with Pattern
  	asyncAdmin.snapshot(anotherSnapshotName, anotherTableName);
  	final Pattern allSnapshots = Pattern.compile(anotherSnapshotName + ".*");
  	asyncAdmin.deleteTableSnapshots(allSnapshots);
  }
  
  @Test
  public void testcloneSnapshot() throws InterruptedException, ExecutionException {    
  	AsyncAdmin asyncAdmin = getAsyncAdmin();
  	asyncAdmin.cloneSnapshot(snapshotName, clonedTableName);;
  }  
  
  @Test
  public void testSnapshot() throws IOException {
    if (sharedTestEnv.isBigtable() && !Boolean.getBoolean("perform.snapshot.test")) {
      return;
    }
    
    try {
    	AsyncAdmin asyncAdmin = getAsyncAdmin();    
    	asyncAdmin.createTable(
        new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(SharedTestEnvRule.COLUMN_FAMILY)));

      Map<String, Long> values = createAndPopulateTable();
      checkSnapshotCount(asyncAdmin, 0);
      asyncAdmin.snapshot(snapshotName, tableName);
      checkSnapshotCount(asyncAdmin, 1);
      asyncAdmin.cloneSnapshot(snapshotName, clonedTableName);
      validateClone(values);
      checkSnapshotCount(asyncAdmin, 1);
      asyncAdmin.deleteSnapshot(snapshotName);
      checkSnapshotCount(asyncAdmin, 0);
      asyncAdmin.restoreSnapshot(snapshotName);
      checkSnapshotCount(asyncAdmin, 1);
      
    } catch (Exception e) {
        e.printStackTrace();
      }
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
  
	private void checkSnapshotCount(AsyncAdmin asyncAdmin, int count) {
			asyncAdmin.listSnapshots().thenApply(r->{
			logger.info("Count from CheckSnapshot :: ", count);
			Assert.assertEquals(count,r.size());
    	return null;
		});
	}
  private Map<String, Long> createAndPopulateTable() throws IOException {
	    Map<String, Long> values = new HashMap<>();
	    try (Table table = getConnection().getTable(tableName)) {
	      values.clear();
	      List<Put> puts = new ArrayList<>();
	      for (long i = 0; i < 10; i++) {
	        final UUID rowKey = UUID.randomUUID();
	        byte[] row = Bytes.toBytes(rowKey.toString());
	        values.put(rowKey.toString(), i);
	        puts.add(new Put(row).addColumn(SharedTestEnvRule.COLUMN_FAMILY, QUALIFIER, Bytes.toBytes(i)));
	      }
	      table.put(puts);
	    }
	    return values;
  }
}
