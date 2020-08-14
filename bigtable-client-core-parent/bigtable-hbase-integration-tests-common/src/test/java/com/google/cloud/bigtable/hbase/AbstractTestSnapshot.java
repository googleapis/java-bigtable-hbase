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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * AbstractTestSnapshot class.
 *
 * @author rupeshit
 */
@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public abstract class AbstractTestSnapshot extends AbstractTest {
  protected final Logger LOG = new Logger(getClass());

  protected static String prefix;

  protected static final int[] BACKOFF_DURATION = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};

  protected static final String TEST_BACKUP_SUFFIX = "backup-it";

  protected final TableName tableName = sharedTestEnv.newTestTableName();
  protected Map<String, Long> values;

  protected final byte[] QUALIFIER = dataHelper.randomData("TestSnapshots");
  protected HTableDescriptor descriptor;

  @After
  public void cleanup() {
    try {
      delete(tableName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void delete(TableName tableName) throws IOException {
    if (tableExists(tableName)) {
      disableTable(tableName);
      deleteTable(tableName);
    }
  }

  @Test
  public void testSnapshot() throws IOException, InterruptedException, ExecutionException {
    String snapshotName = generateId("test-snapshot");
    try {
      snapshot(snapshotName, tableName);
      Assert.assertEquals(1, listSnapshotsSize(snapshotName));
    } finally {
      deleteSnapshot(snapshotName);
      Assert.assertEquals(0, listSnapshotsSize(snapshotName));
    }
  }

  @Test
  public void testListAndDeleteSnapshots()
      throws IOException, InterruptedException, ExecutionException {
    String snapshotName = generateId("list-1");
    String snapshotName2 = generateId("list-2");
    Pattern allSnapshots = Pattern.compile(prefix + ".*" + "list" + ".*");

    try {
      snapshot(snapshotName, tableName);
      Assert.assertEquals(1, listSnapshotsSize(Pattern.compile(snapshotName)));

      snapshot(snapshotName2, tableName);
      Assert.assertEquals(2, listSnapshotsSize(allSnapshots));

      Assert.assertEquals(1, listSnapshotsSize(Pattern.compile(snapshotName2)));
    } finally {
      deleteSnapshot(snapshotName);
      deleteSnapshot(snapshotName2);
      Assert.assertEquals(0, listSnapshotsSize(allSnapshots));
    }
  }

  @Test
  public void testCloneSnapshot() throws IOException, InterruptedException {
    String cloneSnapshotName = generateId("clone-test");
    snapshot(cloneSnapshotName, tableName);

    // Wait 2 minutes so that the RestoreTable API will trigger an optimize restored
    // table operation.
    Thread.sleep(120 * 1000);
    Assert.assertEquals(1, listSnapshotsSize(cloneSnapshotName));

    TableName clonedTableName = TableName.valueOf(tableName.getNameAsString() + "_clne");
    try {
      cloneSnapshot(cloneSnapshotName, clonedTableName);

      // give table time to create
      Thread.sleep(60 * 1000);

      validateClone(clonedTableName, values);
    } finally {
      deleteSnapshot(cloneSnapshotName);
      delete(clonedTableName);
    }
  }

  @Test
  public void testListSnapshotsWithNullAndEmptyString() throws IOException {
    Exception actualError = null;
    try {
      listSnapshotsSize((String) null);
    } catch (Exception ex) {
      actualError = ex;
    }
    Assert.assertNotNull(actualError);
    Assert.assertTrue(actualError instanceof NullPointerException);

    Assert.assertEquals(0, listSnapshotsSize((Pattern) null));

    Assert.assertEquals(0, listSnapshotsSize(""));
  }

  protected void validateClone(TableName tableName, Map<String, Long> values) throws IOException {
    try (Table clonedTable = getConnection().getTable(tableName);
        ResultScanner scanner = clonedTable.getScanner(new Scan())) {
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

  protected abstract void snapshot(String snapshotName, TableName tableName)
      throws IOException, InterruptedException;

  protected abstract Map<String, Long> createAndPopulateTable(TableName tableName)
      throws IOException, ExecutionException, InterruptedException;

  protected abstract int listSnapshotsSize() throws IOException;

  protected abstract int listSnapshotsSize(String regEx) throws IOException;

  protected abstract int listSnapshotsSize(Pattern pattern) throws IOException;

  protected abstract void deleteSnapshot(String snapshotName)
      throws IOException, InterruptedException;

  protected abstract boolean tableExists(final TableName tableName) throws IOException;

  protected abstract void disableTable(final TableName tableName) throws IOException;

  protected abstract void cloneSnapshot(final String snapshotName, final TableName tableName)
      throws IOException;

  protected abstract void deleteTable(final TableName tableName) throws IOException;

  protected static String generateId(String name) {
    return prefix + "-" + name + "-" + TEST_BACKUP_SUFFIX;
  }
}
