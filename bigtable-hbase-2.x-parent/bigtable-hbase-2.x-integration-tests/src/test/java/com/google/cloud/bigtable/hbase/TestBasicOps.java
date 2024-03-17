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
import static com.google.cloud.bigtable.hbase.test_helpers.ResultSubject.assertThat;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.TruthJUnit.assume;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBasicOps extends AbstractTest {
  /** Happy path for a single value. */
  @Test
  public void testPutGetDelete() throws IOException {
    // Initialize
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");
    byte[] testValue = dataHelper.randomData("testValue-");
    testPutGetDelete(true, rowKey, testQualifier, testValue);
  }

  /** Requirement 1.2 - Rowkey, family, qualifer, and value are byte[] */
  @Test
  public void testBinaryPutGetDelete() throws IOException {
    // Initialize
    Random random = new Random();
    byte[] rowKey = new byte[100];
    random.nextBytes(rowKey);
    byte[] testQualifier = new byte[100];
    random.nextBytes(testQualifier);
    byte[] testValue = new byte[100];
    random.nextBytes(testValue);
    // TODO(carterpage) - test that column-family can work as raw binary

    // Put
    testPutGetDelete(true, rowKey, testQualifier, testValue);
  }

  /**
   * Requirement 1.9 - Referring to a column without the qualifier implicitly sets a special "empty"
   * qualifier.
   */
  @Test
  public void testNullQualifier() throws IOException {
    // Initialize values
    Table table = getDefaultTable();
    byte[] rowKey = dataHelper.randomData("testrow-");
    byte[] testValue = dataHelper.randomData("testValue-");

    // Insert value with null qualifier
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, null, testValue);
    table.put(put);

    // This is treated the same as an empty String (which is just an empty byte array).
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, Bytes.toBytes(""));
    Result result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
    Assert.assertArrayEquals(
        testValue, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

    // Get as a null.  This should work.
    get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, null);
    result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
    Assert.assertArrayEquals(
        testValue, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

    // This should return when selecting the whole family too.
    get = new Get(rowKey);
    get.addFamily(COLUMN_FAMILY);
    result = table.get(get);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, null));
    Assert.assertArrayEquals(
        testValue, CellUtil.cloneValue(result.getColumnLatestCell(COLUMN_FAMILY, null)));

    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, null);
    table.delete(delete);

    // Confirm deleted
    Assert.assertFalse(table.exists(get));
    table.close();
  }

  /**
   * Requirement 2.4 - Maximum cell size is 10MB by default. Can be overriden using
   * hbase.client.keyvalue.maxsize property.
   *
   * <p>Cell size includes value and key info, so the value needs to a bit less than the max to
   * work.
   */
  @Test
  @Category(KnownEmulatorGap.class)
  public void testPutGetBigValue() throws IOException {
    testPutGetDeleteExists((10 << 20) - 1024, false, true); // 10 MB - 1kB
  }

  /**
   * Test a put without a get. This will help allow us to see performance differences between put
   * alone and put/get. There are (or hopefully were, by the time this is read), performance issues
   * with testBigValue. The profile for put (uploading) is different from the profile for get
   * (downloading). We need a way to see where the issue is.
   */
  @Test
  @Category(KnownEmulatorGap.class)
  public void testPutBigValue() throws IOException {
    testPutGetDeleteExists((10 << 20) - 1024, false, false); // 10 MB - 1kB
  }

  /**
   * Requirement 2.4 - Maximum cell size is 10MB by default. Can be overridden using
   * hbase.client.keyvalue.maxsize property.
   *
   * <p>Ensure the failure case.
   */
  @Test(expected = IllegalArgumentException.class)
  @Category(KnownEmulatorGap.class)
  public void testPutTooBigValue() throws IOException {
    testPutGetDeleteExists((10 << 20) + 1 + 4, true, true); // 10 MB + 1
  }

  @Test
  @Category(KnownEmulatorGap.class)
  public void testPutAlmostTooBigValue() throws IOException {
    testPutGetDeleteExists(10 << 20, true, true); // 10 MB
  }

  @Test
  @Category(KnownEmulatorGap.class)
  /** Run a large value ten times for performance logging purposes */
  public void testPutAlmostTooBigValueTenTimes() throws IOException {
    for (int i = 0; i < 10; i++) {
      long start = System.currentTimeMillis();
      testPutGetDeleteExists(10 << 20, true, true); // 10 MB
      if (System.currentTimeMillis() - start > 5_000) {
        // If this is a slow connection, don't bother doing a performance test.
        break;
      }
    }
  }

  void testPutGetDeleteExists(int size, boolean removeMetadataSize, boolean doGet)
      throws IOException {
    // Initialize variables
    byte[] testRowKey = dataHelper.randomData("testrow-");
    byte[] testQualifier = dataHelper.randomData("testQualifier-");

    int valueSize = size;
    if (removeMetadataSize) {
      // looks like in hbase 2.0 Cell size increased by 4. TODO verify it.
      int metadataSize = (20 + 4 + testRowKey.length + COLUMN_FAMILY.length + testQualifier.length);
      valueSize -= metadataSize;
    }

    byte[] testValue = new byte[valueSize];
    new Random().nextBytes(testValue);

    testPutGetDelete(doGet, testRowKey, testQualifier, testValue);
  }

  private void testPutGetDelete(
      boolean doGet, byte[] rowKey, byte[] testQualifier, byte[] testValue) throws IOException {
    Table table = getDefaultTable();

    Stopwatch stopwatch = new Stopwatch();
    // Put
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, testQualifier, testValue);
    table.put(put);
    stopwatch.print("Put took %d ms");

    // Get
    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, testQualifier);

    // Do the get on some tests, but not others.  The rationale for that is to do performance
    // testing on large values.
    if (doGet) {
      Result result = table.get(get);
      stopwatch.print("Get took %d ms");
      Assert.assertTrue(result.containsColumn(COLUMN_FAMILY, testQualifier));
      List<Cell> cells = result.getColumnCells(COLUMN_FAMILY, testQualifier);
      Assert.assertEquals(1, cells.size());
      Assert.assertTrue(Arrays.equals(testValue, CellUtil.cloneValue(cells.get(0))));
      stopwatch.print("Verifying took %d ms");
    }
    // Delete
    Delete delete = new Delete(rowKey);
    delete.addColumns(COLUMN_FAMILY, testQualifier);
    table.delete(delete);
    stopwatch.print("Delete took %d ms");

    // Confirm deleted
    Assert.assertFalse(table.exists(get));
    stopwatch.print("Exists took %d ms");
    table.close();

    stopwatch.print("close took %d ms");
  }

  /**
   * Test the behavior of mutateRow that's common before and after 2.4. At version 2.4, mutateRow
   * started accepting Increment & Append and the return type changed from void to Result. When
   * invoked with non-increment & append it will return an empty result.
   */
  @Test
  public void testMutateRowCommon() throws IOException {
    Table table = getDefaultTable();
    byte[] testRowKey = dataHelper.randomData("testMutateRow23-");

    // pre 2.4, the method's return type was void, so the return value for reflectively invoking
    // the method will return the value null. For newer versions it will be EMPTY_RESULT.
    Result expectedReturn = mutateRowHasResult() ? Result.EMPTY_RESULT : null;

    // Test multiple Puts
    Result result =
        mutateRow(
            table,
            new RowMutations(testRowKey)
                .add(
                    (Mutation)
                        new Put(testRowKey)
                            .addColumn(COLUMN_FAMILY, "q".getBytes(), "v1".getBytes()))
                .add(
                    (Mutation)
                        new Put(testRowKey)
                            .addColumn(COLUMN_FAMILY, "q2".getBytes(), "v2".getBytes())));

    assertThat(result).isEqualTo(expectedReturn);

    Result rowResult = table.get(new Get(testRowKey));
    assertThat(rowResult).latestCellHasValue(COLUMN_FAMILY, "q".getBytes(), "v1".getBytes());
    assertThat(rowResult).latestCellHasValue(COLUMN_FAMILY, "q2".getBytes(), "v2".getBytes());

    // Test Delete
    Result result2 =
        mutateRow(
            table,
            new RowMutations(testRowKey)
                .add((Mutation) new Delete(testRowKey).addColumn(COLUMN_FAMILY, "q".getBytes()))
                .add(
                    (Mutation)
                        new Put(testRowKey)
                            .addColumn(COLUMN_FAMILY, "q2".getBytes(), "v2b".getBytes())));

    assertThat(result2).isEqualTo(expectedReturn);

    Result rowResult2 = table.get(new Get(testRowKey));
    assertThat(rowResult2).doesNotHaveCell(COLUMN_FAMILY, "q".getBytes());
    assertThat(rowResult2).latestCellHasValue(COLUMN_FAMILY, "q2".getBytes(), "v2b".getBytes());
  }

  @Test
  public void testMutateRow24() throws IOException {
    assume()
        .withMessage("Using HBase version that returns a Result for mutateRow")
        .that(mutateRowHasResult())
        .isTrue();

    Table table = getDefaultTable();
    byte[] testRowKey1 = dataHelper.randomData("testMutateRow24-");

    Result result =
        mutateRow(
            table,
            new RowMutations(testRowKey1)
                .add(new Increment(testRowKey1).addColumn(COLUMN_FAMILY, "q".getBytes(), 3))
                .add(
                    new Append(testRowKey1)
                        .addColumn(COLUMN_FAMILY, "q2".getBytes(), "moo".getBytes()))
                .add(
                    (Mutation)
                        new Put(testRowKey1)
                            .addColumn(COLUMN_FAMILY, "q3".getBytes(), "something".getBytes()))
                .add((Mutation) new Delete(testRowKey1).addColumn(COLUMN_FAMILY, "q4".getBytes())));

    assertThat(result).latestCellHasValue(COLUMN_FAMILY, "q".getBytes(), Bytes.toBytes(3L));
    assertThat(result).latestCellHasValue(COLUMN_FAMILY, "q2".getBytes(), "moo".getBytes());
    // Only has info about incremented & appended cells
    assertThat(result.rawCells()).hasLength(2);

    Result rowResult2 = table.get(new Get(testRowKey1));
    assertThat(rowResult2.rawCells()).hasLength(3);
  }

  /** Wrapper to deal with method signature change of mutateRow that happened in hbase 2.4 */
  private static @Nullable Result mutateRow(Table table, RowMutations m) {
    try {
      Method mutateRow = Table.class.getDeclaredMethod("mutateRow", RowMutations.class);
      return (Result) mutateRow.invoke(table, m);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Unexpected error trying to invoke mutateRow", e);
    }
  }

  private static boolean mutateRowHasResult() {
    Method mutateRow = null;
    try {
      mutateRow = Table.class.getDeclaredMethod("mutateRow", RowMutations.class);
      Class<?> returnType = mutateRow.getReturnType();
      if (returnType == Void.TYPE) {
        return false;
      }
      assertThat(returnType).isAssignableTo(Result.class);
      return true;
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private class Stopwatch {
    long lastCheckin = System.currentTimeMillis();

    private void print(String string) {
      long now = System.currentTimeMillis();
      logger.info(string, now - lastCheckin);
      lastCheckin = now;
    }
  }
}
