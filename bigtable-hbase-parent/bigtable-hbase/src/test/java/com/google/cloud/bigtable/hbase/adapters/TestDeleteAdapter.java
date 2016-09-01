/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.MutationCase;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;


@RunWith(JUnit4.class)
public class TestDeleteAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected DeleteAdapter deleteAdapter = new DeleteAdapter();
  protected QualifierTestHelper qualifierTestHelper = new QualifierTestHelper();
  protected DataGenerationHelper randomHelper = new DataGenerationHelper();

  @Test
  public void testFullRowDelete() throws IOException {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    MutateRowRequest.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getMutationsCount());

    Mutation.MutationCase mutationCase = rowMutation.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_ROW, mutationCase);

    testTwoWayAdapt(delete, deleteAdapter);
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
  public void testColumnFamilyDelete() throws IOException {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    Delete delete = new Delete(rowKey);
    delete.addFamily(family);
    MutateRowRequest.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getMutationsCount());

    MutationCase mutationCase = rowMutation.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_FAMILY, mutationCase);

    Mutation.DeleteFromFamily deleteFromFamily =
        rowMutation.getMutations(0).getDeleteFromFamily();
    Assert.assertArrayEquals(family, deleteFromFamily.getFamilyNameBytes().toByteArray());

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testColumnFamilyDeleteAtTimestampFails() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    delete.addFamily(Bytes.toBytes("family1"), 10000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion before timestamp");

    deleteAdapter.adapt(delete);
  }

  @Test
  public void testDeleteColumnAtTimestamp() throws IOException {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long bigtableStartTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp);
    long bigtableEndTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp + 1);

    Delete delete = new Delete(rowKey);
    delete.addColumn(family, qualifier, hbaseTimestamp);
    MutateRowRequest.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getMutationsCount());

    MutationCase mutationCase = rowMutation.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_COLUMN, mutationCase);

    Mutation.DeleteFromColumn deleteFromColumn =
        rowMutation.getMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(family, deleteFromColumn.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier, deleteFromColumn.getColumnQualifier().toByteArray());
    Assert.assertTrue(rowMutation.getMutations(0).getDeleteFromColumn().hasTimeRange());

    TimestampRange timeStampRange = deleteFromColumn.getTimeRange();
    Assert.assertEquals(bigtableStartTimestamp, timeStampRange.getStartTimestampMicros());
    Assert.assertEquals(bigtableEndTimestamp, timeStampRange.getEndTimestampMicros());

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testDeleteLatestColumnThrows() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");

    Delete delete = new Delete(rowKey);
    delete.addColumn(family, qualifier);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot delete single latest cell");

    deleteAdapter.adapt(delete);
  }

  @Test
  public void testDeleteColumnBeforeTimestamp() throws IOException {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long bigtableTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp + 1);

    Delete delete = new Delete(rowKey);
    delete.addColumns(family, qualifier, hbaseTimestamp);
    MutateRowRequest.Builder rowMutation = deleteAdapter.adapt(delete);

    Assert.assertArrayEquals(rowKey, rowMutation.getRowKey().toByteArray());
    Assert.assertEquals(1, rowMutation.getMutationsCount());
    Assert.assertEquals(
        MutationCase.DELETE_FROM_COLUMN, rowMutation.getMutations(0).getMutationCase());

    Mutation.DeleteFromColumn deleteFromColumn =
        rowMutation.getMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(qualifier, deleteFromColumn.getColumnQualifier().toByteArray());
    Assert.assertTrue(rowMutation.getMutations(0).getDeleteFromColumn().hasTimeRange());

    TimestampRange timeRange = deleteFromColumn.getTimeRange();
    Assert.assertEquals(0L, timeRange.getStartTimestampMicros());
    Assert.assertEquals(bigtableTimestamp, timeRange.getEndTimestampMicros());

    testTwoWayAdapt(delete, deleteAdapter);
  }

  @Test
  public void testDeleteFamilyVersionIsUnsupported() {
    // Unexpected to see this:
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    long hbaseTimestamp = 1000L;

    Delete delete = new Delete(rowKey);
    delete.addFamilyVersion(family, hbaseTimestamp);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion at timestamp");

    deleteAdapter.adapt(delete);
  }

  /**
   * Convert the {@link Delete} to a {@link Mutation}, back to a {@link Delete}, back to
   * {@link Mutation}. Compare the two mutations for equality. This ensures that the adapt
   * process is idempotent.
   */
  private void testTwoWayAdapt(Delete delete, DeleteAdapter adapter) throws IOException {
    // delete -> mutation
    MutateRowRequest firstAdapt = adapter.adapt(delete).build();
    // mutation -> delete -> mutation;
    MutateRowRequest secondAdapt = adapter.adapt(adapter.adapt(firstAdapt)).build();
    // The round trips
    Assert.assertEquals(firstAdapt, secondAdapt);
  }
}