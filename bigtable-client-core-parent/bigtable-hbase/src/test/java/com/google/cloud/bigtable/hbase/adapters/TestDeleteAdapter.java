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
package com.google.cloud.bigtable.hbase.adapters;

import java.util.concurrent.TimeUnit;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
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

  private static final String PROJECT_ID = "test-project-id";
  private static final String INSTANCE_ID = "test-instance-id";
  private static final String TABLE_ID = "test-table-id";
  private static final String APP_PROFILE_ID = "test-app-profile-id";
  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.create(PROJECT_ID, INSTANCE_ID, APP_PROFILE_ID);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected DeleteAdapter deleteAdapter = new DeleteAdapter();
  protected DataGenerationHelper randomHelper = new DataGenerationHelper();

  @Test
  public void testFullRowDelete() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    MutateRowRequest request = adapt(delete);

    Assert.assertArrayEquals(rowKey, request.getRowKey().toByteArray());
    Assert.assertEquals(1, request.getMutationsCount());

    Mutation.MutationCase mutationCase = request.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_ROW, mutationCase);
  }

  @Test
  public void testDeleteRowAtTimestampIsUnsupported() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey, 1000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform row deletion at timestamp");

    deleteAdapter.adapt(delete, com.google.cloud.bigtable.data.v2.models.Mutation.create());
  }

  @Test
  public void testColumnFamilyDelete() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    Delete delete = new Delete(rowKey);
    delete.addFamily(family);
    MutateRowRequest request = adapt(delete);

    Assert.assertArrayEquals(rowKey, request.getRowKey().toByteArray());
    Assert.assertEquals(1, request.getMutationsCount());

    MutationCase mutationCase = request.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_FAMILY, mutationCase);

    Mutation.DeleteFromFamily deleteFromFamily =
        request.getMutations(0).getDeleteFromFamily();
    Assert.assertArrayEquals(family, deleteFromFamily.getFamilyNameBytes().toByteArray());
  }

  @Test
  public void testColumnFamilyDeleteAtTimestampFails() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    Delete delete = new Delete(rowKey);
    delete.addFamily(Bytes.toBytes("family1"), 10000L);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot perform column family deletion before timestamp");

    deleteAdapter.adapt(delete, com.google.cloud.bigtable.data.v2.models.Mutation.create());
  }

  @Test
  public void testDeleteColumnAtTimestamp() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long bigtableStartTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp);
    long bigtableEndTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp + 1);

    Delete delete = new Delete(rowKey);
    delete.addColumn(family, qualifier, hbaseTimestamp);
    MutateRowRequest request = adapt(delete);

    Assert.assertArrayEquals(rowKey, request.getRowKey().toByteArray());
    Assert.assertEquals(1, request.getMutationsCount());

    MutationCase mutationCase = request.getMutations(0).getMutationCase();

    Assert.assertEquals(MutationCase.DELETE_FROM_COLUMN, mutationCase);

    Mutation.DeleteFromColumn deleteFromColumn =
        request.getMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(family, deleteFromColumn.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier, deleteFromColumn.getColumnQualifier().toByteArray());
    Assert.assertTrue(request.getMutations(0).getDeleteFromColumn().hasTimeRange());

    TimestampRange timeStampRange = deleteFromColumn.getTimeRange();
    Assert.assertEquals(bigtableStartTimestamp, timeStampRange.getStartTimestampMicros());
    Assert.assertEquals(bigtableEndTimestamp, timeStampRange.getEndTimestampMicros());
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

    deleteAdapter.adapt(delete, com.google.cloud.bigtable.data.v2.models.Mutation.create());
  }

  @Test
  public void testDeleteColumnBeforeTimestamp() {
    byte[] rowKey = randomHelper.randomData("rk1-");
    byte[] family = randomHelper.randomData("family1-");
    byte[] qualifier = randomHelper.randomData("qualifier");
    long hbaseTimestamp = 1000L;
    long bigtableTimestamp = TimeUnit.MILLISECONDS.toMicros(hbaseTimestamp + 1);

    Delete delete = new Delete(rowKey);
    delete.addColumns(family, qualifier, hbaseTimestamp);
    MutateRowRequest request = adapt(delete);

    Assert.assertArrayEquals(rowKey, request.getRowKey().toByteArray());
    Assert.assertEquals(1, request.getMutationsCount());
    Assert.assertEquals(
        MutationCase.DELETE_FROM_COLUMN, request.getMutations(0).getMutationCase());

    Mutation.DeleteFromColumn deleteFromColumn =
        request.getMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(qualifier, deleteFromColumn.getColumnQualifier().toByteArray());
    Assert.assertTrue(request.getMutations(0).getDeleteFromColumn().hasTimeRange());

    TimestampRange timeRange = deleteFromColumn.getTimeRange();
    Assert.assertEquals(0L, timeRange.getStartTimestampMicros());
    Assert.assertEquals(bigtableTimestamp, timeRange.getEndTimestampMicros());
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

    deleteAdapter.adapt(delete, com.google.cloud.bigtable.data.v2.models.Mutation.create());
  }

  private MutateRowRequest adapt(Delete delete) {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, ByteString.copyFrom(delete.getRow()));
    deleteAdapter.adapt(delete, rowMutation);
    return rowMutation.toProto(REQUEST_CONTEXT);
  }
}