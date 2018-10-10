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

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.MutationCase;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.BigtableDataGrpcClient;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestPutAdapter {

  private static final String PROJECT_ID = "test-project-id";
  private static final String INSTANCE_ID = "test-instance-id";
  private static final String TABLE_ID = "test-table-id";
  private static final String APP_PROFILE_ID = "test-app-profile-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.create(
      InstanceName.of(PROJECT_ID, INSTANCE_ID),
      APP_PROFILE_ID
  );

  protected final PutAdapter adapter = new PutAdapter(-1);
  protected final DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Test
  public void testSingleCellIsConverted() throws IOException {
    byte[] row = dataHelper.randomData("rk-");
    byte[] family = dataHelper.randomData("f");
    byte[] qualifier = dataHelper.randomData("qual");
    byte[] value = dataHelper.randomData("v1");
    long timestamp = 2L;

    Put hbasePut = new Put(row);
    hbasePut.addColumn(family, qualifier, timestamp, value);
    com.google.cloud.bigtable.data.v2.models.Mutation mutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(hbasePut, mutationModel);
    MutateRowRequest rowMutationBuilder = toMutateRowRequest(row, mutationModel);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getMutationsCount());
    Mutation mutation = rowMutationBuilder.getMutations(0);

    Assert.assertEquals(MutationCase.SET_CELL, mutation.getMutationCase());
    SetCell setCell = mutation.getSetCell();

    Assert.assertArrayEquals(family, setCell.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier, setCell.getColumnQualifier().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp),
        setCell.getTimestampMicros());
    Assert.assertArrayEquals(value, setCell.getValue().toByteArray());

    testTwoWay(hbasePut, adapter);
  }

  private void testTwoWay(Put put, PutAdapter adapter) throws IOException {
    com.google.cloud.bigtable.data.v2.models.Mutation firstMutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(put, firstMutationModel);
    MutateRowRequest firstAdapt = toMutateRowRequest(put.getRow(), firstMutationModel);
    // mutation -> put -> mutation;
    com.google.cloud.bigtable.data.v2.models.Mutation secondMutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(adapter.adapt(firstAdapt), secondMutationModel);
    MutateRowRequest secondAdapt = toMutateRowRequest(put.getRow(), secondMutationModel);
    Assert.assertEquals(firstAdapt, secondAdapt);
  }

  @Test
  public void testMultipleCellsInOneFamilyAreConverted() throws IOException {
    byte[] row = dataHelper.randomData("rk-");
    byte[] family = dataHelper.randomData("f1");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] qualifier2 = dataHelper.randomData("qual2");
    byte[] value1 = dataHelper.randomData("v1");
    byte[] value2 = dataHelper.randomData("v2");
    long timestamp1 = 1L;
    long timestamp2 = 2L;

    Put hbasePut = new Put(row);
    hbasePut.addColumn(family, qualifier1, timestamp1, value1);
    hbasePut.addColumn(family, qualifier2, timestamp2, value2);

    com.google.cloud.bigtable.data.v2.models.Mutation mutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(hbasePut, mutationModel);
    MutateRowRequest rowMutationBuilder = toMutateRowRequest(row, mutationModel);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(2, rowMutationBuilder.getMutationsCount());
    Mutation mutation = rowMutationBuilder.getMutations(0);

    Assert.assertEquals(MutationCase.SET_CELL, mutation.getMutationCase());
    SetCell setCell = mutation.getSetCell();
    Assert.assertArrayEquals(family, setCell.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier1, setCell.getColumnQualifier().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp1),
        setCell.getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getValue().toByteArray());

    Mutation mod2 = rowMutationBuilder.getMutations(1);
    SetCell setCell2 = mod2.getSetCell();
    Assert.assertArrayEquals(family, setCell2.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier2, setCell2.getColumnQualifier().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp2),
        setCell2.getTimestampMicros());
    Assert.assertArrayEquals(value2, setCell2.getValue().toByteArray());

    testTwoWay(hbasePut, adapter);
  }

  @Test
  public void testMultipleCellsInMultipleFamiliesAreConverted() throws IOException {
    byte[] row = dataHelper.randomData("rk-");
    byte[] family1 = dataHelper.randomData("f1");
    byte[] family2 = dataHelper.randomData("f2");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] qualifier2 = dataHelper.randomData("qual2");
    byte[] value1 = dataHelper.randomData("v1");
    byte[] value2 = dataHelper.randomData("v1");
    long timestamp1 = 1L;
    long timestamp2 = 2L;

    Put hbasePut = new Put(row);
    hbasePut.addColumn(family1, qualifier1, timestamp1, value1);
    hbasePut.addColumn(family2, qualifier2, timestamp2, value2);

    com.google.cloud.bigtable.data.v2.models.Mutation mutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(hbasePut, mutationModel);
    MutateRowRequest rowMutationBuilder = toMutateRowRequest(row, mutationModel);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(2, rowMutationBuilder.getMutationsCount());
    Mutation mutation1 = rowMutationBuilder.getMutations(0);

    Assert.assertEquals(MutationCase.SET_CELL, mutation1.getMutationCase());
    SetCell setCell = mutation1.getSetCell();
    Assert.assertArrayEquals(family1, setCell.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier1, setCell.getColumnQualifier().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp1),
        setCell.getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getValue().toByteArray());

    Mutation mutation2 = rowMutationBuilder.getMutations(1);
    SetCell setCell2 = mutation2.getSetCell();
    Assert.assertArrayEquals(family2, setCell2.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier2, setCell2.getColumnQualifier().toByteArray());
    Assert.assertEquals(
        TimeUnit.MILLISECONDS.toMicros(timestamp2),
        setCell2.getTimestampMicros());
    Assert.assertArrayEquals(value2, setCell2.getValue().toByteArray());

    testTwoWay(hbasePut, adapter);
  }

  @Test
  public void testUnsetTimestampsArePopulated() throws IOException {
    byte[] row = dataHelper.randomData("rk-");
    byte[] family1 = dataHelper.randomData("f1");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] value1 = dataHelper.randomData("v1");
    long startTimeMillis = System.currentTimeMillis();

    Put hbasePut = new Put(row).addColumn(family1, qualifier1, value1);

    com.google.cloud.bigtable.data.v2.models.Mutation mutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(hbasePut, mutationModel);
    MutateRowRequest rowMutationBuilder = toMutateRowRequest(row, mutationModel);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getMutationsCount());
    Mutation mutation = rowMutationBuilder.getMutations(0);

    Assert.assertEquals(MutationCase.SET_CELL, mutation.getMutationCase());
    SetCell setCell = mutation.getSetCell();

    Assert.assertArrayEquals(family1, setCell.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier1, setCell.getColumnQualifier().toByteArray());
    Assert.assertTrue(startTimeMillis * 1000 <= setCell.getTimestampMicros());
    Assert.assertTrue(setCell.getTimestampMicros() <= System.currentTimeMillis() * 1000);
    Assert.assertArrayEquals(value1, setCell.getValue().toByteArray());

    testTwoWay(hbasePut, adapter);
  }

  @Test
  public void testUnsetTimestampsAreNotPopulated() throws IOException {
    PutAdapter adapter = new PutAdapter(-1, false);

    byte[] row = dataHelper.randomData("rk-");
    byte[] family1 = dataHelper.randomData("f1");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] value1 = dataHelper.randomData("v1");

    Put hbasePut = new Put(row).addColumn(family1, qualifier1, value1);

    com.google.cloud.bigtable.data.v2.models.Mutation mutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(hbasePut, mutationModel);
    MutateRowRequest rowMutationBuilder = toMutateRowRequest(row, mutationModel);
    Assert.assertArrayEquals(row, rowMutationBuilder.getRowKey().toByteArray());

    Assert.assertEquals(1, rowMutationBuilder.getMutationsCount());
    Mutation mutation = rowMutationBuilder.getMutations(0);

    Assert.assertEquals(MutationCase.SET_CELL, mutation.getMutationCase());
    SetCell setCell = mutation.getSetCell();

    Assert.assertArrayEquals(family1, setCell.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qualifier1, setCell.getColumnQualifier().toByteArray());
    Assert.assertEquals(-1, setCell.getTimestampMicros());
    Assert.assertArrayEquals(value1, setCell.getValue().toByteArray());

    testTwoWay(hbasePut, adapter);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyPut() {
    byte[] row = dataHelper.randomData("rk-");
    Put emptyPut = new Put(row);
    adapter.adapt(emptyPut, com.google.cloud.bigtable.data.v2.models.Mutation.create());
  }

  @Test
  public void testRetry() throws IOException{
    byte[] row = dataHelper.randomData("rk-");
    byte[] family1 = dataHelper.randomData("f1");
    byte[] qualifier1 = dataHelper.randomData("qual1");
    byte[] value1 = dataHelper.randomData("v1");

    Put hbasePut = new Put(row, System.currentTimeMillis());
    hbasePut.addColumn(family1, qualifier1, value1);
    com.google.cloud.bigtable.data.v2.models.Mutation mutationModel =
        com.google.cloud.bigtable.data.v2.models.Mutation.create();
    adapter.adapt(hbasePut, mutationModel);
    MutateRowRequest request = toMutateRowRequest(row, mutationModel);

    // Is the Put retryable?
    Assert.assertTrue(BigtableDataGrpcClient.IS_RETRYABLE_MUTATION.apply(request));
    testTwoWay(hbasePut, adapter);
  }

  private MutateRowRequest toMutateRowRequest(byte[] rowKey, com.google.cloud.bigtable.data.v2.models.Mutation mutation) {
    RowMutation rowMutation = toRowMutationModel(rowKey, mutation);
    MutateRowRequest.Builder builder = rowMutation.toProto(REQUEST_CONTEXT).toBuilder();
    return builder.build();
  }

  private RowMutation toRowMutationModel(byte [] rowKey, com.google.cloud.bigtable.data.v2.models.Mutation mutation) {
    return RowMutation.create(TABLE_ID, ByteString.copyFrom(rowKey), mutation);
  }
}