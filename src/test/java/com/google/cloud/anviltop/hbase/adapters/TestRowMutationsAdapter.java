/*
 * Copyright (c) 2014 Google Inc.
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
package com.google.cloud.anviltop.hbase.adapters;


import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestRowMutationsAdapter {

  @Mock
  private OperationAdapter<Mutation, AnviltopData.RowMutation.Builder> mutationAdapter;

  private RowMutationsAdapter adapter;
  private DataGenerationHelper dataHelper = new DataGenerationHelper();
  private QualifierTestHelper qualifierHelper = new QualifierTestHelper();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    adapter = new RowMutationsAdapter(mutationAdapter);
  }

  @Test
  public void testRowKeyIsSet() {
    byte[] rowKey = dataHelper.randomData("rk-1");
    RowMutations mutations = new RowMutations(rowKey);
    AnviltopData.RowMutation.Builder result = adapter.adapt(mutations);
    Assert.assertArrayEquals(rowKey, result.getRowKey().toByteArray());
  }

  @Test
  public void testMultipleMutationsAreAdapted() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk-1");
    RowMutations mutations = new RowMutations(rowKey);

    byte[] family1 = dataHelper.randomData("cf1");
    byte[] qualifier1 = dataHelper.randomData("qualifier1");
    byte[] value1 = dataHelper.randomData("value1");

    byte[] family2 = dataHelper.randomData("cf2");
    byte[] qualifier2 = dataHelper.randomData("qualifier2");
    byte[] value2 = dataHelper.randomData("value2");

    mutations.add(
        new Put(rowKey)
            .add(family1, qualifier1, value1));

    mutations.add(
        new Put(rowKey)
            .add(family2, qualifier2, value2));

    // When mocKAdapter is asked to adapt the above mutations, we'll return these responses:
    AnviltopData.RowMutation.Builder response1 = AnviltopData.RowMutation.newBuilder();
    response1.addModsBuilder()
        .getSetCellBuilder()
            .setColumnName(ByteString.copyFrom(
                qualifierHelper.makeFullQualifier(family1, qualifier1)))
        .getCellBuilder()
            .setValue(ByteString.copyFrom(value1));

    AnviltopData.RowMutation.Builder response2 = AnviltopData.RowMutation.newBuilder();
    response2.addModsBuilder()
        .getSetCellBuilder()
        .setColumnName(ByteString.copyFrom(
            qualifierHelper.makeFullQualifier(family2, qualifier2)))
        .getCellBuilder()
            .setValue(ByteString.copyFrom(value2));

    Mockito.when(mutationAdapter.adapt(Matchers.any(Mutation.class)))
        .thenReturn(response1)
        .thenReturn(response2);

    // Adapt the RowMutations to a RowMutation:
    AnviltopData.RowMutation.Builder result = adapter.adapt(mutations);
    Assert.assertArrayEquals(rowKey, result.getRowKey().toByteArray());

    // Verify mutations.getMutations(0) is in the first position in result.mods.
    Assert.assertArrayEquals(qualifierHelper.makeFullQualifier(family1, qualifier1),
        result.getMods(0).getSetCell().getColumnName().toByteArray());

    //Verify mutations.getMutation(1) is in the second position in result.mods.
    Assert.assertArrayEquals(qualifierHelper.makeFullQualifier(family2, qualifier2),
        result.getMods(1).getSetCell().getColumnName().toByteArray());
  }
}
