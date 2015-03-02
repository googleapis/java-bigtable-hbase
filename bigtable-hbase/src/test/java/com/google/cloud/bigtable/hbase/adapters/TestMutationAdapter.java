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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class TestMutationAdapter {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Mock
  private OperationAdapter<Delete, MutateRowRequest.Builder> deleteAdapter;
  @Mock
  private OperationAdapter<Put, MutateRowRequest.Builder> putAdapter;
  @Mock
  private OperationAdapter<Increment, MutateRowRequest.Builder> incrementAdapter;
  @Mock
  private OperationAdapter<Append, MutateRowRequest.Builder> appendAdapter;

  private MutationAdapter adapter;
  private DataGenerationHelper dataHelper = new DataGenerationHelper();

  public static class UnknownMutation extends Mutation {}

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    adapter = new MutationAdapter(deleteAdapter, putAdapter, incrementAdapter, appendAdapter);
  }

  @After
  public void tearDown() {
    Mockito.verifyNoMoreInteractions(
        deleteAdapter,
        putAdapter,
        incrementAdapter,
        appendAdapter);
  }

  @Test
  public void testPutIsAdapted() {
    Put put = new Put(dataHelper.randomData("rk1"));
    Mockito.when(putAdapter.adapt(put))
        .thenReturn(MutateRowRequest.newBuilder());

    MutateRowRequest.Builder result = adapter.adapt(put);

    Mockito.verify(putAdapter, Mockito.times(1)).adapt(Mockito.any(Put.class));
  }

  @Test
  public void testDeleteIsAdapted() {
    Delete delete = new Delete(dataHelper.randomData("rk1"));
    Mockito.when(deleteAdapter.adapt(delete))
        .thenReturn(MutateRowRequest.newBuilder());

    MutateRowRequest.Builder result = adapter.adapt(delete);

    Mockito.verify(deleteAdapter, Mockito.times(1)).adapt(Mockito.any(Delete.class));
  }

  @Test
  public void testAppendIsAdapted() {
    Append append = new Append(dataHelper.randomData("rk1"));
    Mockito.when(appendAdapter.adapt(append))
        .thenReturn(MutateRowRequest.newBuilder());

    MutateRowRequest.Builder result = adapter.adapt(append);

    Mockito.verify(appendAdapter, Mockito.times(1)).adapt(Mockito.any(Append.class));
  }

  @Test
  public void testIncrementIsAdapted() {
    Increment increment = new Increment(dataHelper.randomData("rk1"));
    Mockito.when(incrementAdapter.adapt(increment))
        .thenReturn(MutateRowRequest.newBuilder());
    MutateRowRequest.Builder result = adapter.adapt(increment);

    Mockito.verify(incrementAdapter, Mockito.times(1)).adapt(Mockito.any(Increment.class));
  }

  @Test
  public void exceptionIsThrownOnUnknownMutation() {
    UnknownMutation unknownMutation = new UnknownMutation();

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot adapt mutation of type");
    expectedException.expectMessage("UnknownMutation");

    adapter.adapt(unknownMutation);
  }
}
