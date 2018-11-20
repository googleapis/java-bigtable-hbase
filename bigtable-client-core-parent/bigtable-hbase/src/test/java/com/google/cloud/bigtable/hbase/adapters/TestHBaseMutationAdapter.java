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

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;

import java.util.Collections;
import java.util.List;

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
public class TestHBaseMutationAdapter {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Mock
  private MutationAdapter<Delete> deleteAdapter;
  @Mock
  private MutationAdapter<Put> putAdapter;
  @Mock
  private MutationAdapter<Increment> incrementAdapter;
  @Mock
  private MutationAdapter<Append> appendAdapter;

  private HBaseMutationAdapter adapter;
  private DataGenerationHelper dataHelper = new DataGenerationHelper();

  private com.google.cloud.bigtable.data.v2.models.Mutation mutation;

  public static class UnknownMutation extends Mutation {}

  private static final List<com.google.bigtable.v2.Mutation> EMPTY_MUTATIONS =
      Collections.emptyList();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    adapter = new HBaseMutationAdapter(deleteAdapter, putAdapter, incrementAdapter, appendAdapter);
    mutation = com.google.cloud.bigtable.data.v2.models.Mutation.create();
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
    adapter.adaptMutations(put, mutation);

    Mockito.verify(putAdapter, Mockito.times(1))
        .adaptMutations(Mockito.any(Put.class), Mockito.eq(mutation));
  }

  @Test
  public void testDeleteIsAdapted() {
    Delete delete = new Delete(dataHelper.randomData("rk1"));
    adapter.adaptMutations(delete, mutation);

    Mockito.verify(deleteAdapter, Mockito.times(1))
        .adaptMutations(Mockito.any(Delete.class), Mockito.eq(mutation));
  }

  @Test
  public void testAppendIsAdapted() {
    Append append = new Append(dataHelper.randomData("rk1"));
    adapter.adaptMutations(append, mutation);

    Mockito.verify(appendAdapter, Mockito.times(1))
        .adaptMutations(Mockito.any(Append.class), Mockito.eq(mutation));
  }

  @Test
  public void testIncrementIsAdapted() {
    Increment increment = new Increment(dataHelper.randomData("rk1"));
    adapter.adaptMutations(increment, mutation);

    Mockito.verify(incrementAdapter, Mockito.times(1))
        .adaptMutations(Mockito.any(Increment.class), Mockito.eq(mutation));
  }

  @Test
  public void exceptionIsThrownOnUnknownMutation() {
    UnknownMutation unknownMutation = new UnknownMutation();

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot adapt mutation of type");
    expectedException.expectMessage("UnknownMutation");

    adapter.adaptMutations(unknownMutation, mutation);
  }
}
