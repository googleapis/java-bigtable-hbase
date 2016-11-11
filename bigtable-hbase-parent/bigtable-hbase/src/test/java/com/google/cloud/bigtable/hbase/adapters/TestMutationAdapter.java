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

import static org.mockito.Matchers.*;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;

import java.util.Collections;

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
  private MutationAdapter<Delete> deleteAdapter;
  @Mock
  private MutationAdapter<Put> putAdapter;
  @Mock
  private MutationAdapter<Increment> incrementAdapter;
  @Mock
  private MutationAdapter<Append> appendAdapter;

  private HBaseMutationAdapter adapter;
  private DataGenerationHelper dataHelper = new DataGenerationHelper();

  public static class UnknownMutation extends Mutation {}

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    adapter = new HBaseMutationAdapter(deleteAdapter, putAdapter, incrementAdapter, appendAdapter);
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
    verify(new Put(dataHelper.randomData("rk1")), putAdapter, Put.class);
  }

  @Test
  public void testDeleteIsAdapted() {
    verify(new Delete(dataHelper.randomData("rk1")), deleteAdapter, Delete.class);
  }

  @Test
  public void testAppendIsAdapted() {
    verify(new Append(dataHelper.randomData("rk1")), appendAdapter, Append.class);
  }

  @Test
  public void testIncrementIsAdapted() {
    verify(new Increment(dataHelper.randomData("rk1")), incrementAdapter, Increment.class);
  }

  private <T extends Mutation> void verify(T mutation, final MutationAdapter<T> adapter,
      final Class<T> type) {
    Mockito.when(adapter.toMutationList(any(type)))
        .thenReturn(Collections.<com.google.bigtable.v2.Mutation> emptyList());
    Mockito.when(adapter.getRow(any(type)))
        .thenReturn(new byte[0]);

    adapter.adapt(mutation);

    Mockito.verify(adapter, Mockito.times(1)).adapt(same(mutation));
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
