/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Tests for {@link CloudBigtableIO}.
 */
public class CloudBigtableIOTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  Pipeline underTest;

  @Mock
  CloudBigtableOptions cbtOptions;

  private CoderRegistry registry = new CoderRegistry();

  @SuppressWarnings("unchecked")
  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
    when(underTest.getOptions()).thenReturn(cbtOptions);
    when(underTest.getCoderRegistry()).thenReturn(registry);
    when(cbtOptions.as(any(Class.class))).thenReturn(cbtOptions);
    CloudBigtableIO.initializeForWrite(underTest);
  }

  private void checkRegistry(Class<? extends Mutation> mutationClass)
      throws CannotProvideCoderException {
    Coder<? extends Mutation> coder = registry.getCoder(TypeDescriptor.of(mutationClass));
    assertNotNull(coder);
    assertEquals(HBaseMutationCoder.class, coder.getClass());
  }

  @Test
  public void testInitialize() throws Exception{
    checkRegistry(Put.class);
    checkRegistry(Delete.class);
    checkRegistry(Mutation.class);
  }

  @Test
  public void testAppendThrowsCannotProvideCoderException() throws Exception {
    expectedException.expect(CannotProvideCoderException.class);
    registry.getCoder(TypeDescriptor.of(Append.class));
  }

  @Test
  public void testIncrementThrowsCannotProvideCoderException() throws Exception {
    expectedException.expect(CannotProvideCoderException.class);
    registry.getCoder(TypeDescriptor.of(Increment.class));
  }

  @Test
  public void testSourceToString() throws Exception {
    CloudBigtableIO.Source source = new CloudBigtableIO.Source(null);
    byte[] startKey = "abc d".getBytes();
    byte[] stopKey = "def g".getBytes();
    BoundedSource<Result> sourceWithKeys = source.new SourceWithKeys(startKey, stopKey, 10);
    assertEquals("Split start: 'abc d', end: 'def g', size: 10", sourceWithKeys.toString());

    startKey = new byte[]{0, 1, 2, 3, 4, 5};
    stopKey = new byte[]{104, 101, 108, 108, 111};  // hello
    sourceWithKeys = source.new SourceWithKeys(startKey, stopKey, 10);
    assertEquals("Split start: '\\x00\\x01\\x02\\x03\\x04\\x05', end: 'hello', size: 10",
        sourceWithKeys.toString());
  }
}

