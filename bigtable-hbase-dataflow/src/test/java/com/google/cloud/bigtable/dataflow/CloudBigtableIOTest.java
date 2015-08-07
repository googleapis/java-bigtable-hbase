package com.google.cloud.bigtable.dataflow;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
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
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Tests for {@link CloudBigtableIO}.
 */
public class CloudBigtableIOTest {

  private static final String PROJECT = "my_project";
  private static final String CLUSTER = "cluster";
  private static final String ZONE = "some-zone-1a";
  private static final String TABLE = "some-zone-1a";

  private static final CloudBigtableTableConfiguration config =
      new CloudBigtableTableConfiguration(PROJECT, ZONE, CLUSTER, TABLE);

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
 
  @Test
  public void testInitialize() throws Exception{
    checkRegistry(Put.class);
    checkRegistry(Delete.class);
    checkRegistry(Mutation.class);

    CloudBigtableIO.CloudBigtableWriteTransform writeTransform =
        (CloudBigtableIO.CloudBigtableWriteTransform) CloudBigtableIO.writeToTable(config);
    writeTransform.validate(null);
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


  private void checkRegistry(Class<? extends Mutation> mutationClass)
      throws CannotProvideCoderException {
    Coder<? extends Mutation> coder = registry.getCoder(TypeDescriptor.of(mutationClass));
    assertNotNull(coder);
    assertEquals(HBaseMutationCoder.class, coder.getClass());
  }
}

