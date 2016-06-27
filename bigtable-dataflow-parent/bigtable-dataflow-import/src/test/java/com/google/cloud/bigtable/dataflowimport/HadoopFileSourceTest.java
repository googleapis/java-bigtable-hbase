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
package com.google.cloud.bigtable.dataflowimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.dataflowimport.HadoopFileSource.HadoopFileReader;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

/**
 * Unit test for changes made to {@link HadoopFileSource} for HBase Sequence File import.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class HadoopFileSourceTest {
  private static final String FILE_PATTERN = "file_pattern";
  private static final Class<Writable> KEY_CLASS = Writable.class;
  private static final Class<Text> VALUE_CLASS = Text.class;
  private static final String SERIALIZER_PROPERTY = "serializer_prop";
  private static final String SERIALIZER_VALUE = "serializer_value";

  @Mock private HBaseImportOptions importOptions;
  @Mock private Coder<KV<Writable, Text>> overrideCoder;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(importOptions.getFilePattern()).thenReturn(FILE_PATTERN);
  }

  /**
   * Verifies that a {@link HadoopFileSource} created by the simple factory method
   * ({@link HadoopFileSource#from(String, Class, Class, Class)}) is initialized correctly.
   */
  @Test
  public void testSimpleFactoryMethod() throws Exception {
    HadoopFileSource<Writable, Text> source = HadoopFileSource.from(
        FILE_PATTERN, SequenceFileInputFormat.class, KEY_CLASS, VALUE_CLASS);
    verifySourceBasics(source);
    // Verify the output coder.
    assertTrue(source.getDefaultOutputCoder() instanceof KvCoder);
    final KvCoder<Writable, Text> defaultOutputCoder =
        (KvCoder<Writable, Text>) source.getDefaultOutputCoder();
    assertTrue(defaultOutputCoder.getKeyCoder() instanceof WritableCoder);
    assertTrue(defaultOutputCoder.getValueCoder() instanceof WritableCoder);
    // Verify a reader created by the source.
    BoundedReader<KV<Writable, Text>> reader = source.createReader(importOptions);
    assertTrue(reader instanceof HadoopFileReader);
    assertNull(((HadoopFileReader<Writable, Text>) reader).getDeserializerConfiguration()
        .get(SERIALIZER_PROPERTY));
  }

  /**
   * Verifies that a {@link HadoopFileSource} created by the full factory method
   * ({@link HadoopFileSource#from(String, Class, Class, Class, Coder, Map)}) is initialized
   * correctly.
   */
  @Test
  public void testFullFactoryMethod() throws Exception {
    HadoopFileSource<Writable, Text> source = HadoopFileSource.from(
        FILE_PATTERN, SequenceFileInputFormat.class, KEY_CLASS, VALUE_CLASS, overrideCoder,
        ImmutableMap.<String, String>of());
    verifySourceBasics(source);

    assertEquals(source.getDefaultOutputCoder(), overrideCoder);
    // Verify a reader created by the source.
    BoundedReader<KV<Writable, Text>> reader = source.createReader(importOptions);
    assertTrue(reader instanceof HadoopFileReader);
    assertNull(((HadoopFileReader<Writable, Text>) reader).getDeserializerConfiguration()
        .get(SERIALIZER_PROPERTY));
  }

  /**
   * Verifies that a {@link HadoopFileSource} created by the full factory method
   * ({@link HadoopFileSource#from(String, Class, Class, Class, Coder, Map)}) is initialized
   * correctly when properties for the deserializer are present.
   */
  @Test
  public void testFullFactoryMethod_withSerializerProperty() throws Exception {
    HadoopFileSource<Writable, Text> source = HadoopFileSource.from(
        FILE_PATTERN, SequenceFileInputFormat.class, KEY_CLASS, VALUE_CLASS, overrideCoder,
        ImmutableMap.of(SERIALIZER_PROPERTY, SERIALIZER_VALUE));
    verifySourceBasics(source);

    assertEquals(source.getDefaultOutputCoder(), overrideCoder);
    // Verify a reader created by the source.
    BoundedReader<KV<Writable, Text>> reader = source.createReader(importOptions);
    assertTrue(reader instanceof HadoopFileReader);
    assertEquals(((HadoopFileReader<Writable, Text>) reader).getDeserializerConfiguration()
        .get(SERIALIZER_PROPERTY),
      SERIALIZER_VALUE);
  }

  private void verifySourceBasics(HadoopFileSource<Writable, Text> source) throws Exception {
    // Verify that constructor parameters are assigned properly.
    assertEquals(source.getFilepattern(), FILE_PATTERN);
    assertEquals(source.getFormatClass(), SequenceFileInputFormat.class);
    assertEquals(source.getKeyClass(), KEY_CLASS);
    assertEquals(source.getValueClass(), VALUE_CLASS);
  }
}
