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

import com.google.cloud.bigtable.dataflow.HBaseResultCoder;
import com.google.cloud.bigtable.dataflowimport.HadoopFileSource.HadoopFileReader;
import com.google.cloud.bigtable.dataflowimport.testing.SequenceFileIoUtils;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for {@link HBaseImportIO}.
 */
@RunWith(JUnit4.class)
public class HBaseImportIOTest {
  private static final byte[] ROW_KEY = Bytes.toBytes("row_key");
  private static final byte[] ROW_KEY_2 = Bytes.toBytes("row_key_2");
  private static final byte[] CF = Bytes.toBytes("column_family");

  private static final String FILE_PATTERN = "file_pattern";

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Mock private HBaseImportOptions importOptions;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreateSource() throws Exception {
    HadoopFileSource<ImmutableBytesWritable, Result> source =
        createSourceAndVerifyBasics(importOptions);
    // Verify that IMPORT_FORMAT_VER is not set for the deserializer.
    Configuration deserializerConfigs = getDeserializerConfigurationFromSource(source);
    assertNull(deserializerConfigs.get(HBaseImportIO.IMPORT_FORMAT_VER));
  }

  @Test
  public void testCreateSource_forHBase094() throws Exception {
    when(importOptions.isHBase094DataFormat()).thenReturn(true);
    HadoopFileSource<ImmutableBytesWritable, Result> source =
        createSourceAndVerifyBasics(importOptions);
    // Verify that IMPORT_FORMAT_VER is set correctly for the deserializer.
    Configuration deserializerConfigs = getDeserializerConfigurationFromSource(source);
    assertEquals(
        deserializerConfigs.get(HBaseImportIO.IMPORT_FORMAT_VER),
        HBaseImportIO.VERSION_094_STRING);
  }

  /**
   * Creates a Sequence File and verifies that a {@code source} created by
   * {@link HBaseImportIO#createSource(HBaseImportOptions)} can read the file correctly.
   */
  @Test
  public void testReadSequenceFile() throws Exception {
    File tmpFile = tmpFolder.newFile(UUID.randomUUID().toString());
    when(importOptions.getFilePattern()).thenReturn(tmpFile.getPath());
    Set<? extends Cell> keyValues = createFileWithData(tmpFile);
    assertEquals(
        keyValues,
        SequenceFileIoUtils.readCellsFromSource(importOptions));
  }

  private Configuration getDeserializerConfigurationFromSource(
      HadoopFileSource<ImmutableBytesWritable, Result> source) throws Exception {
    BoundedReader<KV<ImmutableBytesWritable, Result>> reader = source.createReader(importOptions);
    assertTrue(reader instanceof HadoopFileReader);
    return ((HadoopFileReader) reader).getDeserializerConfiguration();
  }

  private HadoopFileSource<ImmutableBytesWritable, Result> createSourceAndVerifyBasics(
      HBaseImportOptions importOptions) throws Exception {
    when(importOptions.getFilePattern()).thenReturn(FILE_PATTERN);
    BoundedSource<KV<ImmutableBytesWritable, Result>> boundedSource =
        HBaseImportIO.createSource(importOptions);
    assertTrue(boundedSource instanceof HadoopFileSource);

    HadoopFileSource<ImmutableBytesWritable, Result> source =
        (HadoopFileSource<ImmutableBytesWritable, Result>) boundedSource;
    assertEquals(source.getFilepattern(), FILE_PATTERN);
    assertEquals(source.getFormatClass(), SequenceFileInputFormat.class);
    assertEquals(source.getKeyClass(), ImmutableBytesWritable.class);
    assertEquals(source.getValueClass(), Result.class);

    // Verify the output coder.
    assertTrue(source.getDefaultOutputCoder() instanceof KvCoder);
    assertTrue(((KvCoder) source.getDefaultOutputCoder()).getKeyCoder() instanceof WritableCoder);
    assertTrue(
        ((KvCoder) source.getDefaultOutputCoder()).getValueCoder() instanceof HBaseResultCoder);

    // Verify the constant serializer properties
    Configuration deserializerConfigs = getDeserializerConfigurationFromSource(source);
    for (Map.Entry<String, String> e : HBaseImportIO.CONST_FILE_READER_PROPERTIES.entrySet()) {
      assertEquals(deserializerConfigs.get(e.getKey()), e.getValue());
    }
    return source;
  }

  private Set<? extends Cell> createFileWithData(File sequenceFile) throws Exception {
    Set<? extends Cell> keyValues = Sets.newHashSet(
        new KeyValue(ROW_KEY, CF, Bytes.toBytes("col1"), 1L, Bytes.toBytes("v1")),
        new KeyValue(ROW_KEY, CF, Bytes.toBytes("col1"), 2L, Bytes.toBytes("v2")),
        new KeyValue(ROW_KEY_2, CF, Bytes.toBytes("col2"), 1L, Bytes.toBytes("v3")),
        new KeyValue(ROW_KEY_2, CF, Bytes.toBytes("col2"), 3L, Bytes.toBytes("v4")));
    SequenceFileIoUtils.createFileWithData(sequenceFile, keyValues);
    return keyValues;
  }
}
