/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.validation;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.HashBasedReader;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.KeyBasedHashTableSource;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class HadoopHashTableSourceTest extends TestCase {

  HadoopHashTableSource source;
  TableHashWrapper mockTableHashWrapper;
  SerializableConfiguration conf;

  public static final String HASH_TABLE_OUTPUT_PATH_DIR = "gs://my-bucket/outputDir";
  private static final ImmutableBytesWritable START_ROW =
      new ImmutableBytesWritable("a".getBytes());
  private static final ImmutableBytesWritable STOP_ROW = new ImmutableBytesWritable("z".getBytes());
  private static final ImmutableBytesWritable PARTITION1 =
      new ImmutableBytesWritable("d".getBytes());
  private static final ImmutableBytesWritable PARTITION2 =
      new ImmutableBytesWritable("g".getBytes());
  private static final ImmutableBytesWritable PARTITION3 =
      new ImmutableBytesWritable("k".getBytes());
  private static final ImmutableBytesWritable EMPTY_ROW_KEY =
      new ImmutableBytesWritable(HConstants.EMPTY_BYTE_ARRAY);

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = new SerializableConfiguration();
    source = new HadoopHashTableSource(conf, StaticValueProvider.of(HASH_TABLE_OUTPUT_PATH_DIR));
    mockTableHashWrapper = Mockito.mock(TableHashWrapper.class);
    // Mock the supplier to inject a MockTableHashHelper.
    source.tableHashHelperSupplier =
        () -> {
          return mockTableHashWrapper;
        };
    reset(mockTableHashWrapper);
  }

  @After
  public void tearDown() throws Exception {
    verify(mockTableHashWrapper).getStartRow();
    verify(mockTableHashWrapper).getStopRow();
    verify(mockTableHashWrapper).getPartitions();
    verifyNoMoreInteractions(mockTableHashWrapper);
    validateMockitoUsage();
  }

  @Test
  public void testSplitZeroPartitions() {
    // Row range [a-z) with no splits.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(1);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of());
    when(mockTableHashWrapper.getStartRow()).thenReturn(START_ROW);
    when(mockTableHashWrapper.getStopRow()).thenReturn(STOP_ROW);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, STOP_ROW));
    assertEquals(expected, actual);
  }

  @Test
  public void testSplitOnePartition() {
    // Row range [a-z) with no splits.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(2);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of(PARTITION1));
    when(mockTableHashWrapper.getStartRow()).thenReturn(START_ROW);
    when(mockTableHashWrapper.getStopRow()).thenReturn(STOP_ROW);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, STOP_ROW));
    assertEquals(expected, actual);
  }

  @Test
  public void testMultiplePartitons() {
    // Row range [a-z) with splits on {d,g}. The data files will be for {[a,d), [d,g), [g,z)}.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(3);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of(PARTITION1, PARTITION2));
    when(mockTableHashWrapper.getStartRow()).thenReturn(START_ROW);
    when(mockTableHashWrapper.getStopRow()).thenReturn(STOP_ROW);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, STOP_ROW));
    assertEquals(expected, actual);
  }

  @Test
  public void testSplitEmptyStartRow() {
    // Row range [""-z) with splits on {d,g}. The data files will be for {["",d), [d,g), [g,z)}.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(3);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of(PARTITION1, PARTITION2));
    when(mockTableHashWrapper.getStartRow()).thenReturn(EMPTY_ROW_KEY);
    when(mockTableHashWrapper.getStopRow()).thenReturn(STOP_ROW);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new KeyBasedHashTableSource(
                conf, HASH_TABLE_OUTPUT_PATH_DIR, EMPTY_ROW_KEY, PARTITION1),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, STOP_ROW));
    assertEquals(expected, actual);
  }

  @Test
  public void testSplitEmptyStopRow() {
    // Row range [""-z) with splits on {d,g}. The data files will be for {["",d), [d,g), [g,z)}.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(3);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of(PARTITION1, PARTITION2));
    when(mockTableHashWrapper.getStartRow()).thenReturn(START_ROW);
    when(mockTableHashWrapper.getStopRow()).thenReturn(EMPTY_ROW_KEY);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new KeyBasedHashTableSource(
                conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, EMPTY_ROW_KEY));
    assertEquals(expected, actual);
  }

  @Test
  public void testSplitFullTableScan() {
    // Row range [""-z) with splits on {d,g}. The data files will be for {["",d), [d,g), [g,z)}.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(3);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of(PARTITION1, PARTITION2));
    when(mockTableHashWrapper.getStartRow()).thenReturn(EMPTY_ROW_KEY);
    when(mockTableHashWrapper.getStopRow()).thenReturn(EMPTY_ROW_KEY);

    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);

    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new KeyBasedHashTableSource(
                conf, HASH_TABLE_OUTPUT_PATH_DIR, EMPTY_ROW_KEY, PARTITION1),
            new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new KeyBasedHashTableSource(
                conf, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, EMPTY_ROW_KEY));
    assertEquals(expected, actual);
  }

  public void testCreateKeyBasedSourceReader() throws IOException {
    // Setup the settings to get a source.
    when(mockTableHashWrapper.getNumHashFiles()).thenReturn(1);
    when(mockTableHashWrapper.getPartitions()).thenReturn(ImmutableList.of());
    when(mockTableHashWrapper.getStartRow()).thenReturn(START_ROW);
    when(mockTableHashWrapper.getStopRow()).thenReturn(STOP_ROW);

    // Get the source.
    List<? extends BoundedSource<RangeHash>> actual = source.split(1, null);
    assertEquals(1, actual.size());
    BoundedSource<RangeHash> keyBasedHashTableSource = actual.get(0);
    // Mock the supplier to inject a MockTableHashHelper.
    ((KeyBasedHashTableSource) keyBasedHashTableSource).tableHashHelperSupplier =
        () -> {
          return mockTableHashWrapper;
        };

    // Create a new Reader
    TableHash.Reader mockTableHashReader = Mockito.mock(TableHash.Reader.class);
    assertEquals(KeyBasedHashTableSource.class, keyBasedHashTableSource.getClass());
    when(mockTableHashWrapper.newReader(conf.get(), START_ROW)).thenReturn(mockTableHashReader);
    BoundedReader<RangeHash> reader = keyBasedHashTableSource.createReader(null);

    // Validate that the reader was properly created.
    assertEquals(HashBasedReader.class, reader.getClass());
    assertEquals(keyBasedHashTableSource, reader.getCurrentSource());
    HashBasedReader hashBasedReader = (HashBasedReader) reader;
    assertEquals(START_ROW, hashBasedReader.startRow);
    assertEquals(STOP_ROW, hashBasedReader.stopRow);
    assertEquals(mockTableHashReader, hashBasedReader.reader);

    // Verify that the newReader call was issued with correct params
    verify(mockTableHashWrapper).newReader(eq(conf.get()), eq(START_ROW));
  }
}
