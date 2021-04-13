/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.HashBasedReader;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopHashTableSourceTest extends TestCase {

  HadoopHashTableSource source;
  FakeTableHashWrapper fakeTableHashWrapper;

  private static final ValueProvider<String> PROJECT_ID = StaticValueProvider.of("test-project");
  private static final ValueProvider<String> HASH_TABLE_OUTPUT_PATH_DIR =
      StaticValueProvider.of("gs://my-bucket/outputDir");
  private static final ImmutableBytesWritable START_ROW =
      new ImmutableBytesWritable("a".getBytes());
  private static final ImmutableBytesWritable STOP_ROW = new ImmutableBytesWritable("z".getBytes());
  private static final ImmutableBytesWritable PARTITION1 =
      new ImmutableBytesWritable("d".getBytes());
  private static final ImmutableBytesWritable PARTITION2 =
      new ImmutableBytesWritable("g".getBytes());
  private static final ImmutableBytesWritable EMPTY_ROW_KEY =
      new ImmutableBytesWritable(HConstants.EMPTY_BYTE_ARRAY);

  @Before
  public void setUp() throws Exception {
    super.setUp();
    fakeTableHashWrapper = new FakeTableHashWrapper();
  }

  private List<BoundedSource<RangeHash>> getSplitSources(
      List<ImmutableBytesWritable> partitions,
      ImmutableBytesWritable startRow,
      ImmutableBytesWritable stopRow)
      throws IOException {
    fakeTableHashWrapper.startRowInclusive = startRow;
    fakeTableHashWrapper.stopRowExclusive = stopRow;
    fakeTableHashWrapper.partitions = partitions;

    source =
        new HadoopHashTableSource(
            PROJECT_ID,
            HASH_TABLE_OUTPUT_PATH_DIR,
            startRow,
            stopRow,
            new FakeTableHashWrapperFactory(fakeTableHashWrapper));
    return (List<BoundedSource<RangeHash>>) source.split(0, null);
  }

  private void testSourceSplits(
      List<ImmutableBytesWritable> partitions,
      ImmutableBytesWritable startRow,
      ImmutableBytesWritable stopRow,
      List<BoundedSource<RangeHash>> expectedSources)
      throws IOException {
    assertEquals(expectedSources, getSplitSources(partitions, startRow, stopRow));
  }

  @Test
  public void testSplitZeroPartitions() throws IOException {
    // Row range [a-z) with no splits.
    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new HadoopHashTableSource(PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, STOP_ROW));
    testSourceSplits(ImmutableList.of(), START_ROW, STOP_ROW, expected);
  }

  @Test
  public void testSplitOnePartition() throws IOException {
    // Row range [a-z) with 1 splits.
    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, STOP_ROW));
    testSourceSplits(ImmutableList.of(PARTITION1), START_ROW, STOP_ROW, expected);
  }

  @Test
  public void testMultiplePartitons() throws IOException {
    // Row range [a-z) with splits on {d,g}. The data files will be for {[a,d), [d,g), [g,z)}.
    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, STOP_ROW));
    testSourceSplits(ImmutableList.of(PARTITION1, PARTITION2), START_ROW, STOP_ROW, expected);
  }

  @Test
  public void testSplitEmptyStartRow() throws IOException {
    // Row range [""-z) with splits on {d,g}. The data files will be for {["",d), [d,g), [g,z)}.
    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, EMPTY_ROW_KEY, PARTITION1),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, STOP_ROW));
    testSourceSplits(ImmutableList.of(PARTITION1, PARTITION2), EMPTY_ROW_KEY, STOP_ROW, expected);
  }

  @Test
  public void testSplitEmptyStopRow() throws IOException {
    // Row range [a-"") with splits on {d,g}. The data files will be for {[a,d), [d,g), [g,"")}.
    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, START_ROW, PARTITION1),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, EMPTY_ROW_KEY));
    testSourceSplits(ImmutableList.of(PARTITION1, PARTITION2), START_ROW, EMPTY_ROW_KEY, expected);
  }

  @Test
  public void testSplitFullTableScan() throws IOException {
    // Row range [""-"") with splits on {d,g}. The data files will be for {["",d), [d,g), [g,"")}.
    List<BoundedSource<RangeHash>> expected =
        ImmutableList.of(
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, EMPTY_ROW_KEY, PARTITION1),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION1, PARTITION2),
            new HadoopHashTableSource(
                PROJECT_ID, HASH_TABLE_OUTPUT_PATH_DIR, PARTITION2, EMPTY_ROW_KEY));
    testSourceSplits(
        ImmutableList.of(PARTITION1, PARTITION2), EMPTY_ROW_KEY, EMPTY_ROW_KEY, expected);
  }

  @Test
  public void testCreateReaderWithoutSplit() throws IOException {
    source =
        new HadoopHashTableSource(
            PROJECT_ID,
            HASH_TABLE_OUTPUT_PATH_DIR,
            // When split is not called, start/stop are uninitialized. Start/stop are runtime params
            // and are initialized in split/createReader.
            null,
            null,
            new FakeTableHashWrapperFactory(fakeTableHashWrapper));
    // Setup boundaries on the TableHashWrapper to be used in Source.
    fakeTableHashWrapper.startRowInclusive = START_ROW;
    fakeTableHashWrapper.stopRowExclusive = STOP_ROW;

    // Create a new Reader
    BoundedReader<RangeHash> reader = source.createReader(null);

    // Validate that the reader was properly created.
    assertEquals(HashBasedReader.class, reader.getClass());
    assertEquals(source, reader.getCurrentSource());
    HashBasedReader hashBasedReader = (HashBasedReader) reader;
    assertEquals(START_ROW, hashBasedReader.startRowInclusive);
    assertEquals(STOP_ROW, hashBasedReader.stopRowExclusive);
  }

  @Test
  public void testCreateReaderAfterSplit() throws IOException {
    // Single partitions will return a 2 sources.
    List<BoundedSource<RangeHash>> splitSources =
        getSplitSources(ImmutableList.of(PARTITION1), START_ROW, STOP_ROW);
    BoundedSource<RangeHash> splitHashSource = splitSources.get(0);

    // Create a new Reader
    BoundedReader<RangeHash> reader = splitHashSource.createReader(null);

    // Validate that the reader was properly created.
    assertEquals(HashBasedReader.class, reader.getClass());
    assertEquals(splitHashSource, reader.getCurrentSource());
    HashBasedReader hashBasedReader = (HashBasedReader) reader;
    assertEquals(START_ROW, hashBasedReader.startRowInclusive);
    assertEquals(PARTITION1, hashBasedReader.stopRowExclusive);
  }
}
