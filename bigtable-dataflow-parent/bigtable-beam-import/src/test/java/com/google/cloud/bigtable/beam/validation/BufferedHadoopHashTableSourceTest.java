/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BufferedHadoopHashTableSourceTest {

  private BufferedHadoopHashTableSource bufferedSource;
  private FakeTableHashWrapper fakeTableHashWrapper;

  private static final String HASH_TABLE_OUTPUT_PATH_DIR = "gs://my-bucket/outputDir";
  private static final ImmutableBytesWritable START_ROW =
      new ImmutableBytesWritable("AAAA".getBytes());
  private static final ImmutableBytesWritable STOP_ROW =
      new ImmutableBytesWritable("ZZZZ".getBytes());
  private static final ImmutableBytesWritable POST_STOP_ROW =
      new ImmutableBytesWritable("z".getBytes()); // Lowercase z is lexicographically > uppercase Z
  private static final ImmutableBytesWritable EMPTY_ROW =
      new ImmutableBytesWritable(HConstants.EMPTY_BYTE_ARRAY);
  private static final ImmutableBytesWritable START_HASH =
      new ImmutableBytesWritable("START-HASH".getBytes());
  private static final int BATCH_SIZE = 5;

  @Before
  public void setUp() throws Exception {
    fakeTableHashWrapper =
        new FakeTableHashWrapper(
            START_ROW, STOP_ROW, new ArrayList<>(), new ArrayList<>(), new Scan());
    bufferedSource =
        new BufferedHadoopHashTableSource(
            new HadoopHashTableSource(
                StaticValueProvider.of("cbt-dev"),
                StaticValueProvider.of(HASH_TABLE_OUTPUT_PATH_DIR),
                START_ROW,
                STOP_ROW,
                new FakeTableHashWrapperFactory(fakeTableHashWrapper)),
            BATCH_SIZE);
  }

  protected static ImmutableBytesWritable getKey(int keyIndex) {
    return new ImmutableBytesWritable(("KEY-" + keyIndex).getBytes());
  }

  protected static ImmutableBytesWritable getHash(int hashIndex) {
    return new ImmutableBytesWritable(("HASH-" + hashIndex).getBytes());
  }

  /**
   * Populates the fakeTableHashWrapper with {@code numEntries} entries starting with startKey.
   * Returns a List of expected RangeHashes for this data, for numEntries=1, single RangeHash is
   * returned (startRow, stopRow, START_HASH).
   */
  protected List<KV<String, List<RangeHash>>> setupTestData(
      ImmutableBytesWritable startRow, ImmutableBytesWritable stopRow, int numEntries) {
    fakeTableHashWrapper.startRowInclusive = startRow;
    fakeTableHashWrapper.stopRowExclusive = stopRow;
    fakeTableHashWrapper.hashes.add(KV.of(startRow, START_HASH));
    for (int i = 0; i < numEntries - 1; i++) {
      fakeTableHashWrapper.hashes.add(KV.of(getKey(i), getHash(i)));
    }

    List<KV<String, List<RangeHash>>> out = new ArrayList<>();
    // Setup RangeHashes to be returned
    List<RangeHash> expectedRangeHashes = new ArrayList<>();
    ImmutableBytesWritable key = startRow;
    ImmutableBytesWritable hash = START_HASH;
    for (int i = 0; i < numEntries - 1; i++) {
      expectedRangeHashes.add(RangeHash.of(key, getKey(i), hash));
      key = getKey(i);
      hash = getHash(i);
      if (expectedRangeHashes.size() % BATCH_SIZE == 0) {
        out.add(
            KV.of(
                Bytes.toStringBinary(expectedRangeHashes.get(0).startInclusive.copyBytes()),
                expectedRangeHashes));
        expectedRangeHashes = new ArrayList<>();
      }
    }
    // Process the last range
    expectedRangeHashes.add(RangeHash.of(key, stopRow, hash));
    // Finalize the last batch
    out.add(
        KV.of(
            Bytes.toStringBinary(expectedRangeHashes.get(0).startInclusive.copyBytes()),
            expectedRangeHashes));

    return out;
  }

  @Test
  public void testHashReaderEmpty() throws IOException {
    // The tableHashWrapper has no hashes, this should result in empty source.
    assertEquals(Arrays.asList(), SourceTestUtils.readFromSource(bufferedSource, null));
  }

  @Test
  public void testHashReaderPartialBuffer() throws IOException {
    // Setup 4 entries in this hashtable datafile.
    List<KV<String, List<RangeHash>>> expected = setupTestData(START_ROW, STOP_ROW, 4);
    assertEquals(expected, SourceTestUtils.readFromSource(bufferedSource, null));
  }

  @Test
  public void testHashReaderMultipleBatches() throws IOException {
    // Setup 4 entries in this hashtable datafile.
    List<KV<String, List<RangeHash>>> expected = setupTestData(START_ROW, STOP_ROW, 20);
    assertEquals(expected, SourceTestUtils.readFromSource(bufferedSource, null));
  }

  @Test
  public void testHashReaderMultipleBatchesWithPartialBatchAtEnd() throws IOException {
    // Setup 4 entries in this hashtable datafile.
    List<KV<String, List<RangeHash>>> expected = setupTestData(START_ROW, STOP_ROW, 23);
    assertEquals(expected, SourceTestUtils.readFromSource(bufferedSource, null));
  }

  @Test
  public void testSplitEqualsUnsplit() throws Exception {
    fakeTableHashWrapper.partitions = Arrays.asList(getKey(4), getKey(9));
    SourceTestUtils.assertSourcesEqualReferenceSource(
        bufferedSource, bufferedSource.split(0, null), null);
  }

  @Test
  public void testUnstartedReaderEqualsStarted() throws Exception {
    setupTestData(START_ROW, STOP_ROW, 6);
    SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(
        bufferedSource.createReader(null), null);
  }
}
