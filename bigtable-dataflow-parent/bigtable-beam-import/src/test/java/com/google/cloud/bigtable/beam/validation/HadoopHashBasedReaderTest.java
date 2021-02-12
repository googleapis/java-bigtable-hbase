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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopHashBasedReaderTest {

  private HadoopHashTableSource hashTableSource;
  private FakeTableHashWrapper fakeTableHashWrapper;

  private static final String HASH_TABLE_OUTPUT_PATH_DIR = "gs://my-bucket/outputDir";
  private static final ImmutableBytesWritable START_ROW =
      new ImmutableBytesWritable("AAAA".getBytes());
  private static final ImmutableBytesWritable STOP_ROW =
      new ImmutableBytesWritable("ZZZZ".getBytes());
  private static final ImmutableBytesWritable EMPTY_ROW =
      new ImmutableBytesWritable(HConstants.EMPTY_BYTE_ARRAY);
  private static final ImmutableBytesWritable START_HASH =
      new ImmutableBytesWritable("START-HASH".getBytes());

  @Before
  public void setUp() throws Exception {
    fakeTableHashWrapper =
        new FakeTableHashWrapper(
            START_ROW, STOP_ROW, new ArrayList<>(), new ArrayList<>(), new Scan());
    hashTableSource =
        new HadoopHashTableSource(
            StaticValueProvider.of("cbt-dev"),
            StaticValueProvider.of(HASH_TABLE_OUTPUT_PATH_DIR),
            START_ROW,
            STOP_ROW,
            new FakeTableHashWrapperFactory(fakeTableHashWrapper));
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
  protected List<RangeHash> setupTestData(
      ImmutableBytesWritable startRow, ImmutableBytesWritable stopRow, int numEntries) {
    fakeTableHashWrapper.startRowInclusive = startRow;
    fakeTableHashWrapper.stopRowExclusive = stopRow;
    fakeTableHashWrapper.hashes.add(KV.of(startRow, START_HASH));
    for (int i = 0; i < numEntries - 1; i++) {
      fakeTableHashWrapper.hashes.add(KV.of(getKey(i), getHash(i)));
    }

    // Setup RangeHashes to be returned
    List<RangeHash> expectedRangeHashes = new ArrayList<>();
    ImmutableBytesWritable key = startRow;
    ImmutableBytesWritable hash = START_HASH;
    for (int i = 0; i < numEntries - 1; i++) {
      expectedRangeHashes.add(RangeHash.of(key, getKey(i), hash));
      key = getKey(i);
      hash = getHash(i);
    }
    expectedRangeHashes.add(RangeHash.of(key, stopRow, hash));
    return expectedRangeHashes;
  }

  /////////////////////////////// Test the end of HashTable Output /////////////////////////

  @Test
  public void testHashReaderEmpty() throws IOException {
    // The tableHashWrapper has no hashes, this should result in empty source.
    assertEquals(Arrays.asList(), SourceTestUtils.readFromSource(hashTableSource, null));
  }

  @Test
  public void testHashReaderSingleHashBatch() throws IOException {
    // Setup 1 entry in this hashtable datafile. The test is setup so that HashTable datafile has
    // only 1 entry.
    List<RangeHash> expected = setupTestData(START_ROW, STOP_ROW, 1);

    assertEquals(expected, SourceTestUtils.readFromSource(hashTableSource, null));
  }

  @Test
  public void testHashReaderMultipleHashBatch() throws IOException {
    // Setup 4 entries in this hashtable datafile.
    List<RangeHash> expected = setupTestData(START_ROW, STOP_ROW, 4);
    assertEquals(expected, SourceTestUtils.readFromSource(hashTableSource, null));
  }

  //////////////////// Test the end of HashTable output when end of range is ""/////////////////
  @Test
  public void testHashReaderWithEmptyEndRow() throws IOException {
    // Setup 4 entries in this hashtable datafile with no start or stop keys set.
    List<RangeHash> expected = setupTestData(EMPTY_ROW, EMPTY_ROW, 4);
    hashTableSource.startRowInclusive = EMPTY_ROW;
    hashTableSource.stopRowExclusive = EMPTY_ROW;
    assertEquals(expected, SourceTestUtils.readFromSource(hashTableSource, null));
  }

  /////////////////////////////// Test reader.getCurrent() >= stopRow /////////////////////////

  @Test
  public void testHashReaderWorkItemEndedOnFirstBatch() throws IOException {
    // Setup 1 entry in this hashtable datafile. This entry is outside of the workitem's row
    fakeTableHashWrapper.hashes.add(KV.of(STOP_ROW, START_HASH));
    // Source will be empty as no hashes fall in its bounds.
    assertEquals(new ArrayList<RangeHash>(), SourceTestUtils.readFromSource(hashTableSource, null));
  }

  @Test
  public void testHashReaderWorkItemEndedOnSecondEntry() throws IOException {
    // Setup 1 entry in this hashtable datafile. The test is setup so that HashTable datafile has
    // only 1 entry.
    List<RangeHash> expected = setupTestData(START_ROW, STOP_ROW, 1);
    // Add a next entry at the stop row. Reader should stop and read just 1 entry.
    fakeTableHashWrapper.hashes.add(KV.of(STOP_ROW, getHash(100)));

    assertEquals(expected, SourceTestUtils.readFromSource(hashTableSource, null));
  }

  @Test
  public void testHashReaderWorkItemEndedAfterMultipleBatches() throws IOException {
    // Setup 4 entries in this hashtable datafile.
    List<RangeHash> expected = setupTestData(START_ROW, STOP_ROW, 4);
    // Add a next entry at the stop row. Reader should stop and read just 4 entry.
    fakeTableHashWrapper.hashes.add(KV.of(STOP_ROW, getHash(100)));
    assertEquals(expected, SourceTestUtils.readFromSource(hashTableSource, null));
  }

  @Test
  public void testSplitEqualsUnsplit() throws Exception {
    setupTestData(START_ROW, STOP_ROW, 6);
    fakeTableHashWrapper.partitions = Arrays.asList(getKey(2), getKey(4));
    SourceTestUtils.assertSourcesEqualReferenceSource(
        hashTableSource, hashTableSource.split(1, null), null);
  }

  @Test
  public void testUnstartedReaderEqualsStarted() throws Exception {
    setupTestData(START_ROW, STOP_ROW, 6);
    SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(
        hashTableSource.createReader(null), null);
  }
}
