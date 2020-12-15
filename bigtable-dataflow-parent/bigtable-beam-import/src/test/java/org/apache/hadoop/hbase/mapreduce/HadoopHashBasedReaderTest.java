package org.apache.hadoop.hbase.mapreduce;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.HashBasedReader;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.KeyBasedHashTableSource;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.RangeHash;
import org.apache.hadoop.hbase.mapreduce.HashTable.TableHash;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.H3;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class HadoopHashBasedReaderTest extends TestCase {

  HadoopHashTableSource source;
  KeyBasedHashTableSource keyBasedHashTableSource;
  TableHash.Reader mockTableHashReader;
  HashBasedReader reader;

  public static final String HASH_TABLE_OUTPUT_PATH_DIR = "gs://my-bucket/outputDir";
  private static final ImmutableBytesWritable START_ROW = new ImmutableBytesWritable(
      "a".getBytes());
  private static final ImmutableBytesWritable STOP_ROW = new ImmutableBytesWritable("y".getBytes());
  private static final ImmutableBytesWritable POST_STOP_ROW = new ImmutableBytesWritable(
      "z".getBytes());

  private static final ImmutableBytesWritable PARTITION1 = new ImmutableBytesWritable(
      "d".getBytes());
  private static final ImmutableBytesWritable PARTITION2 = new ImmutableBytesWritable(
      "g".getBytes());
  private static final ImmutableBytesWritable PARTITION3 = new ImmutableBytesWritable(
      "k".getBytes());
  private static final ImmutableBytesWritable EMPTY_ROW_KEY = new ImmutableBytesWritable(
      HConstants.EMPTY_BYTE_ARRAY);
  private static final ImmutableBytesWritable HASH1 = new ImmutableBytesWritable(
      "hash-001".getBytes());
  private static final ImmutableBytesWritable HASH2 = new ImmutableBytesWritable(
      "hash-002".getBytes());
  private static final ImmutableBytesWritable HASH3 = new ImmutableBytesWritable(
      "hash-003".getBytes());
  private static final ImmutableBytesWritable HASH4 = new ImmutableBytesWritable(
      "hash-004".getBytes());


  @Before
  public void setUp() throws Exception {
    super.setUp();
    SerializableConfiguration conf = new SerializableConfiguration(new Configuration());
    source = new HadoopHashTableSource(conf, StaticValueProvider.of(HASH_TABLE_OUTPUT_PATH_DIR));
    mockTableHashReader = Mockito.mock(TableHash.Reader.class);
    keyBasedHashTableSource = new KeyBasedHashTableSource(conf, HASH_TABLE_OUTPUT_PATH_DIR,
        START_ROW, STOP_ROW);
    reader = new HashBasedReader(keyBasedHashTableSource, START_ROW, STOP_ROW, mockTableHashReader);
    reset(mockTableHashReader);
  }

  @After
  public void tearDown() throws Exception {
    verifyNoMoreInteractions(mockTableHashReader);
    validateMockitoUsage();
  }

  @Test
  public void testHadoopHashTableSourceCreateReader() {
    try {
      source.createReader(null);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("Reader can't be created from HadoopHashTableSource"));
      return;
    }
    fail();
  }

  /////////////////////////////// Test the end of HashTable Output /////////////////////////

  @Test
  public void testHashReaderEmpty() throws IOException {
    when(mockTableHashReader.next()).thenReturn(false);
    assertEquals(false, reader.start());
    verify(mockTableHashReader).next();
  }

  @Test
  public void testHashReaderSingleHashBatch() throws IOException {
    // Setup 1 entry in this hashtable datafile. The test is setup so that HashTable datafile has
    // only 1 entry. So next() returns true on first call and false on the second so next() returns
    // true on first call and false on the second.
    when(mockTableHashReader.next()).thenReturn(true, false);
    when(mockTableHashReader.getCurrentKey()).thenReturn(START_ROW);
    when(mockTableHashReader.getCurrentHash()).thenReturn(HASH1);

    // First entry will contain the full source, START to END.
    RangeHash expectedRangeHash = RangeHash.of(START_ROW, STOP_ROW, HASH1);

    assertTrue(reader.start());
    assertEquals(expectedRangeHash, reader.getCurrent());

    // No more hashes in the data file.
    assertFalse(reader.advance());

    verify(mockTableHashReader, times(2)).next();
    verify(mockTableHashReader).getCurrentKey();
    verify(mockTableHashReader).getCurrentHash();
  }

  @Test
  public void testHashReaderMultipleHashBatch() throws IOException {
    // Setup 4 entries in this hashtable datafile. next() returns true 4 times for 4 entries and
    // then simulates end of hashTable output.
    when(mockTableHashReader.next()).thenReturn(true, true, true, true, false);
    // Setup 4 keys returned by reader.getCurrentKey().
    when(mockTableHashReader.getCurrentKey()).thenReturn(
        /*start*/START_ROW,
        /*start*/PARTITION1,
        /*getCurrent*/PARTITION1,
        /*advance*/PARTITION2,
        /*getCurrent*/ PARTITION2,
        /*advance*/ PARTITION3,
        /*getCurrent*/PARTITION3);
    when(mockTableHashReader.getCurrentHash()).thenReturn(HASH1, HASH2, HASH3, HASH4);

    // First entry will contain the full source, START to END.
    RangeHash expectedRangeHash1 = RangeHash.of(START_ROW, PARTITION1, HASH1);
    RangeHash expectedRangeHash2 = RangeHash.of(PARTITION1, PARTITION2, HASH2);
    RangeHash expectedRangeHash3 = RangeHash.of(PARTITION2, PARTITION3, HASH3);
    RangeHash expectedRangeHash4 = RangeHash.of(PARTITION3, STOP_ROW, HASH4);

    assertTrue(reader.start());
    assertEquals(expectedRangeHash1, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash2, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash3, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash4, reader.getCurrent());

    // No more hashes in the data file.
    assertFalse(reader.advance());

    verify(mockTableHashReader, times(5)).next();
    verify(mockTableHashReader, times(7)).getCurrentKey();
    verify(mockTableHashReader, times(4)).getCurrentHash();
  }

  /////////////////////////////// Test reader.getCurrent() >= stopRow /////////////////////////

  @Test
  public void testHashReaderWorkItemEndedOnFirstBatch() throws IOException {
    // Setup 1 entry in this hashtable datafile. This entry is outside of the workitem's row range.
    when(mockTableHashReader.next()).thenReturn(true);
    when(mockTableHashReader.getCurrentKey()).thenReturn(POST_STOP_ROW);
    when(mockTableHashReader.getCurrentHash()).thenReturn(HASH1);

    // First entry is > stopRow, so advance will never return true.
    assertFalse(reader.start());

    verify(mockTableHashReader, times(1)).next();
    verify(mockTableHashReader).getCurrentKey();
  }

  @Test
  public void testHashReaderWorkItemEndedOnSecondEntry() throws IOException {
    // Setup 2 entries in hashtable datafile. The last entry matches stopRow.
    when(mockTableHashReader.next()).thenReturn(true, true);
    // Setup 2 keys returned by reader.getCurrentKey().
    when(mockTableHashReader.getCurrentKey()).thenReturn(
        /*start*/START_ROW,
        /*start*/POST_STOP_ROW,
        /*getCurrent*/POST_STOP_ROW);
    when(mockTableHashReader.getCurrentHash()).thenReturn(HASH1, HASH2);

    RangeHash expectedRangeHash1 = RangeHash.of(START_ROW, POST_STOP_ROW, HASH1);

    assertTrue(reader.start());
    assertEquals(expectedRangeHash1, reader.getCurrent());

    // No more hashes in the data file.
    assertFalse(reader.advance());

    verify(mockTableHashReader, times(2)).next();
    verify(mockTableHashReader, times(3)).getCurrentKey();
    verify(mockTableHashReader, times(2)).getCurrentHash();
  }

  @Test
  public void testHashReaderWorkItemEndedAfterMultipleBatches() throws IOException {
    // Setup 4 entries in hashtable datafile. The last entry matches stopRow.
    when(mockTableHashReader.next()).thenReturn(true, true, true, true);
    // Setup 4 keys returned by reader.getCurrentKey().
    when(mockTableHashReader.getCurrentKey()).thenReturn(
        /*start*/START_ROW,
        /*start*/PARTITION1,
        /*getCurrent*/PARTITION1,
        /*advance*/PARTITION2,
        /*getCurrent*/ PARTITION2,
        /*advance*/ STOP_ROW,
        /*getCurrent*/ STOP_ROW);
    when(mockTableHashReader.getCurrentHash()).thenReturn(HASH1, HASH2, HASH3, HASH4);

    RangeHash expectedRangeHash1 = RangeHash.of(START_ROW, PARTITION1, HASH1);
    RangeHash expectedRangeHash2 = RangeHash.of(PARTITION1, PARTITION2, HASH2);
    RangeHash expectedRangeHash3 = RangeHash.of(PARTITION2, STOP_ROW, HASH3);

    assertTrue(reader.start());
    assertEquals(expectedRangeHash1, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash2, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash3, reader.getCurrent());

    // No more hashes in the data file.
    assertFalse(reader.advance());

    verify(mockTableHashReader, times(4)).next();
    verify(mockTableHashReader, times(7)).getCurrentKey();
    verify(mockTableHashReader, times(4)).getCurrentHash();
  }


  @Test
  public void testHashReaderWorkItemEndedStopRowNotExactMatch() throws IOException {
    // Setup 4 entries in hashtable datafile. The last entry matches stopRow.
    when(mockTableHashReader.next()).thenReturn(true, true, true, true);
    // Setup 4 keys returned by reader.getCurrentKey().
    when(mockTableHashReader.getCurrentKey()).thenReturn(
        /*start*/START_ROW,
        /*start*/PARTITION1,
        /*getCurrent*/PARTITION1,
        /*advance*/PARTITION2,
        /*getCurrent*/ PARTITION2,
        /*advance*/ POST_STOP_ROW,
        /*getCurrent*/ POST_STOP_ROW);
    when(mockTableHashReader.getCurrentHash()).thenReturn(HASH1, HASH2, HASH3, HASH4);

    RangeHash expectedRangeHash1 = RangeHash.of(START_ROW, PARTITION1, HASH1);
    RangeHash expectedRangeHash2 = RangeHash.of(PARTITION1, PARTITION2, HASH2);
    RangeHash expectedRangeHash3 = RangeHash.of(PARTITION2, POST_STOP_ROW, HASH3);

    assertTrue(reader.start());
    assertEquals(expectedRangeHash1, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash2, reader.getCurrent());
    assertTrue(reader.advance());
    assertEquals(expectedRangeHash3, reader.getCurrent());

    // No more hashes in the data file.
    assertFalse(reader.advance());

    verify(mockTableHashReader, times(4)).next();
    verify(mockTableHashReader, times(7)).getCurrentKey();
    verify(mockTableHashReader, times(4)).getCurrentHash();
  }
}