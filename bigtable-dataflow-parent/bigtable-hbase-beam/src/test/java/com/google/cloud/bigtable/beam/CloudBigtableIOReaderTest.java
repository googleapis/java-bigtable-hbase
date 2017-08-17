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
package com.google.cloud.bigtable.beam;

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;

/**
 * Tests for {@link CloudBigtableIO.Reader}.
 * @author sduskis
 *
 */
public class CloudBigtableIOReaderTest {

  @Mock
  BigtableSession mockSession;

  @Mock
  ResultScanner<FlatRow> mockScanner;

  @Mock
  CloudBigtableIO.AbstractSource mockSource;

  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
  }

  private CloudBigtableScanConfiguration.Builder createDefaultConfig() {
    return new CloudBigtableScanConfiguration.Builder()
        .withProjectId("test")
        .withInstanceId("test")
        .withTableId("test")
        .withRequest(ReadRowsRequest.getDefaultInstance())
        .withKeys(new byte[0], new byte[0]);
  }

  @Test
  public void testBasic() throws IOException {

    CloudBigtableIO.Reader underTest = initializeReader(createDefaultConfig().build());

    setRowKey("a");
    Assert.assertTrue(underTest.start());
    Assert.assertEquals(1, underTest.getRowsReadCount());

    when(mockScanner.next()).thenReturn(null);
    Assert.assertFalse(underTest.advance());
    Assert.assertEquals(1, underTest.getRowsReadCount());

    underTest.close();
  }

  private void setRowKey(String rowKey) throws IOException {
    ByteString rowKeyByteString = ByteString.copyFrom(Bytes.toBytes(rowKey));
    FlatRow row = FlatRow.newBuilder().withRowKey(rowKeyByteString).build();
    when(mockScanner.next()).thenReturn(row);
  }

  private CloudBigtableIO.Reader initializeReader(CloudBigtableScanConfiguration config) {
    when(mockSource.getConfiguration()).thenReturn(config);
    return new CloudBigtableIO.Reader(mockSource) {
      @Override
      void initializeScanner() throws IOException {
        setSession(mockSession);
        setScanner(mockScanner);
      }
    };
  }

  @Test
  public void testPercent() throws IOException{
    byte[] start = "aa".getBytes();
    byte[] end = "zz".getBytes();
    CloudBigtableScanConfiguration config = createDefaultConfig().withKeys(start, end).build();
    CloudBigtableIO.Reader underTest = initializeReader(config);
    ByteKeyRangeTracker tracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(ByteKey.copyFrom(start), ByteKey.copyFrom(end)));

    testTrackerAtKey(underTest, tracker, "dd", 1);
    testTrackerAtKey(underTest, tracker, "qq", 2);

    double splitAtFraction =
        (1 - tracker.getFractionConsumed()) * .5 + tracker.getFractionConsumed();
    ByteKey newSplitEnd = tracker.getRange().interpolateKey(splitAtFraction);

    underTest.splitAtFraction(splitAtFraction);
    tracker.trySplitAtPosition(newSplitEnd);

    Assert.assertEquals(tracker.getFractionConsumed(), underTest.getFractionConsumed(), 0.0001d);
  }

  private void testTrackerAtKey(CloudBigtableIO.Reader underTest, ByteKeyRangeTracker tracker,
      final String key, final int count) throws IOException {
    setRowKey(key);
    tracker.tryReturnRecordAt(true, ByteKey.copyFrom(key.getBytes()));
    Assert.assertTrue(underTest.start());
    Assert.assertEquals(count, underTest.getRowsReadCount());
    Assert.assertEquals(tracker.getFractionConsumed(),
      underTest.getFractionConsumed().doubleValue(), .001d);
  }

  @Test
  public void testSplits() throws IOException{
    byte[] startKey = "AAAAAAA".getBytes();
    byte[] stopKey = "ZZZZZZZ".getBytes();
    CloudBigtableScanConfiguration config = createDefaultConfig().withKeys(startKey, stopKey).build();
    CloudBigtableIO.Source source = (CloudBigtableIO.Source) CloudBigtableIO.read(config);
    BoundedSource<Result> sourceWithKeys = source.createSourceWithKeys(startKey, stopKey, 10);

    CloudBigtableIO.Reader reader = (CloudBigtableIO.Reader) sourceWithKeys.createReader(null);
    ByteKey startByteKey = ByteKey.copyFrom(startKey);
    ByteKey stopByteKey = ByteKey.copyFrom(stopKey);
    ByteKeyRangeTracker baseRangeTracker =
        ByteKeyRangeTracker.of(ByteKeyRange.of(startByteKey, stopByteKey));

    setKey(reader, baseRangeTracker, ByteKey.copyFrom("B".getBytes()));

    for (int i = 0; i < 20; i++) {
      compare(reader, baseRangeTracker);
      bisect(reader, baseRangeTracker);
      split(reader, baseRangeTracker);
    }
  }

  private void split(CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker) {
    double halfway = bisectPercentage(baseRangeTracker);
    reader.splitAtFraction(halfway);
    ByteKey bisectedKey = baseRangeTracker.getRange().interpolateKey(halfway);
    baseRangeTracker.trySplitAtPosition(bisectedKey);
    compare(reader, baseRangeTracker);
  }

  private static void compare(CloudBigtableIO.Reader reader,
      ByteKeyRangeTracker baseRangeTracker) {
    Assert.assertEquals(baseRangeTracker.getFractionConsumed(), reader.getFractionConsumed(), 0.01);
  }

  private static void bisect(CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker) {
    double halfway = bisectPercentage(baseRangeTracker);
    ByteKey bisectedKey = baseRangeTracker.getRange().interpolateKey(halfway);
    setKey(reader, baseRangeTracker, bisectedKey);
    compare(reader, baseRangeTracker);
  }

  private static void setKey(CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker,
      ByteKey key) {
    reader.getRangeTracker().tryReturnRecordAt(true, key);
    baseRangeTracker.tryReturnRecordAt(true, key);
  }

  private static double bisectPercentage(ByteKeyRangeTracker baseRangeTracker) {
    double fractionConsumed = baseRangeTracker.getFractionConsumed();
    return (1.0 + fractionConsumed) / 2.0;
  }
}