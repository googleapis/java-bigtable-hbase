/*
 * Copyright 2017 Google LLC
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

import com.google.bigtable.repackaged.com.google.api.gax.rpc.WatchdogTimeoutException;
import com.google.bigtable.repackaged.com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowRange;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowSet;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.internal.ByteStringComparator;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableList;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.repackaged.com.google.protobuf.BytesValue;
import com.google.bigtable.repackaged.com.google.protobuf.StringValue;
import com.google.bigtable.repackaged.io.grpc.Server;
import com.google.bigtable.repackaged.io.grpc.ServerBuilder;
import com.google.bigtable.repackaged.io.grpc.stub.StreamObserver;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Tests for {@link com.google.cloud.bigtable.beam.CloudBigtableIO.Reader}.
 *
 * @author sduskis
 */
public class CloudBigtableIOReaderTest {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock Connection mockConnection;

  @Mock ResultScanner mockScanner;

  @Mock CloudBigtableIO.AbstractSource mockSource;

  private Server server;

  private FakeService fakeService;

  int port;

  @Before
  public void setup() throws IOException {
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }
    fakeService = new FakeService();
    server = ServerBuilder.forPort(port).addService(fakeService).build().start();
  }

  @After
  public void close() {
    if (server != null) {
      server.shutdown();
    }
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
    Result row =
        Result.create(
            ImmutableList.<Cell>of(
                new com.google.cloud.bigtable.hbase.adapters.read.RowCell(
                    Bytes.toBytes(rowKey),
                    Bytes.toBytes("cf"),
                    Bytes.toBytes("q"),
                    10L,
                    Bytes.toBytes("value"),
                    ImmutableList.of("label"))));
    when(mockScanner.next()).thenReturn(row);
  }

  private CloudBigtableIO.Reader initializeReader(CloudBigtableScanConfiguration config) {
    when(mockSource.getConfiguration()).thenReturn(config);
    return new CloudBigtableIO.Reader(mockSource) {
      @Override
      void initializeScanner() throws IOException {
        setConnection(mockConnection);
        setScanner(mockScanner);
      }
    };
  }

  @Test
  public void testPercent() throws IOException {
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

  private void testTrackerAtKey(
      CloudBigtableIO.Reader underTest,
      ByteKeyRangeTracker tracker,
      final String key,
      final int count)
      throws IOException {
    setRowKey(key);
    tracker.tryReturnRecordAt(true, ByteKey.copyFrom(key.getBytes()));
    Assert.assertTrue(underTest.start());
    Assert.assertEquals(count, underTest.getRowsReadCount());
    Assert.assertEquals(
        tracker.getFractionConsumed(), underTest.getFractionConsumed().doubleValue(), .001d);
  }

  @Test
  public void testSplits() throws IOException {
    byte[] startKey = "AAAAAAA".getBytes();
    byte[] stopKey = "ZZZZZZZ".getBytes();
    CloudBigtableScanConfiguration config =
        createDefaultConfig().withKeys(startKey, stopKey).build();
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

  @Test
  public void testRetryIdleTimeoutWithScan() throws Exception {
    byte[] startKey = "A".getBytes();
    byte[] endKey = "B".getBytes();

    List<ReadRowsResponse> responses = generateResponses(fakeService, "A", "B", true, 10);

    responses.remove(responses.size() - 1);

    Scan scan = new Scan().withStartRow(startKey).withStopRow(endKey);

    CloudBigtableScanConfiguration config =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId("project")
            .withInstanceId("instsance")
            .withTableId("table")
            .withScan(scan)
            .withConfiguration(
                BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port)
            .withConfiguration(BigtableOptionsFactory.BIGTABLE_TEST_IDLE_TIMEOUT_MS, "15000")
            .build();

    CloudBigtableIO.Source source = (CloudBigtableIO.Source) CloudBigtableIO.read(config);
    CloudBigtableIO.Reader reader = (CloudBigtableIO.Reader) source.createReader(null);

    List<Result> actual = new ArrayList<>();

    // Reader.start() will read the first row
    reader.start();
    actual.add(reader.getCurrent());
    int count = 1;

    boolean sleep = false;
    while (reader.advance()) {
      count++;
      actual.add(reader.getCurrent());
      if (!sleep) {
        Thread.sleep(20 * 1000);
        sleep = true;
      }
    }

    // Make sure idle timeout is retried
    Assert.assertTrue(fakeService.count.get() > 1);
    Assert.assertEquals(responses.size(), count);
    Assert.assertEquals(
        responses.stream()
            .map(response -> response.getChunks(0).getRowKey().toStringUtf8())
            .collect(Collectors.toList()),
        actual.stream()
            .map(result -> ByteString.copyFrom(result.getRow()).toStringUtf8())
            .collect(Collectors.toList()));
  }

  @Test
  public void testRetryIdleTimeoutWithReadRowsRequest() throws Exception {
    byte[] startKey = "A".getBytes();
    byte[] endKey = "B".getBytes();

    List<ReadRowsResponse> responses = generateResponses(fakeService, "A", "B", true, 10);

    responses.remove(responses.size() - 1);

    ReadRowsRequest request =
        ReadRowsRequest.newBuilder()
            .setTableName("projects/project/instances/instance/tables/table")
            .setRows(
                RowSet.newBuilder()
                    .addRowRanges(
                        RowRange.newBuilder()
                            .setStartKeyClosed(ByteString.copyFrom(startKey))
                            .setEndKeyOpen(ByteString.copyFrom(endKey))
                            .build())
                    .build())
            .build();

    CloudBigtableScanConfiguration config =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId("project")
            .withInstanceId("instsance")
            .withTableId("table")
            .withRequest(request)
            .withConfiguration(
                BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port)
            .withConfiguration(BigtableOptionsFactory.BIGTABLE_TEST_IDLE_TIMEOUT_MS, "15000")
            .build();

    CloudBigtableIO.Source source = (CloudBigtableIO.Source) CloudBigtableIO.read(config);
    CloudBigtableIO.Reader reader = (CloudBigtableIO.Reader) source.createReader(null);

    List<Result> actual = new ArrayList<>();

    // Reader.start() will read the first row
    reader.start();
    actual.add(reader.getCurrent());
    int count = 1;

    boolean sleep = false;
    while (reader.advance()) {
      count++;
      actual.add(reader.getCurrent());
      if (!sleep) {
        Thread.sleep(20 * 1000);
        sleep = true;
      }
    }

    // Make sure idle timeout is retried
    Assert.assertTrue(fakeService.count.get() > 1);
    Assert.assertEquals(responses.size(), count);
    Assert.assertEquals(
        responses.stream()
            .map(response -> response.getChunks(0).getRowKey())
            .collect(Collectors.toList()),
        actual.stream()
            .map(result -> ByteString.copyFrom(result.getRow()))
            .collect(Collectors.toList()));
  }

  @Test
  public void testDisableRetryIdleTimeout() throws Exception {
    byte[] startKey = "A".getBytes();
    byte[] endKey = "B".getBytes();

    List<ReadRowsResponse> responses = generateResponses(fakeService, "A", "B", true, 10);

    responses.remove(responses.size() - 1);

    ReadRowsRequest request =
        ReadRowsRequest.newBuilder()
            .setTableName("projects/project/instances/instance/tables/table")
            .setRows(
                RowSet.newBuilder()
                    .addRowRanges(
                        RowRange.newBuilder()
                            .setStartKeyClosed(ByteString.copyFrom(startKey))
                            .setEndKeyOpen(ByteString.copyFrom(endKey))
                            .build())
                    .build())
            .build();

    CloudBigtableScanConfiguration config =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId("project")
            .withInstanceId("instsance")
            .withTableId("table")
            .withRequest(request)
            .withConfiguration(
                BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port)
            .withConfiguration(BigtableOptionsFactory.BIGTABLE_TEST_IDLE_TIMEOUT_MS, "15000")
            .withConfiguration(CloudBigtableIO.Reader.RETRY_IDLE_TIMEOUT, "false")
            .build();

    CloudBigtableIO.Source source = (CloudBigtableIO.Source) CloudBigtableIO.read(config);
    CloudBigtableIO.Reader reader = (CloudBigtableIO.Reader) source.createReader(null);

    // Reader.start() will read the first row
    reader.start();

    boolean sleep = false;
    try {
      while (reader.advance()) {
        if (!sleep) {
          Thread.sleep(20 * 1000);
          sleep = true;
        }
      }
      Assert.fail("Should throw idle timeout exception");
    } catch (Throwable e) {
      Throwable throwable = e;
      while (throwable != null && !(throwable instanceof WatchdogTimeoutException)) {
        throwable = throwable.getCause();
      }

      if (throwable == null) {
        Assert.fail("Exception is not idle timeout");
      }

      Assert.assertTrue(throwable.getMessage().contains("idle"));
    }
  }

  private List<ReadRowsResponse> generateResponses(
      FakeService fakeService, String starKey, String endKey, boolean addEndKey, int numResponses) {
    List<ReadRowsResponse> responses = new ArrayList<>();

    for (int i = 0; i < numResponses; i++) {
      responses.add(
          ReadRowsResponse.newBuilder()
              .addChunks(
                  ReadRowsResponse.CellChunk.newBuilder()
                      .setRowKey(ByteString.copyFromUtf8(starKey + i))
                      .setFamilyName(StringValue.of("cf"))
                      .setQualifier(
                          BytesValue.newBuilder().setValue(ByteString.copyFrom("q".getBytes())))
                      .setTimestampMicros(1000)
                      .setValue(ByteString.copyFromUtf8("value"))
                      .setCommitRow(true)
                      .build())
              .build());
    }

    if (addEndKey) {
      // add the end key which shouldn't be included in the response
      responses.add(
          ReadRowsResponse.newBuilder()
              .addChunks(
                  ReadRowsResponse.CellChunk.newBuilder()
                      .setRowKey(ByteString.copyFromUtf8(endKey))
                      .setFamilyName(StringValue.of("cf"))
                      .setQualifier(
                          BytesValue.newBuilder().setValue(ByteString.copyFrom("q".getBytes())))
                      .setTimestampMicros(1000)
                      .setValue(ByteString.copyFromUtf8("value"))
                      .setCommitRow(true)
                      .build())
              .build());
    }

    fakeService.addResponses(responses);

    return responses;
  }

  private void split(CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker) {
    double halfway = bisectPercentage(baseRangeTracker);
    reader.splitAtFraction(halfway);
    ByteKey bisectedKey = baseRangeTracker.getRange().interpolateKey(halfway);
    baseRangeTracker.trySplitAtPosition(bisectedKey);
    compare(reader, baseRangeTracker);
  }

  private static void compare(CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker) {
    Assert.assertEquals(baseRangeTracker.getFractionConsumed(), reader.getFractionConsumed(), 0.01);
  }

  private static void bisect(CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker) {
    double halfway = bisectPercentage(baseRangeTracker);
    ByteKey bisectedKey = baseRangeTracker.getRange().interpolateKey(halfway);
    setKey(reader, baseRangeTracker, bisectedKey);
    compare(reader, baseRangeTracker);
  }

  private static void setKey(
      CloudBigtableIO.Reader reader, ByteKeyRangeTracker baseRangeTracker, ByteKey key) {
    reader.getRangeTracker().tryReturnRecordAt(true, key);
    baseRangeTracker.tryReturnRecordAt(true, key);
  }

  private static double bisectPercentage(ByteKeyRangeTracker baseRangeTracker) {
    double fractionConsumed = baseRangeTracker.getFractionConsumed();
    return (1.0 + fractionConsumed) / 2.0;
  }

  static class FakeService extends BigtableGrpc.BigtableImplBase {
    List<ReadRowsResponse> responses = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(0);

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      count.getAndIncrement();
      ByteString startKey = request.getRows().getRowRanges(0).getStartKeyClosed();
      ByteString endKey = request.getRows().getRowRanges(0).getEndKeyOpen();
      int index = 0;

      while (index < responses.size()) {
        ReadRowsResponse current = responses.get(index++);
        if (ByteStringComparator.INSTANCE.compare(current.getChunks(0).getRowKey(), startKey) >= 0
            && ByteStringComparator.INSTANCE.compare(current.getChunks(0).getRowKey(), endKey)
                < 0) {
          responseObserver.onNext(current);
        }
      }
      responseObserver.onCompleted();
    }

    void addResponses(List<ReadRowsResponse> allResponses) {
      responses.addAll(allResponses);
    }
  }
}
