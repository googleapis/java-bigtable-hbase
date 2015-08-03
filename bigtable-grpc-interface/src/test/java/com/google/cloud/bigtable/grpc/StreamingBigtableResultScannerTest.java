/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.api.client.util.Strings;
import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.bigtable.v1.Row;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(JUnit4.class)
public class StreamingBigtableResultScannerTest {

  /**
   * A matcher that requires the message of a causing exception to match a given substring
   */
  public static class CausedByMessage extends BaseMatcher<Throwable> {
    private String capturedExceptionMessage;
    private final String expectedCausedByMessage;

    public CausedByMessage(String expectedCausedByMessageSubstring) {
      this.expectedCausedByMessage = expectedCausedByMessageSubstring;
    }

    @Override
    public boolean matches(Object o) {
      if (o instanceof Throwable) {
        Throwable exception = (Throwable) o;
        Throwable causedBy = exception.getCause();
        if (causedBy == null) {
          capturedExceptionMessage = "(null getCause())";
          return false;
        }
        capturedExceptionMessage = causedBy.getMessage();
        return capturedExceptionMessage.contains(expectedCausedByMessage);
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(
          String.format(
              "Expected a caused by exception message containing '%s', but found '%s'",
              expectedCausedByMessage,
              capturedExceptionMessage));
    }
  }

  static Chunk createContentChunk(
      String familyName, String columnQualifier, byte[] value, long timestamp) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(familyName), "Family name may not be null or empty");

    Family.Builder familyBuilder = Family.newBuilder()
        .setName(familyName);

    if (columnQualifier != null) {
      Column.Builder columnBuilder = Column.newBuilder();
      columnBuilder.setQualifier(ByteString.copyFromUtf8(columnQualifier));
      familyBuilder.addColumns(columnBuilder);

      if (value != null) {
        Cell.Builder cellBuilder = Cell.newBuilder();
        cellBuilder.setTimestampMicros(timestamp);
        cellBuilder.setValue(ByteString.copyFrom(value));
        columnBuilder.addCells(cellBuilder);
      }
    }

    return Chunk.newBuilder().setRowContents(familyBuilder).build();
  }

  static ReadRowsResponse createReadRowsResponse(String rowKey, Chunk ... chunks) {
    return ReadRowsResponse.newBuilder()
        .setRowKey(ByteString.copyFromUtf8(rowKey))
        .addAllChunks(Arrays.asList(chunks))
        .build();
  }

  static byte[] randomBytes(int length) {
    Random rnd = new Random();
    byte[] result = new byte[length];
    rnd.nextBytes(result);
    return result;
  }

  static List<ReadRowsResponse> generateReadRowsResponses(
      String rowKeyFormat, int count) {

    List<ReadRowsResponse> results = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String rowKey = String.format(rowKeyFormat, i);
      Chunk contentChunk = createContentChunk("Family1", null, null, 0L);
      Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();

      ReadRowsResponse response =
          ReadRowsResponse.newBuilder()
              .addChunks(contentChunk)
              .addChunks(rowCompleteChunk)
              .setRowKey(ByteString.copyFromUtf8(rowKey))
              .build();

      results.add(response);
    }

    return results;
  }

  static void addResponsesToScanner(
      StreamingBigtableResultScanner scanner,
      Iterator<ReadRowsResponse> responses) {
    while (responses.hasNext()) {
      scanner.addResult(responses.next());
    }
  }

  static void assertScannerContains(
      StreamingBigtableResultScanner scanner, Iterable<Row> rows)
      throws IOException {
    Iterator<Row> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Assert.assertEquals(iterator.next().getKey(), scanner.next().getKey());
    }
  }

  static void assertScannerEmpty(StreamingBigtableResultScanner scanner) throws IOException {
    Assert.assertNull(scanner.next());
  }

  static Iterable<Row> extractRowsWithKeys(Iterable<ReadRowsResponse> responses) {
    return Iterables.transform(responses, new Function<ReadRowsResponse, Row>() {
      @Override
      public Row apply(ReadRowsResponse readRowsResponse) {
        return Row.newBuilder().setKey(readRowsResponse.getRowKey()).build();
      }
    });
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private int defaultTimeout =
      (int) TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

  @Test
  public void resultsAreReadable() throws IOException {
    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    List<ReadRowsResponse> responses =
        generateReadRowsResponses("rowKey-%s", 3);

    addResponsesToScanner(scanner, responses.iterator());
    scanner.complete();

    assertScannerContains(scanner, extractRowsWithKeys(responses));
    assertScannerEmpty(scanner);
  }

  @Test
  public void cancellationIsSignalled() throws IOException, InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    CancellationToken cancellationToken = new CancellationToken();
    cancellationToken.addListener(new Runnable() {
      @Override
      public void run() {
        countDownLatch.countDown();
      }
    }, Executors.newCachedThreadPool());

    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    scanner.close();
    // Verify that we've in fact finished.
    Assert.assertTrue("Waited 10ms for cancellation, but it was not triggered.",
        countDownLatch.await(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void throwablesAreThrownWhenRead() throws IOException {
    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    List<ReadRowsResponse> responses = generateReadRowsResponses("rowKey-%s", 2);
    final String innerExceptionMessage = "This message is the causedBy message";
    addResponsesToScanner(scanner, responses.iterator());
    scanner.setError(new IOException(innerExceptionMessage));

    assertScannerContains(scanner, extractRowsWithKeys(responses));

    // The next call to next() should throw the exception:
    expectedException.expect(IOException.class);
    // Our exception is wrapped in another IOException so this is the expected outer message:
    expectedException.expectMessage("Error in response stream");
    expectedException.expect(new CausedByMessage(innerExceptionMessage));
    scanner.next();
  }

  @Test
  public void multipleResponsesAreReturnedAtOnce() throws IOException {
    int generatedResponseCount = 3;
    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    List<ReadRowsResponse> responses = generateReadRowsResponses(
        "rowKey-%s", generatedResponseCount);
    List<Row> rows = Lists.newArrayList(extractRowsWithKeys(responses));
    addResponsesToScanner(scanner, responses.iterator());
    scanner.complete();

    Row[] readRows = scanner.next(generatedResponseCount + 1);
    for (int idx = 0; idx < generatedResponseCount; idx++) {
      Assert.assertEquals(rows.get(idx).getKey(), readRows[idx].getKey());
    }
    Assert.assertEquals(generatedResponseCount, readRows.length);
    assertScannerEmpty(scanner);
  }

  @Test
  public void multipleChunksAreMerged() throws IOException {
    String rowKey = "row-1";

    Chunk contentChunk = createContentChunk("Family1", "c1", randomBytes(10), 100L);
    Chunk contentChunk2 = createContentChunk("Family1", "c2", randomBytes(10), 100L);
    Chunk contentChunk3 = createContentChunk("Family2", null, null, 0L);
    Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();

    ReadRowsResponse response = createReadRowsResponse(rowKey, contentChunk);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, contentChunk2);
    ReadRowsResponse response3 = createReadRowsResponse(rowKey, contentChunk3);
    ReadRowsResponse response4 = createReadRowsResponse(rowKey, rowCompleteChunk);

    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    scanner.addResult(response);
    scanner.addResult(response2);
    scanner.addResult(response3);
    scanner.addResult(response4);

    scanner.complete();

    Row resultRow = scanner.next();

    Assert.assertEquals(2, resultRow.getFamiliesCount());

    Map<String, Family> familyMap = new HashMap<>();
    for (Family family : resultRow.getFamiliesList()) {
      familyMap.put(family.getName(), family);
    }

    Family resultFamily1 = familyMap.get("Family1");
    Assert.assertEquals(2, resultFamily1.getColumnsCount());
    Assert.assertEquals(ByteString.copyFromUtf8("c1"),
        resultFamily1.getColumns(0).getQualifier());
    Assert.assertEquals(ByteString.copyFromUtf8("c2"),
        resultFamily1.getColumns(1).getQualifier());

    Family resultFamily2 = familyMap.get("Family2");
    Assert.assertEquals(0, resultFamily2.getColumnsCount());

    assertScannerEmpty(scanner);
  }

  @Test
  public void rowsCanBeReset() throws IOException {
    String rowKey = "row-1";

    Chunk contentChunk = createContentChunk("Family1", "c1", randomBytes(10), 10L);
    Chunk contentChunk2 = createContentChunk("Family1", "c2", randomBytes(10), 100L);

    Chunk rowResetChunk = Chunk.newBuilder().setResetRow(true).build();
    Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();

    ReadRowsResponse response = createReadRowsResponse(rowKey, contentChunk);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, rowResetChunk);
    ReadRowsResponse response3 = createReadRowsResponse(rowKey, contentChunk2);
    ReadRowsResponse response4 = createReadRowsResponse(rowKey, rowCompleteChunk);

    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    scanner.addResult(response);
    scanner.addResult(response2);
    scanner.addResult(response3);
    scanner.addResult(response4);

    scanner.complete();

    Row resultRow = scanner.next();

    Assert.assertEquals(1, resultRow.getFamiliesCount());

    Family resultFamily = resultRow.getFamilies(0);
    Assert.assertEquals("Family1", resultFamily.getName());
    Assert.assertEquals(1, resultFamily.getColumnsCount());
    Column resultColumn = resultFamily.getColumns(0);
    Assert.assertEquals(ByteString.copyFromUtf8("c2"), resultColumn.getQualifier());

    assertScannerEmpty(scanner);
  }

  @Test
  public void singleChunkRowsAreRead() throws IOException {
    String rowKey = "row-1";
    Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();
    ReadRowsResponse response = createReadRowsResponse(rowKey, rowCompleteChunk);

    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    scanner.addResult(response);
    scanner.complete();

    Row resultRow = scanner.next();
    Assert.assertEquals(0, resultRow.getFamiliesCount());
    Assert.assertEquals(ByteString.copyFromUtf8("row-1"), resultRow.getKey());

    assertScannerEmpty(scanner);
  }

  @Test
  public void anEmptyStreamDoesNotThrow() throws IOException {
    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    scanner.complete();

    Row resultRow = scanner.next();

    Assert.assertNull(resultRow);
    assertScannerEmpty(scanner);
  }

  @Test
  public void readingPastEndReturnsNull() throws IOException {
    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    scanner.complete();

    Row resultRow = scanner.next();
    Assert.assertNull(resultRow);
    resultRow = scanner.next();
    Assert.assertNull(resultRow);
    assertScannerEmpty(scanner);
  }

  @Test
  public void endOfStreamMidRowThrows() throws IOException {
    CancellationToken cancellationToken = new CancellationToken();
    StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);

    String rowKey = "row-1";
    Chunk contentChunk = createContentChunk("Family1", "c1", randomBytes(10), 100L);
    ReadRowsResponse response = createReadRowsResponse(rowKey, contentChunk);

    scanner.addResult(response);
    scanner.complete();

    expectedException.expectMessage("End of stream marker encountered while merging a row.");
    expectedException.expect(IllegalStateException.class);
    @SuppressWarnings("unused")
    Row resultRow = scanner.next();
  }

  @Test
  public void scannerBlocksWhenFull() throws IOException, InterruptedException {
    // AtomicLong used as a way to easily return values from a closure:
    final AtomicLong timeBeforeBlock = new AtomicLong();
    final AtomicLong timeAfterBlock = new AtomicLong();

    final int queueDepth = 10;
    final CountDownLatch addProcessDone = new CountDownLatch(1);

    CancellationToken cancellationToken = new CancellationToken();
    final StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(queueDepth, defaultTimeout, cancellationToken);

    final List<ReadRowsResponse> responses =
        generateReadRowsResponses("rowKey-%s", queueDepth * 2);
    final ExecutorService executorService = Executors.newFixedThreadPool(1);

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < queueDepth; i++) {
          scanner.addResult(responses.get(i));
        }
        // The next addResult will block so start a reader in a second and record the time before
        // the write and after the write. We'll expect a time greater than 500ms of blocking.
        timeBeforeBlock.set(System.currentTimeMillis());
        scanner.addResult(responses.get(queueDepth));
        timeAfterBlock.set(System.currentTimeMillis());
        scanner.complete();
        addProcessDone.countDown();
      }
    });

    // Make sure it's blocked.
    Assert.assertFalse(addProcessDone.await(500, TimeUnit.MILLISECONDS));

    // Other tests validate the contents of the queue, we just care that we blocked.
    scanner.next();
    // We buffer queueDepth messages in the queue, one slot is needed for scanner.complete()
    scanner.next();

    Assert.assertTrue(
        "Expected add process to complete within 500 ms",
        addProcessDone.await(500, TimeUnit.MILLISECONDS));

    executorService.shutdownNow();
  }

  @Test
  public void readTimeoutOnPartialRows() throws IOException, InterruptedException {
    CancellationToken cancellationToken = new CancellationToken();
    final StreamingBigtableResultScanner scanner =
        new StreamingBigtableResultScanner(
            10, 10 /* timeout millis */, cancellationToken);

    ByteString rowKey = ByteString.copyFromUtf8("rowKey");
    // Add a single response that does not complete the row or stream:
    scanner.addResult(ReadRowsResponse.newBuilder().setRowKey(rowKey).build());

    expectedException.expect(ReadTimeoutException.class);
    expectedException.expectMessage("Timeout while merging responses.");

    scanner.next();
  }

  @Test
  public void threadInterruptStatusIsSetOnInterruptedException()
      throws BrokenBarrierException, InterruptedException {
    final AtomicReference<Exception> thrownException = new AtomicReference<>();
    final AtomicBoolean interruptedSet = new AtomicBoolean(false);
    // Two parties involved in sync: one for the main thread and one for
    // the test thread.
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Thread testThread = new Thread(new Runnable() {
      @Override
      public void run() {
        CancellationToken cancellationToken = new CancellationToken();
        StreamingBigtableResultScanner scanner =
            new StreamingBigtableResultScanner(10, defaultTimeout, cancellationToken);
        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          thrownException.set(e);
          return;
        }
        try {
          scanner.next();
        } catch (IOException e) {
          thrownException.set(e);
          if (Thread.currentThread().isInterrupted()) {
            interruptedSet.set(true);
          }
        }
      }
    });

    testThread.start();
    barrier.await();
    // Even with a barrier, we need to wait for our scanner.next() call to be invoked:
    // TODO(angusdavis): refactor StreamingBigtableResultScanner to take a queue we
    // control and can throw InterruptedException from at will.
    Thread.sleep(50);
    testThread.interrupt();
    testThread.join();

    Assert.assertNotNull(
        "An exception should have been thrown while calling scanner.next()",
        thrownException.get());
    Assert.assertTrue(
        "Interrupted exception set as cause of IOException",
        thrownException.get().getCause() instanceof InterruptedException);

    Assert.assertTrue(
        "testThread should have recorded that it was interrupted.",
        interruptedSet.get());
  }
}
