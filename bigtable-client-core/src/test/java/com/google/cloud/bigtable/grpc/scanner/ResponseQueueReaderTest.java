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
package com.google.cloud.bigtable.grpc.scanner;

import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.createContentChunk;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.createReadRowsResponse;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.extractRowsWithKeys;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.generateReadRowsResponses;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.randomBytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.bigtable.v1.Row;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

@RunWith(JUnit4.class)
public class ResponseQueueReaderTest {

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

  static void addResponsesToReader(
      ResponseQueueReader reader,
      Iterator<ReadRowsResponse> responses) throws InterruptedException {
    while (responses.hasNext()) {
      reader.add(ResultQueueEntry.newResult(responses.next()));
    }
  }

  static void assertReaderContains(ResponseQueueReader reader, Iterable<Row> rows)
      throws Exception {
    Iterator<Row> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Assert.assertEquals(iterator.next().getKey(), reader.getNextMergedRow().getKey());
    }
  }

  static void assertReaderEmpty(ResponseQueueReader reader) throws Exception {
    Assert.assertNull(reader.getNextMergedRow());
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private int defaultTimeout = (int) TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);


  @Test
  public void resultsAreReadable() throws Exception {
    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    List<ReadRowsResponse> responses =
        generateReadRowsResponses("rowKey-%s", 3);

    addResponsesToReader(reader, responses.iterator());
    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    assertReaderContains(reader, extractRowsWithKeys(responses));
    assertReaderEmpty(reader);
  }

  @Test
  public void throwablesAreThrownWhenRead() throws Exception {
    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    List<ReadRowsResponse> responses = generateReadRowsResponses("rowKey-%s", 2);
    final String innerExceptionMessage = "This message is the causedBy message";
    addResponsesToReader(reader, responses.iterator());
    reader.add(ResultQueueEntry.<ReadRowsResponse>newThrowable(new IOException(innerExceptionMessage)));

    assertReaderContains(reader, extractRowsWithKeys(responses));

    // The next call to next() should throw the exception:
    expectedException.expect(IOException.class);
    // Our exception is wrapped in another IOException so this is the expected outer message:
    expectedException.expectMessage("Error in response stream");
    expectedException.expect(new CausedByMessage(innerExceptionMessage));
    reader.getNextMergedRow();
  }

  @Test
  public void multipleResponsesAreReturnedAtOnce() throws Exception {
    int generatedResponseCount = 3;
    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    List<ReadRowsResponse> responses = generateReadRowsResponses(
        "rowKey-%s", generatedResponseCount);
    List<Row> rows = Lists.newArrayList(extractRowsWithKeys(responses));
    addResponsesToReader(reader, responses.iterator());
    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    Assert.assertEquals(generatedResponseCount + 1, reader.available());
    for (int idx = 0; idx < generatedResponseCount; idx++) {
      Assert.assertEquals(rows.get(idx).getKey(), reader.getNextMergedRow().getKey());
    }
    assertReaderEmpty(reader);
  }

  @Test
  public void multipleChunksAreMerged() throws Exception {
    String rowKey = "row-1";

    Chunk contentChunk = createContentChunk("Family1", "c1", randomBytes(10), 100L);
    Chunk contentChunk2 = createContentChunk("Family1", "c2", randomBytes(10), 100L);
    Chunk contentChunk3 = createContentChunk("Family2", null, null, 0L);
    Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();

    ReadRowsResponse response = createReadRowsResponse(rowKey, contentChunk);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, contentChunk2);
    ReadRowsResponse response3 = createReadRowsResponse(rowKey, contentChunk3);
    ReadRowsResponse response4 = createReadRowsResponse(rowKey, rowCompleteChunk);

    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    addResponsesToReader(reader, Arrays.asList(response, response2, response3, response4)
        .iterator());

    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    Row resultRow = reader.getNextMergedRow();

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

    assertReaderEmpty(reader);
  }

  @Test
  public void rowsCanBeReset() throws Exception {
    String rowKey = "row-1";

    Chunk contentChunk = createContentChunk("Family1", "c1", randomBytes(10), 10L);
    Chunk contentChunk2 = createContentChunk("Family1", "c2", randomBytes(10), 100L);

    Chunk rowResetChunk = Chunk.newBuilder().setResetRow(true).build();
    Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();

    ReadRowsResponse response = createReadRowsResponse(rowKey, contentChunk);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, rowResetChunk);
    ReadRowsResponse response3 = createReadRowsResponse(rowKey, contentChunk2);
    ReadRowsResponse response4 = createReadRowsResponse(rowKey, rowCompleteChunk);

    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    addResponsesToReader(reader, Arrays.asList(response, response2, response3, response4)
      .iterator());

    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    Row resultRow = reader.getNextMergedRow();

    Assert.assertEquals(1, resultRow.getFamiliesCount());

    Family resultFamily = resultRow.getFamilies(0);
    Assert.assertEquals("Family1", resultFamily.getName());
    Assert.assertEquals(1, resultFamily.getColumnsCount());
    Column resultColumn = resultFamily.getColumns(0);
    Assert.assertEquals(ByteString.copyFromUtf8("c2"), resultColumn.getQualifier());

    assertReaderEmpty(reader);
  }

  @Test
  public void singleChunkRowsAreRead() throws Exception {
    String rowKey = "row-1";
    Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();
    ReadRowsResponse response = createReadRowsResponse(rowKey, rowCompleteChunk);

    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    reader.add(ResultQueueEntry.<ReadRowsResponse> newResult(response));
    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    Row resultRow = reader.getNextMergedRow();
    Assert.assertEquals(0, resultRow.getFamiliesCount());
    Assert.assertEquals(ByteString.copyFromUtf8("row-1"), resultRow.getKey());

    assertReaderEmpty(reader);
  }

  @Test
  public void anEmptyStreamDoesNotThrow() throws Exception {
    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    assertReaderEmpty(reader);
    assertReaderEmpty(reader);
  }

  @Test
  public void readingPastEndReturnsNull() throws Exception {
    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    assertReaderEmpty(reader);
    assertReaderEmpty(reader);
    assertReaderEmpty(reader);
  }

  @Test
  public void endOfStreamMidRowThrows() throws Exception {
    ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);

    String rowKey = "row-1";
    Chunk contentChunk = createContentChunk("Family1", "c1", randomBytes(10), 100L);
    ReadRowsResponse response = createReadRowsResponse(rowKey, contentChunk);

    reader.add(ResultQueueEntry.<ReadRowsResponse> newResult(response));
    reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());

    expectedException.expectMessage("End of stream marker encountered while merging a row.");
    expectedException.expect(IllegalStateException.class);
    @SuppressWarnings("unused")
    Row resultRow = reader.getNextMergedRow();
  }

  @Test
  public void scannerBlocksWhenFull() throws Exception {
    // AtomicLong used as a way to easily return values from a closure:
    final AtomicLong timeBeforeBlock = new AtomicLong();
    final AtomicLong timeAfterBlock = new AtomicLong();

    final int queueDepth = 10;
    final CountDownLatch addProcessDone = new CountDownLatch(1);

    final ResponseQueueReader reader = new ResponseQueueReader(queueDepth, defaultTimeout);
    final ExecutorService executorService = Executors.newFixedThreadPool(1);

    final List<ReadRowsResponse> responses =
        generateReadRowsResponses("rowKey-%s", queueDepth * 2);

    executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        for (int i = 0; i < queueDepth; i++) {
          reader.add(ResultQueueEntry.<ReadRowsResponse> newResult(responses.get(i)));
        }
        // The next addResult will block so start a reader in a second and record the time before
        // the write and after the write. We'll expect a time greater than 500ms of blocking.
        timeBeforeBlock.set(System.currentTimeMillis());
        reader.add(ResultQueueEntry.<ReadRowsResponse> newResult(responses.get(queueDepth)));
        timeAfterBlock.set(System.currentTimeMillis());
        reader.add(ResultQueueEntry.<ReadRowsResponse> newCompletionMarker());
        addProcessDone.countDown();
        return null;
      }
    });

    // Make sure it's blocked.
    Assert.assertFalse(addProcessDone.await(500, TimeUnit.MILLISECONDS));

    // Other tests validate the contents of the queue, we just care that we blocked.
    reader.getNextMergedRow();
    // We buffer queueDepth messages in the queue, one slot is needed for scanner.complete()
    reader.getNextMergedRow();

    Assert.assertTrue(
        "Expected add process to complete within 500 ms",
        addProcessDone.await(500, TimeUnit.MILLISECONDS));
    executorService.shutdownNow();
  }

  @Test
  public void readTimeoutOnPartialRows() throws Exception {
    ResponseQueueReader reader = new ResponseQueueReader(10, 10 /* milliseconds */);

    ByteString rowKey = ByteString.copyFromUtf8("rowKey");
    // Add a single response that does not complete the row or stream:
    ReadRowsResponse response = ReadRowsResponse.newBuilder().setRowKey(rowKey).build();
    reader.add(ResultQueueEntry.<ReadRowsResponse> newResult(response));

    expectedException.expect(ScanTimeoutException.class);
    expectedException.expectMessage("Timeout while merging responses.");

    reader.getNextMergedRow();
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
        ResponseQueueReader reader = new ResponseQueueReader(10, defaultTimeout);
        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          thrownException.set(e);
          return;
        }
        try {
          reader.getNextMergedRow();
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

  @Test
  public void availableGivesTheNumberOfBufferedItems() throws Exception {
    final int queueDepth = 10;

    ResponseQueueReader reader = new ResponseQueueReader(queueDepth, defaultTimeout);
    final List<ReadRowsResponse> responses = generateReadRowsResponses("rowKey-%s", queueDepth);

    for (ReadRowsResponse response : responses) {
      reader.add(ResultQueueEntry.newResult(response));
    }

    Assert.assertEquals("Expected same number of items in scanner as added", queueDepth,
      reader.available());

    reader.getNextMergedRow();

    Assert.assertEquals("Expected same number of items in scanner as added less one",
      queueDepth - 1, reader.available());
  }
}
