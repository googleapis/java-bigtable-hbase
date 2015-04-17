package com.google.cloud.hadoop.hbase;

import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.bigtable.v1.ReadRowsResponse.Chunk.ChunkCase;
import com.google.bigtable.v1.Row;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ResultScanner} implementation against the v1 bigtable API.
 */
public class StreamingBigtableResultScanner extends AbstractBigtableResultScanner {

  /**
   * Builds a complete Row from partial ReadRowsResponse objects. This class
   * does not currently handle multiple interleaved rows. It is assumed that it is
   * handling results for a request with allow_row_interleaving = false.
   * <p/>
   * Each RowMerger object is valid only for building a single Row. Expected usage
   * is along the lines of:
   * <pre>
   * RowMerger rm = new RowMerger();
   * while (!rm.isRowCommited()) {
   *   rm.addPartialRow(...);
   * }
   * Row r = rm.buildRow();
   * </pre>
   */
  public static class RowMerger {
    private final Map<String, Family.Builder> familyMap = new HashMap<>();
    private boolean committed = false;
    private ByteString currentRowKey;

    /**
     * Add a partial row response to this builder.
     */
    public void addPartialRow(ReadRowsResponse partialRow) {
      Preconditions.checkState(
          currentRowKey == null || currentRowKey.equals(partialRow.getRowKey()),
          "Interleaved ReadRowResponse messages are not supported.");

      if (currentRowKey == null) {
        currentRowKey = partialRow.getRowKey();
      }

      for (Chunk chunk : partialRow.getChunksList()) {
        Preconditions.checkState(!committed, "Encountered chunk after row commit.");
        if (chunk.getChunkCase() == ChunkCase.ROW_CONTENTS) {
          merge(familyMap, chunk.getRowContents());
        } else if (chunk.getChunkCase() == ChunkCase.RESET_ROW) {
          familyMap.clear();
        } else if (chunk.getChunkCase() == ChunkCase.COMMIT_ROW) {
          committed = true;
        } else {
          throw new IllegalStateException(
              String.format("Unknown ChunkCase encountered %s", chunk.getChunkCase()));
        }
      }
    }

    /**
     * Indicate whether a Chunk of type COMMIT_ROW been encountered.
     */
    public boolean isRowCommitted() {
      return committed;
    }

    /**
     * Construct a row from previously seen partial rows. This method may only be invoked
     * when isRowCommitted returns true indicating a COMMIT_ROW chunk has been encountered.
     */
    public Row buildRow() {
      Preconditions.checkState(committed,
          "Cannot build a Row object if we have not yet encountered a COMMIT_ROW chunk.");
      Row.Builder currentRowBuilder = Row.newBuilder();
      currentRowBuilder.setKey(currentRowKey);
      for (Family.Builder builder : familyMap.values()) {
        currentRowBuilder.addFamilies(builder.build());
      }
      return currentRowBuilder.build();
    }

    // Merge newRowContents into the map of family builders, creating one if necessary.
    private void merge(
        Map<String, Family.Builder> familyBuilderMap,
        Family newRowContents) {
      Family.Builder familyBuilder =
          getOrCreateFamilyBuilder(familyBuilderMap, newRowContents);
      familyBuilder.addAllColumns(newRowContents.getColumnsList());
    }

    private Family.Builder getOrCreateFamilyBuilder(
        Map<String, Family.Builder> familyBuilderMap, Family rowContents) {
      Family.Builder familyBuilder = familyBuilderMap.get(rowContents.getName());
      if (familyBuilder == null) {
        familyBuilder = Family.newBuilder();
        familyBuilder.setName(rowContents.getName());
        familyBuilderMap.put(familyBuilder.getName(), familyBuilder);
      }
      return familyBuilder;
    }
  }

  /**
   * Helper to read a queue of ResultQueueEntries and use the RowMergers to reconstruct
   * complete Row objects from the partial ReadRowsResponse objects.
   */
  protected static class ResponseQueueReader {
    private final BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue;
    private final int readPartialRowTimeoutMillis;
    private boolean lastResponseProcessed = false;

    public ResponseQueueReader(
        BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue,
        int readPartialRowTimeoutMillis) {
      this.resultQueue = resultQueue;
      this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    }

    /**
     * Get the next complete Row object from the response queue.
     * @return Optional.absent() if end-of-stream, otherwise a complete Row.
     * @throws IOException On errors.
     */
    public Optional<Row> getNextMergedRow() throws IOException {
      ResultQueueEntry<ReadRowsResponse> queueEntry;
      RowMerger builder = null;

      while (!lastResponseProcessed) {
        try {
          queueEntry = resultQueue.poll(readPartialRowTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for next result", e);
        }
        if (queueEntry == null) {
          throw new IOException("Timeout while merging responses.");
        }

        if (queueEntry.isCompletionMarker()) {
          lastResponseProcessed = true;
          break;
        }

        ReadRowsResponse partialRow = queueEntry.getResponseOrThrow();
        if (builder == null) {
          builder = new RowMerger();
        }

        builder.addPartialRow(partialRow);

        if (builder.isRowCommitted()) {
          return Optional.of(builder.buildRow());
        }
      }

      Preconditions.checkState(
          builder == null,
          "End of stream marker encountered while merging a row.");
      Preconditions.checkState(
          lastResponseProcessed,
          "Should only exit merge loop with by returning a complete Row or hitting end of stream.");
      return Optional.absent();
    }
  }

  private final CancellationToken cancellationToken;
  private final BlockingQueue<ResultQueueEntry<ReadRowsResponse>> resultQueue;
  private final ResponseQueueReader responseQueueReader;

  public StreamingBigtableResultScanner(
      int capacity,
      int readPartialRowTimeoutMillis,
      CancellationToken cancellationToken) {
    Preconditions.checkArgument(cancellationToken != null, "cancellationToken cannot be null");
    Preconditions.checkArgument(capacity > 0, "capacity must be a positive integer");
    this.cancellationToken = cancellationToken;
    this.resultQueue = new LinkedBlockingQueue<>(capacity);
    this.responseQueueReader = new ResponseQueueReader(
        resultQueue, readPartialRowTimeoutMillis);
  }

  private void add(ResultQueueEntry<ReadRowsResponse> entry) {
    try {
      resultQueue.put(entry);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while adding a ResultQueueEntry", e);
    }
  }

  public void addResult(ReadRowsResponse response) {
    add(ResultQueueEntry.newResult(response));
  }

  public void setError(Throwable error) {
    add(ResultQueueEntry.<ReadRowsResponse>newThrowable(error));
  }

  public void complete() {
    add(ResultQueueEntry.<ReadRowsResponse>newCompletionMarker());
  }

  @Override
  public Row next() throws IOException {
    Optional<Row> next = responseQueueReader.getNextMergedRow();
    if (next.isPresent()) {
      return next.get();
    } else {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    cancellationToken.cancel();
  }
}
