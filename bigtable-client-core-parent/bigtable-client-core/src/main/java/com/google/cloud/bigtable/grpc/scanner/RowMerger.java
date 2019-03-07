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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.util.ByteStringComparator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk.RowStatusCase;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

/**
 * <p>
 * Builds a complete {@link FlatRow} from {@link com.google.bigtable.v2.ReadRowsResponse} objects. A
 * {@link com.google.bigtable.v2.ReadRowsResponse} may contain a single {@link FlatRow}, multiple
 * {@link FlatRow}s, or even a part of a {@link com.google.bigtable.v2.Cell} if the cell is
 * </p>
 * <p>
 * Each RowMerger object is valid only for building a single FlatRow. Expected usage is along the
 * lines of:
 * </p>
 *
 * <pre>
 * {@link io.grpc.stub.StreamObserver}&lt;{@link FlatRow}&gt; observer = ...;
 * RowMerger rowMerger = new RowMerger(observer);
 * ...
 * rowMerger.onNext(...);
 * ..
 * rowMerger.onComplete();
 * </pre>
 * <p>
 * When a complete row is found, {@link io.grpc.stub.StreamObserver#onNext(Object)} will be called.
 * {@link io.grpc.stub.StreamObserver#onError(Throwable)} will be called for
 * </p>
 * <p>
 * <b>NOTE: RowMerger is not threadsafe.</b>
 * </p>
 * @author sduskis
 * @version $Id: $Id
 */
public class RowMerger implements StreamObserver<ReadRowsResponse> {

protected static final Logger LOG = new Logger(RowMerger.class);

  /**
   * <p>toRows.</p>
   *
   * @param responses a {@link java.lang.Iterable} object.
   * @return a {@link java.util.List} object.
   */
  public static List<FlatRow> toRows(Iterable<ReadRowsResponse> responses) {
    final ArrayList<FlatRow> result = new ArrayList<>();
    RowMerger rowMerger = new RowMerger(new StreamObserver<FlatRow>() {
      @Override
      public void onNext(FlatRow value) {
        result.add(value);
      }

      @Override
      public void onError(Throwable t) {
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else {
          throw new IllegalStateException(t);
        }
      }

      @Override
      public void onCompleted() {
      }
    });
    for (ReadRowsResponse response : responses) {
      rowMerger.onNext(response);
    }
    rowMerger.onCompleted();
    return result;
  }

  /**
   * Encapsulates validation for different states based on the stream of the {@link CellChunk}.
   */
  private enum RowMergerState {

    /**
     * A new {@link CellChunk} represents a completely new {@link FlatRow}.
     */
    NewRow {
      @Override
      void validateChunk(RowInProgress rowInProgess, ByteString previousKey, CellChunk newChunk) {
        Preconditions.checkArgument(rowInProgess == null || !rowInProgess.hasRowKey(),
          "A new row cannot have existing state: %s", newChunk);
        Preconditions.checkArgument(newChunk.getRowStatusCase() != RowStatusCase.RESET_ROW,
          "A new row cannot be reset: %s", newChunk);
        Preconditions.checkArgument(newChunk.hasFamilyName(), "A family must be set: %s", newChunk);
        final ByteString rowKey = newChunk.getRowKey();
        Preconditions.checkArgument(!rowKey.isEmpty(), "A row key must be set: %s",
          newChunk);
        if (previousKey != null &&
            ByteStringComparator.INSTANCE.compare(previousKey, rowKey) >= 0) {
          throw new IllegalArgumentException(String
              .format("Found key '%s' after key '%s'", rowKey.toStringUtf8(),
                  previousKey.toStringUtf8()));
        }
        Preconditions.checkArgument(newChunk.hasQualifier(), "A column qualifier must be set: %s",
          newChunk);
        Preconditions.checkArgument(!newChunk.getCommitRow() || newChunk.getValueSize() == 0,
          "A row cannot be have a value size and be a commit row: %s", newChunk);
      }

      @Override
      void handleOnComplete(StreamObserver<FlatRow> observer) {
        observer.onCompleted();
      }
    },

    /**
     * A new {@link CellChunk} represents a new {@link FlatRow.Cell} in a {@link FlatRow}.
     */
    RowInProgress {
      @Override
      void validateChunk(RowInProgress rowInProgess, ByteString previousKey, CellChunk newChunk) {
        if (newChunk.hasFamilyName()) {
          Preconditions.checkArgument(newChunk.hasQualifier(), "A qualifier must be specified: %s",
            newChunk);
        }
        ByteString newRowKey = newChunk.getRowKey();
        if (newChunk.getResetRow()) {
          Preconditions.checkState(
            newRowKey.isEmpty() && !newChunk.hasFamilyName() && !newChunk.hasQualifier()
                && newChunk.getValue().isEmpty() && newChunk.getTimestampMicros() == 0,
            "A reset should have no data");
        } else {
          Preconditions.checkState(
            newRowKey.isEmpty() || newRowKey.equals(rowInProgess.getRowKey()),
            "A commit is required between row keys: %s", newChunk);
          Preconditions.checkArgument(newChunk.getValueSize() == 0 || !newChunk.getCommitRow(),
            "A row cannot be have a value size and be a commit row: %s", newChunk);
        }
      }

      @Override
      void handleOnComplete(StreamObserver<FlatRow> observer) {
        observer.onError(new IllegalStateException("Got a partial row, but the stream ended"));
      }
    },

    /**
     * A new {@link CellChunk} represents a portion of the value in a {@link FlatRow.Cell} in a
     * {@link FlatRow}.
     */
    CellInProgress {
      @Override
      void validateChunk(RowInProgress rowInProgess, ByteString previousKey, CellChunk newChunk) {
        if(newChunk.getResetRow()) {
          Preconditions.checkState(newChunk.getRowKey().isEmpty() &&
            !newChunk.hasFamilyName() &&
            !newChunk.hasQualifier() &&
            newChunk.getValue().isEmpty() &&
            newChunk.getTimestampMicros() == 0,
              "A reset should have no data");
        } else {
          Preconditions.checkArgument(newChunk.getValueSize() == 0 || !newChunk.getCommitRow(),
            "A row cannot be have a value size and be a commit row: %s", newChunk);
        }
      }

      @Override
      void handleOnComplete(StreamObserver<FlatRow> observer) {
        observer.onError(new IllegalStateException("Got a partial row, but the stream ended"));
      }
    };

    abstract void validateChunk(RowInProgress rowInProgess, ByteString previousKey,
        CellChunk newChunk) throws Exception;

    abstract void handleOnComplete(StreamObserver<FlatRow> observer);
  }

  /**
   * A CellIdentifier represents the matadata for a Cell. The information in this class can be
   * collected from a variety of {@link CellChunk}, for example the rowKey will be expressed only
   * in the first {@link CellChunk}, and family will be present only when a family changes.
   */
  private static class CellIdentifier {
    String family;
    ByteString qualifier;
    long timestampMicros;
    List<String> labels;

    private CellIdentifier(CellChunk chunk) {
      updateForFamily(chunk);
    }

    private void updateForFamily(CellChunk chunk) {
      String chunkFamily = chunk.getFamilyName().getValue();
      if (!chunkFamily.equals(family)) {
        // Try to get a reference to the same object if there's equality.
        this.family = chunkFamily;
      }
      updateForQualifier(chunk);
    }

    private void updateForQualifier(CellChunk chunk) {
      this.qualifier = chunk.getQualifier().getValue();
      updateForTimestamp(chunk);
    }

    private void updateForTimestamp(CellChunk chunk) {
      this.timestampMicros = chunk.getTimestampMicros();
      this.labels = chunk.getLabelsList();
    }
  }

  /**
   * This class represents the data in the row that's currently being processed.
   */
  private static final class RowInProgress {

    // 50MB is pretty large for a row, so log any rows that are of that size
    private final static int LARGE_ROW_SIZE = 50 * 1024 * 1024;

    private ByteString rowKey;

    // cell in progress info
    private CellIdentifier currentId;
    private ByteString.Output outputStream;
    private int currentByteSize = 0;
    private int loggedAtSize = 0;

    private final Map<String, List<FlatRow.Cell>> cells = new TreeMap<>();
    private int cellCount = 0;
    private List<FlatRow.Cell> currentFamilyRowCells = null;
    private String currentFamily;
    private FlatRow.Cell previousNoLabelCell;

    private final void addFullChunk(ReadRowsResponse.CellChunk chunk) {
      Preconditions.checkState(!hasChunkInProgess());
      currentByteSize += chunk.getSerializedSize();
      addCell(chunk.getValue());
      if (currentByteSize >= loggedAtSize + LARGE_ROW_SIZE) {
        LOG.warn("Large row read is in progress. key: `%s`, size: %d, cells: %d",
            rowKey.toStringUtf8(), currentByteSize, cellCount);
        loggedAtSize = currentByteSize;
      }
    }

    private final void completeMultiChunkCell() {
      Preconditions.checkArgument(hasChunkInProgess());
      addCell(outputStream.toByteString());
      outputStream = null;
    }

    /**
     * Adds a Cell to {@link #cells} map which is ordered by family. Cloud Bigtable returns values
     * sorted by family (by internal id, not lexicographically), a lexicographically ascending
     * ordering of qualifiers, and finally by timestamp descending. Each cell can appear more than
     * once, if there are {@link Interleave}s in the {@link ReadRowsRequest#getFilter()}, but the
     * duplicates will appear one after the other.
     * <p>
     * The end result will be that {@link #cells} will be lexicographically ordered by family, and
     * the list of cells will be ordered by qualifier and timestamp. A flattened version of the
     * {@link #cells} map will be sorted correctly.
     */
    private void addCell(ByteString value) {
      if (!Objects.equal(currentFamily, currentId.family)) {
        currentFamilyRowCells = new ArrayList<>();
        currentFamily = currentId.family;
        cells.put(currentId.family, currentFamilyRowCells);
        previousNoLabelCell = null;
      }

      FlatRow.Cell cell = new FlatRow.Cell(currentId.family, currentId.qualifier,
        currentId.timestampMicros, value, currentId.labels);
      if (!currentId.labels.isEmpty()) {
        currentFamilyRowCells.add(cell);
      } else if (!isSameTimestampAndQualifier()) {
        currentFamilyRowCells.add(cell);
        previousNoLabelCell = cell;
      } // else, this is a duplicate cell.

      cellCount++;
    }

    /**
     * Checks to see if the current {@link FlatRow.Cell}'s qualifier and timestamp are equal to the
     * previous {@link #previousNoLabelCell}'s. This method assumes that the family is the same
     * and the {@link #previousNoLabelCell} is not null.
     * @return true if the new cell and old cell have logical equivalency.
     */
    private boolean isSameTimestampAndQualifier() {
      return previousNoLabelCell != null
          && currentId.timestampMicros == previousNoLabelCell.getTimestamp()
          && Objects.equal(previousNoLabelCell.getQualifier(), currentId.qualifier);
    }

    /**
     * update the current key with the new chunk info
     */
    private final void updateCurrentKey(ReadRowsResponse.CellChunk chunk) {
      ByteString newRowKey = chunk.getRowKey();
      if (rowKey == null || (!newRowKey.isEmpty() && !newRowKey.equals(rowKey))) {
        rowKey = newRowKey;
        currentId = new CellIdentifier(chunk);
        currentFamily = null;
        cells.clear();
        currentFamilyRowCells = null;
      } else if (chunk.hasFamilyName()) {
        currentId.updateForFamily(chunk);
      } else if (chunk.hasQualifier()) {
        currentId.updateForQualifier(chunk);
      } else {
        currentId.updateForTimestamp(chunk);
      }
    }

    private boolean hasChunkInProgess() {
      return outputStream != null;
    }

    private void addPartialCellChunk(ReadRowsResponse.CellChunk chunk) throws IOException {
      if (outputStream == null) {
        outputStream = ByteString.newOutput();
      }
      chunk.getValue().writeTo(outputStream);
    }

    private ByteString getRowKey() {
      return rowKey;
    }

    private boolean hasRowKey() {
      return rowKey != null;
    }

    private FlatRow buildRow() {
      if (currentByteSize >= LARGE_ROW_SIZE) {
        LOG.warn("Large row was read. key: `%s`, size: %d, cellCount: %d",
            rowKey.toStringUtf8(), currentByteSize, cellCount);
      }

      return new FlatRow(rowKey, flattenCells());
    }

    /**
     * This method flattens the {@link #cells} which has a map of Lists keyed by family name.
     * The {@link #cells} TreeMap is sorted lexicographically, and each List is sorted by
     * qualifier in lexicographically ascending order, and timestamp in descending order.
     *
     * @return an array of HBase {@link FlatRow.Cell}s that is sorted by family asc, qualifier asc, timestamp desc.
     */
    private ImmutableList<FlatRow.Cell> flattenCells() {
      ImmutableList.Builder<FlatRow.Cell> combined = ImmutableList.builder();
      for (List<FlatRow.Cell> familyCellList : cells.values()) {
        combined.addAll(familyCellList);
      }
      return combined.build();
    }
  }

  private final StreamObserver<FlatRow> observer;

  private RowMergerState state = RowMergerState.NewRow;
  private ByteString lastCompletedRowKey = null;
  private RowInProgress rowInProgress = null;
  private boolean complete = false;
  private Integer rowCountInLastMessage = null;

  /**
   * <p>Constructor for RowMerger.</p>
   *
   * @param observer a {@link io.grpc.stub.StreamObserver} object.
   */
  public RowMerger(StreamObserver<FlatRow> observer) {
    this.observer = observer;
  }

  public void clearRowInProgress() {
    Preconditions.checkState(!complete, "Cannot reset Rowmerger after completion");
    state = RowMergerState.NewRow;
    rowInProgress = null;
    rowCountInLastMessage = null;
  }

  /** {@inheritDoc} */
  @Override
  public final void onNext(ReadRowsResponse readRowsResponse) {
    if (complete) {
      onError(new IllegalStateException("Adding partialRow after completion"));
      return;
    }
    int rowsProcessed = 0;
    for (int i = 0; i < readRowsResponse.getChunksCount(); i++) {
      try {
        CellChunk chunk = readRowsResponse.getChunks(i);
        state.validateChunk(rowInProgress, lastCompletedRowKey, chunk);
        if (chunk.getResetRow()) {
          rowInProgress = null;
          state = RowMergerState.NewRow;
          continue;
        }
        if(state == RowMergerState.NewRow) {
          rowInProgress = new RowInProgress();
          rowInProgress.updateCurrentKey(chunk);
        } else if (state == RowMergerState.RowInProgress) {
          rowInProgress.updateCurrentKey(chunk);
        }
        if (chunk.getValueSize() > 0) {
          rowInProgress.addPartialCellChunk(chunk);
          state = RowMergerState.CellInProgress;
        } else if (rowInProgress.hasChunkInProgess()) {
          rowInProgress.addPartialCellChunk(chunk);
          rowInProgress.completeMultiChunkCell();
          state = RowMergerState.RowInProgress;
        } else {
          rowInProgress.addFullChunk(chunk);
          state = RowMergerState.RowInProgress;
        }

        if (chunk.getCommitRow()) {
          observer.onNext(rowInProgress.buildRow());
          lastCompletedRowKey = rowInProgress.getRowKey();
          state = RowMergerState.NewRow;
          rowInProgress = null;
          rowsProcessed++;
        }
      } catch (Throwable e) {
        onError(e);
        return;
      }
    }
    this.rowCountInLastMessage = rowsProcessed;
  }

  /**
   * @return the number of rows processed in the previous call to {@link #onNext(ReadRowsResponse)}.
   */
  public Integer getRowCountInLastMessage() {
    return rowCountInLastMessage;
  }

  public ByteString getLastCompletedRowKey() {
    return lastCompletedRowKey;
  }

  @VisibleForTesting
  boolean isInNewState() {
    return state == RowMergerState.NewRow && rowInProgress == null;
  }

  /**
   * {@inheritDoc}
   *
   * All {@link ReadRowsResponse} have been processed, and HTTP OK was sent.
   */
  @Override
  public void onCompleted() {
    complete = true;
    state.handleOnComplete(observer);
  }

  /** {@inheritDoc} */
  @Override
  public void onError(Throwable e) {
    complete = true;
    observer.onError(e);
  }

  @VisibleForTesting
  boolean isComplete() {
    return complete;
  }
}
