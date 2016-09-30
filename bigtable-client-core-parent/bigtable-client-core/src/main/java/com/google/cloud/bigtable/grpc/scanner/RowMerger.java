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

import com.google.common.base.MoreObjects;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk.RowStatusCase;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.util.ZeroCopyByteStringUtil;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

/**
 * <p>
 * Builds a complete {@link com.google.bigtable.v2.Row} from {@link com.google.bigtable.v2.ReadRowsResponse} objects. A {@link com.google.bigtable.v2.ReadRowsResponse}
 * may contain a single {@link com.google.bigtable.v2.Row}, multiple {@link com.google.bigtable.v2.Row}s, or even a part of a {@link com.google.bigtable.v2.Cell} if the
 * cell is
 * </p>
 * <p>
 * Each RowMerger object is valid only for building a single Row. Expected usage is along the lines
 * of:
 * </p>
 *
 * <pre>
 * {@link io.grpc.stub.StreamObserver}&lt;{@link com.google.bigtable.v2.Row}&gt; observer = ...;
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
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RowMerger implements StreamObserver<ReadRowsResponse> {

  /**
   * <p>toRows.</p>
   *
   * @param responses a {@link java.lang.Iterable} object.
   * @return a {@link java.util.List} object.
   */
  public static List<Row> toRows(Iterable<ReadRowsResponse> responses) {
    final ArrayList<Row> result = new ArrayList<>();
    RowMerger rowMerger = new RowMerger(new StreamObserver<Row>() {
      @Override
      public void onNext(Row value) {
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
     * A new {@link CellChunk} represents a completely new {@link Row}.
     */
    NewRow {
      @Override
      void handleLastScannedRowKey(ByteString lastScannedRowKey) {
        throw new IllegalStateException("Encountered a lastScannedRowKey while processing a row.");
      }

      @Override
      void validateChunk(RowInProgress rowInProgess, ByteString previousKey, CellChunk newChunk) {
        Preconditions.checkArgument(rowInProgess == null,
          "A new row cannot have existing state: %s", newChunk);
        Preconditions.checkArgument(newChunk.getRowStatusCase() != RowStatusCase.RESET_ROW,
          "A new row cannot be reset: %s", newChunk);
        Preconditions.checkArgument(newChunk.hasFamilyName(), "A family must be set: %s", newChunk);
        final ByteString rowKey = newChunk.getRowKey();
        Preconditions.checkArgument(!rowKey.isEmpty(), "A row key must be set: %s",
          newChunk);
        Preconditions.checkState(previousKey == null || !rowKey.equals(previousKey),
          "A commit happened but the same key followed: %s", newChunk);

        Preconditions.checkArgument(newChunk.hasQualifier(), "A column qualifier must be set: %s",
          newChunk);
        if (newChunk.getValueSize() > 0) {
          Preconditions.checkArgument(!isCommit(newChunk),
            "A row cannot be have a value size and be a commit row: %s", newChunk);
        }
      }

      @Override
      void handleOnComplete(StreamObserver<Row> observer) {
        observer.onCompleted();
      }
    },

    /**
     * A new {@link CellChunk} represents a new {@link Cell} in a {@link Row}.
     */
    RowInProgress {
      @Override
      void handleLastScannedRowKey(ByteString lastScannedRowKey) {
        throw new IllegalStateException("Encountered a lastScannedRowKey while processing a row.");
      }

      @Override
      void validateChunk(RowInProgress rowInProgess, ByteString previousKey, CellChunk newChunk) {
        if (newChunk.hasFamilyName()) {
          Preconditions.checkArgument(newChunk.hasQualifier(), "A qualifier must be specified: %s",
            newChunk);
        }
        ByteString newRowKey = newChunk.getRowKey();
        if (isReset(newChunk)) {
          Preconditions.checkState(
            newRowKey.isEmpty() && !newChunk.hasFamilyName() && !newChunk.hasQualifier()
                && newChunk.getValue().isEmpty() && newChunk.getTimestampMicros() == 0,
            "A reset should have no data");
        } else {
          Preconditions.checkState(
            newRowKey.isEmpty() || newRowKey.equals(rowInProgess.getRowKey()),
            "A commit is required between row keys: %s", newChunk);
          rowInProgess.updateCurrentKey(newChunk);
          Preconditions.checkArgument(newChunk.getValueSize() == 0 || !isCommit(newChunk),
            "A row cannot be have a value size and be a commit row: %s", newChunk);
        }
      }

      @Override
      void handleOnComplete(StreamObserver<Row> observer) {
        observer.onError(new IllegalStateException("Got a partial row, but the stream ended"));
      }
    },

    /**
     * A new {@link CellChunk} represents a portion of the value in a {@link Cell} in a {@link Row}.
     */
    CellInProgress {
      @Override
      void handleLastScannedRowKey(ByteString lastScannedRowKey) {
        throw new IllegalStateException("Encountered a lastScannedRowKey while processing a cell.");
      }

      @Override
      void validateChunk(RowInProgress rowInProgess, ByteString previousKey, CellChunk newChunk) {
        if(isReset(newChunk)) {
          Preconditions.checkState(newChunk.getRowKey().isEmpty() &&
            !newChunk.hasFamilyName() &&
            !newChunk.hasQualifier() &&
            newChunk.getValue().isEmpty() &&
            newChunk.getTimestampMicros() == 0,
              "A reset should have no data");
        } else {
          Preconditions.checkArgument(newChunk.getValueSize() == 0 || !isCommit(newChunk),
            "A row cannot be have a value size and be a commit row: %s", newChunk);
        }
      }

      @Override
      void handleOnComplete(StreamObserver<Row> observer) {
        observer.onError(new IllegalStateException("Got a partial row, but the stream ended"));
      }
    };

    abstract void handleLastScannedRowKey(ByteString lastScannedRowKey);

    abstract void validateChunk(RowInProgress rowInProgess, ByteString previousKey,
        CellChunk newChunk) throws Exception;

    abstract void handleOnComplete(StreamObserver<Row> observer);
  }

  private static class FamilyBuilderManager {
    private final Map<CellKey, Column.Builder> columnBuilders = new TreeMap<>();

    public void addCell(String family, ByteString qualifier, Cell cell) {
      CellKey key = new CellKey(family, qualifier);
      Column.Builder columnBuilder = columnBuilders.get(key);
      if (columnBuilder == null) {
        columnBuilder = Column.newBuilder().setQualifier(qualifier);
        columnBuilders.put(key, columnBuilder);
      }
      columnBuilder.addCells(cell);
    }

    public Row.Builder addFamiliesTo(Row.Builder rowBuilder) {
      CellKey previousKey = null;
      Family.Builder currentFamilyBuilder = null;
      for (Entry<CellKey, Column.Builder> entry : columnBuilders.entrySet()) {
        CellKey currentKey = entry.getKey();
        if (previousKey == null || !previousKey.family.equals(currentKey.family)) {
          currentFamilyBuilder = rowBuilder.addFamiliesBuilder().setName(currentKey.family);
        }
        currentFamilyBuilder.addColumns(entry.getValue());
        previousKey = currentKey;
      }
      return rowBuilder;
    }
  }

  private static class CellKey implements Comparable<CellKey> {
    final String family;
    final ByteString qualifier;

    CellKey(String family, ByteString qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }

    @Override
    public int compareTo(CellKey o) {
      int comp = family.compareTo(o.family);
      if (comp != 0) {
        return comp;
      }
      return qualifier.asReadOnlyByteBuffer().compareTo(o.qualifier.asReadOnlyByteBuffer());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("family", family).add("qualifier", qualifier).toString();
    }
  }

  /**
   * A CellIdentifier represents the matadata for a Cell. The information in this class can be
   * collected from a variety of {@link CellChunk}, for example the rowKey will be expressed only
   * in the first {@link CellChunk}, and family will be present only when a family changes.
   */
  private static class CellIdentifier {
    final ByteString rowKey;
    final String family;
    final ByteString qualifier;
    final long timestampMicros;
    final List<String> labels;

    CellIdentifier(CellChunk chunk) {
      this(chunk.getRowKey(), chunk);
    }

    CellIdentifier(ByteString rowKey, CellChunk chunk) {
      this(rowKey, chunk.getFamilyName().getValue(), chunk);
    }

    CellIdentifier(ByteString rowKey, String family, CellChunk chunk) {
      this(rowKey, family, chunk.getQualifier().getValue(), chunk);
    }

    CellIdentifier(ByteString rowKey, String family, ByteString qualifier, CellChunk chunk) {
      this(rowKey, family, qualifier,
          chunk.getTimestampMicros(), chunk.getLabelsList());
    }

    CellIdentifier(ByteString rowKey, String family, ByteString qualifier, long timestampMicros,
        List<String> labels) {
      this.rowKey = rowKey;
      this.family = family;
      this.qualifier = qualifier;
      this.timestampMicros = timestampMicros;
      this.labels = labels;
    }

    CellIdentifier nextKeyForFamily(CellChunk chunk) {
      return new CellIdentifier(rowKey, chunk);
    }

    CellIdentifier nextKeyForQualifier(CellChunk chunk) {
      return new CellIdentifier(rowKey, family, chunk);
    }

    CellIdentifier nextKeyForTimestamp(CellChunk chunk) {
      return new CellIdentifier(rowKey, family, qualifier, chunk);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || !(obj instanceof CellIdentifier)) {
        return false;
      }
      CellIdentifier other = (CellIdentifier) obj;
      return Objects.equals(rowKey, other.rowKey)
          && Objects.equals(family, other.family)
          && Objects.equals(qualifier, other.qualifier)
          && timestampMicros == other.timestampMicros
          && Objects.equals(labels, other.labels);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rowKey, family, qualifier, timestampMicros, labels);
    }
  }

  /**
   * This 
   */
  private static final class RowInProgress {
    private final FamilyBuilderManager families = new FamilyBuilderManager();

    // cell in progress info
    private CellIdentifier currentId;
    private Cell.Builder cellBuilderInProgress;
    private ByteArrayOutputStream outputStream;

    void addFullChunk(ReadRowsResponse.CellChunk chunk) {
      Preconditions.checkState(!hasChunkInProgess());
      addCell(
          Cell.newBuilder()
              .setTimestampMicros(chunk.getTimestampMicros())
              .addAllLabels(chunk.getLabelsList())
              .setValue(chunk.getValue())
              .build());
    }

    public void completeMultiChunkCell() {
      Preconditions.checkArgument(hasChunkInProgess());
      // TODO(rameshdharan): Can we eliminate the .toByteArray() copying?
      ByteString value = ZeroCopyByteStringUtil.wrap(outputStream.toByteArray());
      addCell(cellBuilderInProgress.setValue(value).build());
      outputStream = null;
      cellBuilderInProgress = null;
    }

    private void addCell(Cell cell) {
      families.addCell(currentId.family, currentId.qualifier, cell);
    }

    /**
     * update the current key with the new chunk info
     */
    void updateCurrentKey(ReadRowsResponse.CellChunk chunk) {
      if (currentId == null || isNewRowKey(chunk)) {
        currentId = new CellIdentifier(chunk);
      } else if (chunk.hasFamilyName()) {
        currentId = currentId.nextKeyForFamily(chunk);
      } else if (chunk.hasQualifier()) {
        currentId = currentId.nextKeyForQualifier(chunk);
      } else {
        currentId = currentId.nextKeyForTimestamp(chunk);
      }
    }

    private boolean isNewRowKey(ReadRowsResponse.CellChunk chunk) {
      ByteString rowKey = chunk.getRowKey();
      return !rowKey.isEmpty() && !rowKey.equals(currentId.rowKey);
    }

    public boolean hasChunkInProgess() {
      return outputStream != null;
    }

    void addPartialCellChunk(ReadRowsResponse.CellChunk chunk) throws IOException {
      if (outputStream == null) {
        outputStream = new ByteArrayOutputStream(chunk.getValueSize());
        cellBuilderInProgress = Cell.newBuilder()
            .setTimestampMicros(chunk.getTimestampMicros())
            .addAllLabels(chunk.getLabelsList());
      }
      chunk.getValue().writeTo(outputStream);
    }

    public Row createRow() {
      Row.Builder rowBuilder = Row.newBuilder().setKey(getRowKey());
      families.addFamiliesTo(rowBuilder);
      return rowBuilder.build();
    }

    public ByteString getRowKey() {
      return currentId.rowKey;
    }
  }

  private static boolean isCommit(CellChunk chunk) {
    return chunk.getRowStatusCase() == RowStatusCase.COMMIT_ROW && chunk.getCommitRow();
  }

  private static boolean isReset(CellChunk chunk) {
    return chunk.getRowStatusCase() == RowStatusCase.RESET_ROW && chunk.getResetRow();
  }

  private final StreamObserver<Row> observer;

  private RowMergerState state = RowMergerState.NewRow;
  private ByteString previousKey;
  private RowInProgress rowInProgress;
  private boolean complete;

  /**
   * <p>Constructor for RowMerger.</p>
   *
   * @param observer a {@link io.grpc.stub.StreamObserver} object.
   */
  public RowMerger(StreamObserver<Row> observer) {
    this.observer = observer;
  }

  /** {@inheritDoc} */
  @Override
  public void onNext(ReadRowsResponse readRowsResponse) {
    if (complete) {
      onError(new IllegalStateException("Adding partialRow after completion"));
      return;
    }
    ByteString lastScannedRowKey = readRowsResponse.getLastScannedRowKey();
    if (!lastScannedRowKey.isEmpty()) {
      state.handleLastScannedRowKey(lastScannedRowKey);
    }
    for (ReadRowsResponse.CellChunk chunk : readRowsResponse.getChunksList()) {
      try {
        state.validateChunk(rowInProgress, previousKey, chunk);
      } catch(Exception e) {
        onError(e);
        return;
      }
      try {
        if (isReset(chunk)) {
          rowInProgress = null;
          state = RowMergerState.NewRow;
          continue;
        }
        if (rowInProgress == null) {
          rowInProgress = new RowInProgress();
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

        if (isCommit(chunk)) {
          observer.onNext(rowInProgress.createRow());
          previousKey = rowInProgress.getRowKey();
          rowInProgress = null;
          state = RowMergerState.NewRow;
        }
      } catch(IOException e) {
        onError(e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onError(Throwable e) {
    observer.onError(e);
    complete = true;
  }

  /**
   * {@inheritDoc}
   *
   * All {@link ReadRowsResponse} have been processed, and HTTP OK was sent.
   */
  @Override
  public void onCompleted() {
    state.handleOnComplete(observer);
  }
}
