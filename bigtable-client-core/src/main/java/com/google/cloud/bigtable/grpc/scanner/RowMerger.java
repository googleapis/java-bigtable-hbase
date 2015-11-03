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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * <p>Builds a complete Row from partial ReadRowsResponse objects. This class
 * does not currently handle multiple interleaved rows. It is assumed that it is
 * handling results for a request with allow_row_interleaving = false.
 * </p>
 * <p>Each RowMerger object is valid only for building a single Row. Expected usage
 * is along the lines of:
 * </p>
 * <pre>
 * RowMerger rm = new RowMerger();
 * while (!rm.isRowCommited()) {
 *   rm.addPartialRow(...);
 * }
 * Row r = rm.buildRow();
 * </pre>
 *
 * <p> {@link RowMerger#readNextRow(Iterator)} will essentially perform the code above.</p>
 */
public class RowMerger {

  /**
   * Reads the next {@link Row} from the responseIterator.
   */
  public static Row readNextRow(Iterator<ReadRowsResponse> responseIterator) {
    while (true) {
      Preconditions.checkState(responseIterator.hasNext(),
        "End of stream marker encountered while merging a row.");

      RowMerger rowMerger = new RowMerger();
      while (responseIterator.hasNext() && !rowMerger.isRowCommitted()) {
        rowMerger.addPartialRow(responseIterator.next());
      }

      Preconditions.checkState(rowMerger.isRowCommitted(),
        "End of stream marker encountered while merging a row.");

      Row builtRow = rowMerger.buildRow();
      // builtRow could be null if the row was deleted after the scan started.
      if (builtRow != null) {
        return builtRow;
      } else if (!responseIterator.hasNext()) {
        return null;
      }
    }
  }

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
      switch (chunk.getChunkCase()) {
        case ROW_CONTENTS:
          merge(familyMap, chunk.getRowContents());
          break;
        case RESET_ROW:
          familyMap.clear();
          break;
        case COMMIT_ROW:
          committed = true;
          break;
        default:
          throw new IllegalStateException(String.format("Unknown ChunkCase encountered %s",
            chunk.getChunkCase()));
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
   * Construct a row from previously seen partial rows. This method may only be invoked when
   * isRowCommitted returns true indicating a COMMIT_ROW chunk has been encountered without a
   * RESET_ROW.
   * @return a Row if there was a Chunk with ROW_CONTENTS without a subsequent RESET_ROW. Otherwise,
   *         return null
   * @throws IllegalStateException if the last Chunk was not a COMMIT_ROW.
   */
  public @Nullable Row buildRow() {
    Preconditions.checkState(committed,
        "Cannot build a Row object if we have not yet encountered a COMMIT_ROW chunk.");
    if (familyMap.isEmpty()) {
      // A chunk of a row could be partially read, but then deleted by a different operation.
      // It would result in a sequence like: [ROW_CONTENTS, ROW_CONTENTS, RESET_ROW, COMMIT_ROW].
      // Return null in that case.
      return null;
    }
    Row.Builder currentRowBuilder = Row.newBuilder();
    currentRowBuilder.setKey(currentRowKey);
    for (Family.Builder builder : familyMap.values()) {
      currentRowBuilder.addFamilies(builder.build());
    }
    return currentRowBuilder.build();
  }

  // Merge newRowContents into the map of family builders, creating one if necessary.
  private void merge(Map<String, Family.Builder> familyBuilderMap, Family newRowContents) {
    String familyName = newRowContents.getName();
    Family.Builder familyBuilder = familyBuilderMap.get(familyName);
    if (familyBuilder == null) {
      familyBuilder = Family.newBuilder().setName(familyName);
      familyBuilderMap.put(familyName, familyBuilder);
    }
    familyBuilder.addAllColumns(newRowContents.getColumnsList());
  }
}
