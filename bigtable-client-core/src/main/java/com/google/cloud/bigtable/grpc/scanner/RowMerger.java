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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

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
 * <p> {@link RowMerger#toRows(Iterable)} will essentially perform the code above.</p>
 */
public class RowMerger implements StreamObserver<ReadRowsResponse> {

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

  private final StreamObserver<Row> observer;

  public RowMerger(StreamObserver<Row> observer) {
    this.observer = observer;
  }

  private final Map<String, Family.Builder> familyMap = new HashMap<>();
  private ByteString currentRowKey;

  /**
   * Add a partial row response to this builder.
   */
  @Override
  public void onNext(ReadRowsResponse partialRow) {
    if (currentRowKey != null && !currentRowKey.equals(partialRow.getRowKey())){
      onError(new IllegalStateException("Interleaved ReadRowResponse messages are not supported."));
    }

    for (Chunk chunk : partialRow.getChunksList()) {
      if (currentRowKey == null) {
        currentRowKey = partialRow.getRowKey();
      }
      switch (chunk.getChunkCase()) {
        case ROW_CONTENTS:
          merge(familyMap, chunk.getRowContents());
          break;
        case RESET_ROW:
          clear();
          break;
        case COMMIT_ROW:
          emitRow();
          clear();
          break;
        default:
          onError(new IllegalStateException(String.format("Unknown ChunkCase encountered %s",
            chunk.getChunkCase())));
      }
    }
  }

  protected void clear() {
    familyMap.clear();
    currentRowKey = null;
  }

  @Override
  public void onError(Throwable t) {
    clear();
    observer.onError(t);
  }

  @Override
  public void onCompleted() {
    if (!familyMap.isEmpty()) {
      onError(
          new IllegalStateException("End of stream marker encountered while merging a row."));
    } else {
      observer.onCompleted();
    }
  }

  /**
   * Construct a row from previously seen partial rows. This method may only be invoked when
   * isRowCommitted returns true indicating a COMMIT_ROW chunk has been encountered without a
   * RESET_ROW. Calls {@link StreamObserver#onNext(Object)} a Row if there was a Chunk with
   * ROW_CONTENTS without a subsequent RESET_ROW. Otherwise, return null
   */
  private void emitRow() {
    if (familyMap.isEmpty()) {
      // A chunk of a row could be partially read, but then deleted by a different operation.
      // It would result in a sequence like: [ROW_CONTENTS, ROW_CONTENTS, RESET_ROW, COMMIT_ROW].
      return;
    }
    Row.Builder currentRowBuilder = Row.newBuilder();
    currentRowBuilder.setKey(currentRowKey);
    for (Family.Builder builder : familyMap.values()) {
      currentRowBuilder.addFamilies(builder.build());
    }
    observer.onNext(currentRowBuilder.build());
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
