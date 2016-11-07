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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.stub.StreamObserver;

@RunWith(JUnit4.class)
public class RowMergerTest {

  @Mock
  StreamObserver<FlatRow> observer;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testOneCellRow() {
    CellChunk cellChunk1 = createCell("row_key1", "family", "qualifier", "value", 1, true);
    CellChunk cellChunk2 = createCell("row_key2", "family", "qualifier", "value", 1, true);
    RowMerger underTest = new RowMerger(observer);
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk1).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk2).build());
    verify(observer, times(1)).onNext(eq(toRow(cellChunk1)));
    verify(observer, times(1)).onNext(eq(toRow(cellChunk2)));
  }

  @Test
  public void testThreeCellRow() {
    CellChunk cellChunk1 = createCell("row_key1", "family", "qualifier", "value", 1, false);
    CellChunk cellChunk2 = createCell(null, null, "qualifier2", "value2", 2, false);
    CellChunk cellChunk3 = createCell(null, null, null, "value3", 1, true);
    RowMerger underTest = new RowMerger(observer);
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk1).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk2).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk3).build());

    FlatRow expected =
        FlatRow.newBuilder()
            .withRowKey(cellChunk1.getRowKey())
            .addCell(
                  cellChunk1.getFamilyName().getValue(),
                  cellChunk1.getQualifier().getValue(), 
                  cellChunk1.getTimestampMicros(),
                  cellChunk1.getValue())
            .addCell(
                  cellChunk1.getFamilyName().getValue(),
                  cellChunk2.getQualifier().getValue(), 
                  cellChunk2.getTimestampMicros(),
                  cellChunk2.getValue())
            .addCell(
                cellChunk1.getFamilyName().getValue(),
                cellChunk2.getQualifier().getValue(), 
                cellChunk3.getTimestampMicros(),
                cellChunk3.getValue())
            .build();
    verify(observer, times(1)).onNext(eq(expected));
  }

  @Test
  public void testMultiChunkCell() {
    CellChunk cellChunk1 =
        createCell("row_key1", "family", "qualifier", "v", 1, false)
            .toBuilder()
            .setValueSize("value".length())
            .build();
    CellChunk cellChunk2 = createCell(null, null, null, "alue", 1, true);
    RowMerger underTest = new RowMerger(observer);
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk1).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk2).build());
    
    ByteString value = ByteString.copyFrom("value".getBytes());
    FlatRow row =
        FlatRow.newBuilder()
            .withRowKey(cellChunk1.getRowKey())
            .addCell(
                  cellChunk1.getFamilyName().getValue(),
                  cellChunk1.getQualifier().getValue(),
                  cellChunk1.getTimestampMicros(),
                  value)
            .build();
    verify(observer, times(1)).onNext(eq(row));
  }


  @Test
  public void testSimpleReset() {
    CellChunk cellChunk1 = createCell("row_key1", "family", "qualifier", "value", 1, false);
    CellChunk cellChunk2 = createCell("row_key2", "family", "qualifier", "value", 1, true);
    RowMerger underTest = new RowMerger(observer);
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk1).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(CellChunk.newBuilder().setResetRow(true)).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk2).build());
    verify(observer, times(0)).onNext(eq(toRow(cellChunk1)));
    verify(observer, times(1)).onNext(eq(toRow(cellChunk2)));
  }

  private static CellChunk createCell(String key, String family, String qualifier,
      String value, long timestampMicros, boolean isCommit) {
    CellChunk.Builder cellChunk = CellChunk.newBuilder();
    if (key != null) {
      cellChunk.setRowKey(ByteString.copyFrom(key.getBytes()));
    }
    if (family != null) {
      cellChunk.setFamilyName(StringValue.newBuilder().setValue(family));
    }
    if (qualifier != null) {
      cellChunk.setQualifier(
        BytesValue.newBuilder().setValue(ByteString.copyFrom(qualifier.getBytes())));
    }
    cellChunk.setTimestampMicros(timestampMicros)
        .setValue(ByteString.copyFrom(value.getBytes()));
    if (isCommit) {
      cellChunk.setCommitRow(true);
    }
    return cellChunk.build();
  }

  private static FlatRow toRow(CellChunk... cellChunks) {
    FlatRow.Builder rowBuilder = FlatRow.newBuilder().withRowKey(cellChunks[0].getRowKey());
    for (CellChunk chunk : cellChunks) {
      rowBuilder.addCell(
          chunk.getFamilyName().getValue(),
          chunk.getQualifier().getValue(),
          chunk.getTimestampMicros(),
          chunk.getValue());
    }
    return rowBuilder.build();
  }
}
