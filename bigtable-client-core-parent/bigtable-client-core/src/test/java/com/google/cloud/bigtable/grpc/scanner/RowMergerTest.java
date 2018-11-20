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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;

import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
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
  public void testOneCellRow_success() {
    CellChunk cellChunk1 = createCell("row_key1", "family", "qualifier", "value", 1, true);
    CellChunk cellChunk2 = createCell("row_key2", "family", "qualifier", "value", 1, true);
    RowMerger underTest = new RowMerger(observer);
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk1).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk2).build());
    verify(observer, times(1)).onNext(eq(toRow(cellChunk1)));
    verify(observer, times(1)).onNext(eq(toRow(cellChunk2)));
    verify(observer, times(0)).onError(any(Exception.class));
  }

  @Test
  public void testOneCellRow_reverse_key() {
    CellChunk cellChunk1 = createCell("row_key2", "family", "qualifier", "value", 1, true);
    CellChunk cellChunk2 = createCell("row_key1", "family", "qualifier", "value", 1, true);
    RowMerger underTest = new RowMerger(observer);
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk1).build());
    underTest.onNext(ReadRowsResponse.newBuilder().addChunks(cellChunk2).build());
    verify(observer, times(1)).onError(any(IllegalArgumentException.class));
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


  @Test
  public void testVariousConditions() {
    RowMerger underTest = new RowMerger(observer);

    String rowKey = "row_key1";
    String family1 = "family1";
    String family2 = "family2";
    String qualifier1 = "qualifier1";
    String qualifier2 = "qualifier2";
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";
    String value4 = "value4";
    String value5 = "value5";

    ByteString rowKeyByteString = toByteString(rowKey);
    ByteString qualifier1ByteString = toByteString(qualifier1);
    ByteString qualifier2ByteString = toByteString(qualifier2);
    ByteString value1ByteString = toByteString(value1);
    ByteString value2ByteString = toByteString(value2);
    ByteString value3ByteString = toByteString(value3);
    ByteString value4ByteString = toByteString(value4);
    ByteString value5ByteString = toByteString(value5);

    final long ts1 = 54321L;
    final long ts2 = 12345L;

    final String label = "label";

    // TODO: For now, all values in the response should be translated. There should be some deduping soon.
    underTest.onNext(ReadRowsResponse.newBuilder()
        .addAllChunks(Arrays.asList(
          // First cell.
          createCell(rowKey, family1, qualifier1, value1, ts1, false),
          // Duplicate cell.
          createCell(rowKey, family1, qualifier1, value1, ts1, false),
          // Same family, same column, but different timestamps.
          createCell(rowKey, family1, qualifier1, value2, ts2, false),
          // With label
          createCell(rowKey, family1, qualifier1, value2, ts2, false, label),
          // Same family, same timestamp, but different column.
          createCell(rowKey, family1, qualifier2, value3, ts1, false),
          // Same column, same timestamp, but different family.
          createCell(rowKey, family2, qualifier1, value4, ts1, false),
          // Same timestamp, but different family qualifier2 column.
          createCell(rowKey, family2, qualifier2, value5, ts1, true)))
        .build());

    verify(observer, times(1)).onNext(eq(FlatRow.newBuilder().withRowKey(rowKeyByteString)
        .addCell(family1, qualifier1ByteString, ts1, value1ByteString)
        .addCell(family1, qualifier1ByteString, ts2, value2ByteString)
        .addCell(family1, qualifier1ByteString, ts2, value2ByteString, Arrays.asList(label))
        .addCell(family1, qualifier2ByteString, ts1, value3ByteString)
        .addCell(family2, qualifier1ByteString, ts1, value4ByteString)
        .addCell(family2, qualifier2ByteString, ts1, value5ByteString).build()));
  }

  protected static ByteString toByteString(String str) {
    return ByteString.copyFrom(str.getBytes());
  }

  private static CellChunk createCell(String key, String family, String qualifier,
      String value, long timestampMicros, boolean isCommit, String ... labels) {
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
    cellChunk.addAllLabels(Arrays.asList(labels));
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
