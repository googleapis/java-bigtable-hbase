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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.google.api.client.util.Strings;
import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

public class ReadRowTestUtils {

  final static Function<ReadRowsResponse, Row> ROW_KEY_EXTRACTOR_FUNCTION =
      new Function<ReadRowsResponse, Row>() {
        @Override
        public Row apply(ReadRowsResponse readRowsResponse) {
          return Row.newBuilder().setKey(readRowsResponse.getRowKey()).build();
        }
      };

  static Iterable<Row> extractRowsWithKeys(Iterable<ReadRowsResponse> responses) {
    return Iterables.transform(responses, ROW_KEY_EXTRACTOR_FUNCTION);
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

      if (value != null) {
        Cell.Builder cellBuilder = Cell.newBuilder();
        cellBuilder.setTimestampMicros(timestamp);
        cellBuilder.setValue(ByteString.copyFrom(value));
        columnBuilder.addCells(cellBuilder);
      }
      familyBuilder.addColumns(columnBuilder);
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
}
