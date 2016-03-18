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
package com.google.cloud.bigtable.hbase;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.RowAdapter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;

import org.apache.commons.lang.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple microbenchmark for {@link RowMerger}
 */
public class RowMergerPerf {

  public static void main(String[] args) {

    for (int i = 0; i < 10; i++) {
      List<ReadRowsResponse> responses = createResponses();
      rowMergerPerf(responses);
    }
  }

  private static List<ReadRowsResponse> createResponses() {
    List<ReadRowsResponse> responses = new ArrayList<>(1);
    for (int i = 0; i < 1; i++) {
      String rowKey = String.format("rowKey-%s", i);
      byte[] value = RandomStringUtils.randomAlphanumeric(10000).getBytes();
      Preconditions.checkArgument(!Strings.isNullOrEmpty("Family1"),
        "Family name may not be null or empty");

      Family.Builder familyBuilder = Family.newBuilder().setName("Family1");

      Column.Builder columnBuilder = Column.newBuilder();
      columnBuilder.setQualifier(ByteString.copyFromUtf8("Qaulifier"));

      if (value != null) {
        Cell.Builder cellBuilder = Cell.newBuilder();
        cellBuilder.setTimestampMicros(0L);
        cellBuilder.setValue(ByteString.copyFrom(value));
        columnBuilder.addCells(cellBuilder);
      }
      familyBuilder.addColumns(columnBuilder);
      Chunk contentChunk = Chunk.newBuilder().setRowContents(familyBuilder).build();
      Chunk rowCompleteChunk = Chunk.newBuilder().setCommitRow(true).build();

      ReadRowsResponse response =
          ReadRowsResponse.newBuilder().addChunks(contentChunk).addChunks(rowCompleteChunk)
              .setRowKey(ByteString.copyFromUtf8(rowKey)).build();

      responses.add(response);
    }
    return responses;
  }

  static int count = 5000000;

  private static void rowMergerPerf(List<ReadRowsResponse> responses) {
    RowAdapter adapter = Adapters.ROW_ADAPTER;
    adapter.adaptResponse(RowMerger.readNextRow(responses.iterator()));

    System.out.println("Size: " + responses.get(0).getSerializedSize());
    {
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        RowMerger.readNextRow(responses.iterator());
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger.readNext: %d rows merged in %d ms.  %d nanos per row.", count,
              time / 1000000, time / count));
    }
    {
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        adapter.adaptResponse(RowMerger.readNextRow(responses.iterator()));
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger + adaptResponse: %d rows merged in %d ms.  %d nanos per row.",
              count, time / 1000000, time / count));
    }
  }
}

