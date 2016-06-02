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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;

import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.RowAdapter;
import com.google.protobuf.ByteString;

/**
 * Simple microbenchmark for {@link RowMerger}
 */
public class RowMergerPerf {

  public static void main(String[] args) {
    final List<ReadRowsResponse> responses = createResponses();
    for (int i = 0; i < 10; i++) {
      rowMergerPerf(responses);
    }
  }

  // TODO: test with more cells
  private static List<ReadRowsResponse> createResponses() {
    Family.Builder familyBuilder = Family.newBuilder().setName("Family1");
    Column.Builder columnBuilder =
        familyBuilder.addColumnsBuilder()
        .setQualifier(ByteString.copyFromUtf8("Qualifier"));
    columnBuilder.addCellsBuilder()
        .setTimestampMicros(0L)
        .setValue(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(10000).getBytes()));

    return Arrays.asList(ReadRowsResponse.newBuilder()
        .addChunks(Chunk.newBuilder().setRowContents(familyBuilder))
        .addChunks(Chunk.newBuilder().setCommitRow(true))
        .setRowKey(ByteString.copyFromUtf8("rowKey-0")).build());
  }

  static int count = 5000000;

  private static void rowMergerPerf(List<ReadRowsResponse> responses) {
    RowAdapter adapter = Adapters.ROW_ADAPTER;
    adapter.adaptResponse(RowMerger.toRows(responses).get(0));

    System.out.println("Size: " + responses.get(0).getSerializedSize());
    {
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        RowMerger.toRows(responses);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger.readNext: %d rows merged in %d ms.  %d nanos per row.", count,
              time / 1000000, time / count));
    }
    {
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        adapter.adaptResponse(RowMerger.toRows(responses).get(0));
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger + adaptResponse: %d rows merged in %d ms.  %d nanos per row.",
              count, time / 1000000, time / count));
    }
  }
}

