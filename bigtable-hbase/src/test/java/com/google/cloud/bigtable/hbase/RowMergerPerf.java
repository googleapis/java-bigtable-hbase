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

import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.grpc.scanner.v2.RowMerger;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.RowAdapter;
import com.google.protobuf.BigtableZeroCopyByteStringUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;

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
    return Arrays.asList(ReadRowsResponse.newBuilder().addChunks(CellChunk.newBuilder()
        .setRowKey(ByteString.copyFromUtf8("rowKey-0"))
        .setFamilyName(StringValue.newBuilder().setValue("Family1"))
        .setQualifier(BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("Qaulifier")))
        .setValue(BigtableZeroCopyByteStringUtil.wrap(RandomStringUtils.randomAlphanumeric(10000).getBytes()))
        .setCommitRow(true)).build());
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

