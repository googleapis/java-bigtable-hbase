package com.google.cloud.bigtable.hbase;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.Builder;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.RowAdapter;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;

/**
 * Simple microbenchmark for {@link RowMerger}
 */
public class RowMergerPerf {

  public static void main(String[] args) {
    for (int i = 0; i < 5; i++) {
      rowMergerPerf(createResponses(1));
    }
    for (int i = 1; i <= 101; i += 10) {
      System.out.println("===================");
      System.out.println("using " + i + " Cells");
      rowMergerPerf(createResponses(i));
    }
  }

  private static List<ReadRowsResponse> createResponses(int cellCount) {
    Builder readRowsResponse = ReadRowsResponse.newBuilder();

    int size = 100_000 / cellCount;
    for (int i = 0; i < cellCount; i++) {
      CellChunk contentChunk =
          CellChunk.newBuilder()
              .setFamilyName(StringValue.newBuilder().setValue("Family" + i))
              .setQualifier(BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("Qaulifier")))
              .setRowKey(ByteString.copyFrom(Bytes.toBytes("rowkey-0")))
              .setValue(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(size).getBytes()))
              .setTimestampMicros(0L)
              .setCommitRow(i == cellCount - 1)
              .build();

      readRowsResponse.addChunks(contentChunk);
    }
    return Arrays.asList(readRowsResponse.build());
  }

  static int count = 1_000_000;

  private static void rowMergerPerf(List<ReadRowsResponse> responses) {
    RowAdapter adapter = Adapters.ROW_ADAPTER;
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
      Row response = RowMerger.toRows(responses).get(0);
      for (int i = 0; i < count; i++) {
        adapter.adaptResponse(response);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("AdaptResponse: %d rows adapted in %d ms.  %d nanos per row.",
              count, time / 1000000, time / count));
    }
  }
}
