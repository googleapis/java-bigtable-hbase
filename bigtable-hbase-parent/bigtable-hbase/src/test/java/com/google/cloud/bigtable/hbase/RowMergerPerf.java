package com.google.cloud.bigtable.hbase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.codahale.metrics.ConsoleReporter;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.Builder;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.grpc.BigtableSession;
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
    for (int i = 0; i < 10; i++) {
      rowMergerPerf(createResponses());
    }

    ConsoleReporter.forRegistry(BigtableSession.metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MICROSECONDS)
        .build().report();
  }

  private static List<ReadRowsResponse> createResponses() {
    int max = 10;
    Builder response = ReadRowsResponse.newBuilder();
    final StringValue family = StringValue.newBuilder().setValue("Family1").build();
    final ByteString row = ByteString.copyFrom(Bytes.toBytes("rowkey-0"));
    for (int i = 1; i <= max; i++) {
      CellChunk contentChunk =
          CellChunk.newBuilder().setFamilyName(family)
              .setQualifier(
                BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("Qaulifier" + i)))
              .setRowKey(row)
              .setValue(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(10000).getBytes()))
              .setTimestampMicros(0L).setCommitRow(i == max).build();
      response.addChunks(contentChunk);
    }
    return Arrays.asList(response.build());
  }

  static int count = 50_000;

  private static void rowMergerPerf(List<ReadRowsResponse> responses) {
    RowAdapter adapter = Adapters.ROW_ADAPTER;
//    System.out.println("Size: " + responses.get(0).getSerializedSize());
//    {
//      long start = System.nanoTime();
//      for (int i = 0; i < count; i++) {
//        RowMerger.toRows(responses);
//      }
//      long time = System.nanoTime() - start;
//      System.out.println(
//          String.format("RowMerger.readNext: %d rows merged in %d ms.  %d nanos per row.", count,
//              time / 1000000, time / count));
//    }
//    {
//      long start = System.nanoTime();
//      for (int i = 0; i < count; i++) {
//        adapter.adaptResponse(RowMerger.toRows(responses).get(0));
//      }
//      long time = System.nanoTime() - start;
//      System.out.println(
//          String.format("RowMerger + adaptResponse: %d rows merged in %d ms.  %d nanos per row.",
//              count, time / 1000000, time / count));
//    }
    {
      long start = System.nanoTime();
      Row row = RowMerger.toRows(responses).get(0);
      for (int i = 0; i < count; i++) {
        adapter.adaptResponse(row);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("adaptResponse: %d rows merged in %d ms.  %d nanos per row.",
              count, time / 1000000, time / count));
    }
  }
}
