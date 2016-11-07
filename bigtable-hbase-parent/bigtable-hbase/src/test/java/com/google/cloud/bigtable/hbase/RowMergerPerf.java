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
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;

/**
 * Simple microbenchmark for {@link RowMerger}
 */
public class RowMergerPerf {

  static final int VALUE_SIZE_IN_BYTES = 10_000_000;
  static final long CUMULATIVE_CELL_COUNT = 10_000_000l;

  public static void main(String[] args) {
    // warm up
    for (int i = 0; i < 3; i++) {
      rowMergerPerf(createResponses(1), 1);
    }
    for (int i = 5; i <= 105; i += 10) {
      System.out.println("===================");
      System.out.println("using " + i + " Cells");
      rowMergerPerf(createResponses(i), i);
    }
  }

  private static List<ReadRowsResponse> createResponses(int cellCount) {
    Builder readRowsResponse = ReadRowsResponse.newBuilder();

    Preconditions.checkArgument(cellCount > 0, "cellCount has to be > 0.");

    // It's ok if 100_000 / cellCount rounds down.  This only has to be approximate.
    int size = VALUE_SIZE_IN_BYTES / cellCount;
    Preconditions.checkArgument(size > 0, "size has to be > 0.");
    for (int i = 0; i < cellCount; i++) {
      CellChunk contentChunk =
          CellChunk.newBuilder()
              .setRowKey(ByteString.copyFrom(Bytes.toBytes("rowkey-0")))
              .setFamilyName(StringValue.newBuilder().setValue("Family" + (i / 4)))
              .setQualifier(BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("Qualifier" + (i%4))))
              .setValue(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(size).getBytes()))
              .setTimestampMicros(0L)
              .setCommitRow(i == cellCount - 1)
              .build();

      readRowsResponse.addChunks(contentChunk);
    }
    return Arrays.asList(readRowsResponse.build());
  }

  private static void rowMergerPerf(List<ReadRowsResponse> responses, int cellCount) {
    RowAdapter adapter = Adapters.ROW_ADAPTER;
    long rowCount = CUMULATIVE_CELL_COUNT / cellCount;
    System.out.println("Size: " + responses.get(0).getSerializedSize());
    {
      long start = System.nanoTime();
      for (int i = 0; i < rowCount; i++) {
        RowMerger.toRows(responses).get(0);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format(
              "RowMerger: %d rows adapted in %d ms.\n"
                  + "\t%d nanos per row\n"
                  + "\t%d nanos per cell",
              rowCount, time / 1000000, time / rowCount, time / CUMULATIVE_CELL_COUNT));
    }
    {
      long start = System.nanoTime();
      Row response = RowMerger.toRows(responses).get(0);
      for (int i = 0; i < rowCount; i++) {
        adapter.adaptResponse(response);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format(
              "AdaptResponse: %d rows adapted in %d ms.\n"
                  + "\t%d nanos per row\n"
                  + "\t%d nanos per cell",
              rowCount, time / 1000000, time / rowCount, time / CUMULATIVE_CELL_COUNT));
    }
  }
}
