package com.google.cloud.bigtable.hbase;


import java.util.Arrays;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.Builder;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;

import io.grpc.stub.StreamObserver;

/**
 * Simple microbenchmark for {@link RowMerger}
 */
public class RowMergerPerf {

  static final int VALUE_SIZE_IN_BYTES = 10_000_000;
  static final long CUMULATIVE_CELL_COUNT = 30_000_000l;

  public static void main(String[] args) {
//     warm up
    for (int i = 0; i < 2; i++) {
      System.out.println("===================");
      System.out.println("testing 1 Cell");
      rowMergerPerf(1);
    }

      System.out.println("===================");
      System.out.println("testing " + 10 + " Cells");
      rowMergerPerf(10);

    for (int i = 5; i <= 105; i += 10) {
      System.out.println("===================");
      System.out.println("testing " + i + " Cells");
      rowMergerPerf(i);
    }
  }

  private static ReadRowsResponse createResponses(int cellCount) {
    Builder readRowsResponse = ReadRowsResponse.newBuilder();

    Preconditions.checkArgument(cellCount > 0, "cellCount has to be > 0.");

    // It's ok if 100_000 / cellCount rounds down.  This only has to be approximate.
    int size = VALUE_SIZE_IN_BYTES / cellCount;
    final int qualifiersPerFamily = 15;
    Preconditions.checkArgument(size > 0, "size has to be > 0.");
    final ByteString rowKey = ByteString.copyFrom(Bytes.toBytes("rowkey-0"));
    for (int i = 0; i < cellCount; i++) {
      CellChunk.Builder contentChunk =
          CellChunk.newBuilder()
              .setRowKey(i == 0 ? rowKey : ByteString.EMPTY)
              .setQualifier(BytesValue.newBuilder()
                  .setValue(ByteString.copyFromUtf8("Qualifier" + (i % qualifiersPerFamily))))
              .setValue(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(size).getBytes()))
              .setTimestampMicros(330020L)
              .setCommitRow(i == cellCount - 1);
      if (i % qualifiersPerFamily == 0) {
        contentChunk.setFamilyName(StringValue.newBuilder().setValue("Family" + (i / qualifiersPerFamily)));
      }

      readRowsResponse.addChunks(contentChunk);
    }
    return readRowsResponse.build();
  }

  final static StreamObserver<FlatRow> EMPTY_OBSERVER = new StreamObserver<FlatRow>() {
    @Override
    public void onNext(FlatRow value) {
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
  };

  private static void rowMergerPerf(int cellCountPerRow) {
    ReadRowsResponse response = createResponses(cellCountPerRow);
    System.out.println("Size: " + response.getSerializedSize());

    {
      long rowCount = CUMULATIVE_CELL_COUNT / cellCountPerRow;
      long start = System.nanoTime();
      for (int i = 0; i < rowCount; i++) {
        new RowMerger(EMPTY_OBSERVER).onNext(response);
      }
      print("RowMerger", start, rowCount, cellCountPerRow);
    }

    {
      // The adapter is slower than the RowMerger, so decrease the number of rows by a factor of 3
      // so that the test finishes faster. This will be enough of a sample to get an idea about the
      // adapter's performance
      long rowCount = CUMULATIVE_CELL_COUNT / cellCountPerRow;

      FlatRow flatRow = RowMerger.toRows(Arrays.asList(response)).get(0);
      long start = System.nanoTime();
      for (int i = 0; i < rowCount; i++) {
        Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow);
      }
      print("AdaptResponse", start, rowCount, cellCountPerRow);
    }
  }

  protected static void print(String type, long start, long rowCount, int cellCountPerRow) {
    long time = System.nanoTime() - start;
    System.out.println(
        String.format(
            "%s: %d rows adapted in %d ms.\n"
                + "\t%d nanos per row\n"
                + "\t%d nanos per cell",
            type, rowCount, time / 1000000, time / rowCount, time / (rowCount * cellCountPerRow)));
  }
}
