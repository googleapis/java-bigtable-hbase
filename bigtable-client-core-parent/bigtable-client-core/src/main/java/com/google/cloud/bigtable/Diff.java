package com.google.cloud.bigtable;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Diff {

  public static void main(String[] args) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(100);

    BigtableOptions options = new BigtableOptions.Builder()
        .setUserAgent("igorbernstein-dev")
        .setProjectId("igorbernstein-dev")
        .setInstanceId("instance1")

        .build();

    try (BigtableSession session = new BigtableSession(options)) {
      final BigtableDataClient dataClient = session.getDataClient();

      SampleRowKeysRequest r = SampleRowKeysRequest.newBuilder()
          .setTableName("projects/igorbernstein-dev/instances/instance1/tables/table1")
          .build();
      ImmutableList<SampleRowKeysResponse> sampleRows = dataClient.sampleRowKeys(r);

      ByteString lastKey = ByteString.EMPTY;

      for (SampleRowKeysResponse sampleRow : sampleRows) {
        System.out.printf("Sample: offset=%d key=%s\n", sampleRow.getOffsetBytes(), sampleRow.getRowKey().toStringUtf8());
        ByteString curKey = sampleRow.getRowKey();
        schedule(executorService, dataClient, lastKey, curKey);
        lastKey = curKey;
        break;
      }
      schedule(executorService, dataClient, lastKey, ByteString.EMPTY);

      executorService.shutdown();
      while(!executorService.awaitTermination(1, TimeUnit.SECONDS)) {

      }
      System.out.println("closing");
    }

  }

  private static void schedule(ExecutorService exe, final BigtableDataClient dataClient, final ByteString startKey, final ByteString endKey) {
    System.out.println("Submitting: " + startKey.toStringUtf8() + " - " + endKey.toStringUtf8());

    exe.submit(new Runnable() {
      @Override
      public void run() {
        try {
          diffBundle(dataClient, startKey, endKey);
        } catch (Exception e) {
          System.out.println("Fuck:" + e);
        }
      }
    });
  }

  static AtomicLong cnt = new AtomicLong();

  private static void diffBundle(BigtableDataClient dataClient, ByteString startKey, ByteString endKey)
      throws IOException {
    ReadRowsRequest r1 = ReadRowsRequest.newBuilder()
        .setTableName("projects/igorbernstein-dev/instances/instance1/tables/table1")
        .setRows(
            RowSet.newBuilder()
                .addRowRanges(
                    RowRange.newBuilder()
                        .setStartKeyClosed(startKey)
                        .setEndKeyOpen(endKey)
                )
        ).build();

    ReadRowsRequest r2 = ReadRowsRequest.newBuilder(r1)
        .setTableName("projects/igorbernstein-dev/instances/instance1/tables/table2")
        .build();


        ResultScanner<FlatRow> s1 = dataClient.readFlatRows(r1);
        ResultScanner<FlatRow> s2 = dataClient.readFlatRows(r2);

    long i = 0;

      while (true) {
        i++;

        if (i%1000 == 0) {
          long total = cnt.addAndGet(1000);
          System.out.println(total);
        }
        FlatRow row1 = s1.next();
        FlatRow row2 = s2.next();

        if (row2 == null) {
          System.out.println("-");
          break;
        }

        if (!Objects.equals(row1, row2)) {
          System.out.printf("Mismatch in (%s, %s)\n", startKey.toStringUtf8(), endKey.toStringUtf8());
          return;
        }
        else if (row1 == null) {
          System.out.println("=");
          break;
        }
      }

      System.out.print(".");
      System.out.flush();
    }

}
