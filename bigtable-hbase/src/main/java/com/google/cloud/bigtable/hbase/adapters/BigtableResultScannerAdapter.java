package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.util.Throwables;
import com.google.bigtable.v1.Row;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Adapt a Bigtable ResultScanner to an HBase Result Scanner.
 */
public class BigtableResultScannerAdapter {

  final ResponseAdapter<Row, Result> rowAdapter;

  public BigtableResultScannerAdapter(ResponseAdapter<Row, Result> rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

  public ResultScanner adapt(
      final com.google.cloud.hadoop.hbase.ResultScanner<Row> bigtableResultScanner) {
    return new AbstractClientScanner() {
      @Override
      public Result next() throws IOException {
        Row row = bigtableResultScanner.next();
        if (row == null) {
          // Null signals EOF.
          return null;
        }
        return rowAdapter.adaptResponse(row);
      }

      @Override
      public void close() {
        try {
          bigtableResultScanner.close();
        } catch (IOException ioe) {
          throw Throwables.propagate(ioe);
        }
      }
    };
  }
}
