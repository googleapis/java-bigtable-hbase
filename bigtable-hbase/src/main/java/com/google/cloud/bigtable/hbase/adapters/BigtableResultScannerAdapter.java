package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.util.Throwables;
import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.hadoop.hbase.AnviltopResultScanner;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

public class BigtableResultScannerAdapter {

  final RowAdapter rowAdapter;

  public BigtableResultScannerAdapter(RowAdapter rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

  public ResultScanner adapt(final AnviltopResultScanner bigtableResultScanner) {
    return new AbstractClientScanner() {
      @Override
      public Result next() throws IOException {
        AnviltopData.Row row = bigtableResultScanner.next();
        if (row == null) {
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
