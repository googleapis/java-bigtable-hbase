package com.google.cloud.anviltop.hbase.adapters;

import com.google.api.client.util.Throwables;
import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.hadoop.hbase.AnviltopResultScanner;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

public class AnviltopResultScannerAdapter {

  final RowAdapter rowAdapter;

  public AnviltopResultScannerAdapter(RowAdapter rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

  public ResultScanner adapt(final AnviltopResultScanner anviltopResultScanner) {
    return new AbstractClientScanner() {
      @Override
      public Result next() throws IOException {
        AnviltopData.Row row = anviltopResultScanner.next();
        if (row == null) {
          return null;
        }
        return rowAdapter.adaptResponse(row);
      }

      @Override
      public void close() {
        try {
          anviltopResultScanner.close();
        } catch (IOException ioe) {
          throw Throwables.propagate(ioe);
        }
      }
    };
  }
}
