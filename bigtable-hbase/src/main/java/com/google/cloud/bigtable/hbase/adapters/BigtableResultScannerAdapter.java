/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
      final com.google.cloud.bigtable.grpc.ResultScanner<Row> bigtableResultScanner) {
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
