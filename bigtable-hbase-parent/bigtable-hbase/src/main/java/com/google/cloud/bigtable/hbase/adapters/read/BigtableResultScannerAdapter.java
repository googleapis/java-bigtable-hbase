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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.api.client.util.Throwables;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Adapt a Bigtable ResultScanner to an HBase Result Scanner.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableResultScannerAdapter {

  final ResponseAdapter<Row, Result> rowAdapter;

  /**
   * <p>Constructor for BigtableResultScannerAdapter.</p>
   *
   * @param rowAdapter a {@link com.google.cloud.bigtable.hbase.adapters.ResponseAdapter} object.
   */
  public BigtableResultScannerAdapter(ResponseAdapter<Row, Result> rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

  /**
   * <p>adapt.</p>
   *
   * @param bigtableResultScanner a {@link com.google.cloud.bigtable.grpc.scanner.ResultScanner} object.
   * @return a {@link org.apache.hadoop.hbase.client.ResultScanner} object.
   */
  public ResultScanner adapt(
      final com.google.cloud.bigtable.grpc.scanner.ResultScanner<Row> bigtableResultScanner) {
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

      /**
       * This is an HBase concept that was added in hbase 1.0.2.  It's not relevent for Cloud
       * Bigtable.  It will not be called from the hbase code and should not be called by the user.
       */
      // Developers Note: Do not add @Override so that this can remain backwards compatible with
      // 1.0.1.
      public boolean renewLease() {
        throw new UnsupportedOperationException("renewLease");
      }
    };
  }
}
