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
package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.common.base.Throwables;

import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;

import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;

import java.io.IOException;

/**
 * Adapt a Bigtable ResultScanner to an HBase Result Scanner. Stops when {@link org.apache.hadoop.hbase.filter.WhileMatchFilter}
 * filters out the remaining rows.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableWhileMatchResultScannerAdapter {

  private static final String WHILE_MATCH_FILTER_IN_LABEL_SUFFIX = "-in";
  private static final String WHILE_MATCH_FILTER_OUT_LABEL_SUFFIX = "-out";

  final ResponseAdapter<FlatRow, Result> rowAdapter;

  /**
   * <p>Constructor for BigtableWhileMatchResultScannerAdapter.</p>
   *
   * @param rowAdapter a {@link com.google.cloud.bigtable.hbase.adapters.ResponseAdapter} object.
   */
  public BigtableWhileMatchResultScannerAdapter(ResponseAdapter<FlatRow, Result> rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

  /**
   * <p>
   * adapt.
   * </p>
   * @param bigtableResultScanner a {@link com.google.cloud.bigtable.grpc.scanner.ResultScanner}
   *          object.
   * @param span A parent {@link Span} for the scan that needs to be closed when the scanning is
   *          complete. The span has an HBase specific tag, which needs to be handled by the
   *          adapter.
   * @return a {@link org.apache.hadoop.hbase.client.ResultScanner} object.
   */
  public ResultScanner adapt(
      final com.google.cloud.bigtable.grpc.scanner.ResultScanner<FlatRow> bigtableResultScanner,
      final Span span) {
    return new AbstractClientScanner() {
      @Override
      public Result next() throws IOException {
        FlatRow row = bigtableResultScanner.next();
        if (row == null) {
          // Null signals EOF.
          closeSpan();
          return null;
        }

        if (!hasMatchingLabels(row)) {
          close();
          return null;
        }

        return rowAdapter.adaptResponse(row);
      }

      protected void closeSpan() {
        try (AutoCloseable c = Tracing.getTracer().withSpan(span)) {
          span.end();
        } catch (Exception e) {
          // ignore. withSpan() returns a Closable which can throw exceptions, but no exceptions
          // will actually be thrown
        }
      }

      @Override
      public void close() {
        try {
          bigtableResultScanner.close();
        } catch (IOException ioe) {
          throw Throwables.propagate(ioe);
        } finally {
          closeSpan();
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

  /**
   * Returns {@code true} iff there are matching {@link WhileMatchFilter} labels or no {@link
   * WhileMatchFilter} labels.
   */
  public static boolean hasMatchingLabels(FlatRow row) {
    int inLabelCount = 0;
    int outLabelCount = 0;
    for (FlatRow.Cell cell : row.getCells()) {
      for (String label : cell.getLabels()) {
        // TODO(kevinsi4508): Make sure {@code label} is a {@link WhileMatchFilter} label.
        // TODO(kevinsi4508): Handle multiple {@link WhileMatchFilter} labels.
        if (label.endsWith(WHILE_MATCH_FILTER_IN_LABEL_SUFFIX)) {
          inLabelCount++;
        } else if (label.endsWith(WHILE_MATCH_FILTER_OUT_LABEL_SUFFIX)) {
          outLabelCount++;
        }
      }
    }

    // Checks if there is mismatching {@link WhileMatchFilter} label.
    if (inLabelCount != outLabelCount) {
      return false;
    }

    return true;
  }
}