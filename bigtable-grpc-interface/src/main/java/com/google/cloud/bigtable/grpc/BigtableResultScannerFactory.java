package com.google.cloud.bigtable.grpc;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;

/**
 * A factory for creating ResultScanners that can be used to scan over Rows for a
 * given ReadRowsRequest.
 */
interface BigtableResultScannerFactory {

  /**
   * Create a scanner for the given request.
   */
  ResultScanner<Row> createScanner(ReadRowsRequest request);
}
