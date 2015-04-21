package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

/**
 * A Get adapter that transform the Get into a ReadRowsRequest using the proto-based
 * filter language.
 */
public class GetAdapter implements OperationAdapter<Get, ReadRowsRequest.Builder> {

  protected final ScanAdapter scanAdapter;
  public GetAdapter(ScanAdapter scanAdapter) {
    this.scanAdapter = scanAdapter;
  }

  @Override
  public ReadRowsRequest.Builder adapt(Get operation) {
    Scan operationAsScan = new Scan(operation);
    scanAdapter.throwIfUnsupportedScan(operationAsScan);
    return ReadRowsRequest.newBuilder()
        .setFilter(scanAdapter.buildFilter(operationAsScan))
        .setRowKey(ByteString.copyFrom(operation.getRow()));
  }
}