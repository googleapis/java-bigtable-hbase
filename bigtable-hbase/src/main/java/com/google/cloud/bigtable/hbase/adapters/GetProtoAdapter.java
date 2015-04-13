package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.v1.ReadRowsRequest;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

/**
 * A Get adapter that transform the Get into a ReadRowsRequest using the proto-based
 * filter language.
 * TODO(angusdavis): Rename this GetAdapter when fully implemented.
 */
public class GetProtoAdapter implements OperationAdapter<Get, ReadRowsRequest.Builder> {

  protected final ScanProtoAdapter scanAdapter;
  public GetProtoAdapter(ScanProtoAdapter scanAdapter) {
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