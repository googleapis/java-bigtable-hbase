/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;


import com.google.bigtable.anviltop.AnviltopServices.GetRowRequest;
import com.google.bigtable.anviltop.AnviltopServices.GetRowRequest.Builder;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Adapter for HBase Get operations to Anviltop GetRowRequest.Builder.
 */
public class GetAdapter implements OperationAdapter<Get, GetRowRequest.Builder>{
  @Override
  public Builder adapt(Get operation) {
    ScanAdapter.throwIfUnsupportedScan(new Scan(operation));

    GetRowRequest.Builder result = GetRowRequest.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));
    result.setFilterBytes(
        ByteString.copyFrom(
            ScanAdapter.buildFilterByteString(new Scan(operation))));
    return result;
  }
}
