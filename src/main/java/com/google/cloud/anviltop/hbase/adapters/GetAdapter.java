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

/**
 * Adapter for HBase Get operations to Anviltop GetRowRequest.Builder.
 */
public class GetAdapter implements OperationAdapter<Get, GetRowRequest.Builder>{
  @Override
  public Builder adapt(Get operation) {
    throwIfUnsupportedOperation(operation);

    GetRowRequest.Builder result = GetRowRequest.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));
    return result;
  }

  private void throwIfUnsupportedOperation(Get operation) {
    // As more operations are supported at the Anviltop API level and Result adapters are added,
    // these checks will be reduced or changed.
    if (operation.getFilter() != null) {
      throw new UnsupportedOperationException(
          "Filters on Get are not supported.");
    }
    if (!operation.getFamilyMap().isEmpty()) {
      throw new UnsupportedOperationException(
          "Limiting of column families returned is not supported.");
    }
    if (operation.getMaxVersions() != Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          "Limiting of versions on Get is not supported.");
    }
    if (operation.getMaxResultsPerColumnFamily() != -1) {
      throw new UnsupportedOperationException(
          "Limiting of max results per column family is not supported.");
    }
    if (!operation.getTimeRange().isAllTime()) {
      throw new UnsupportedOperationException(
          "Time range limiting is not supported.");
    }
  }
}
