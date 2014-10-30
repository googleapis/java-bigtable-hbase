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


import com.google.bigtable.anviltop.AnviltopServices.IncrementRowRequest;
import com.google.bigtable.anviltop.AnviltopServices.IncrementRowRequest.Builder;
import com.google.bigtable.anviltop.AnviltopData.RowIncrement;
import com.google.cloud.anviltop.hbase.AnviltopConstants;
import com.google.common.collect.ImmutableList;
import com.google.cloud.hadoop.hbase.repackaged.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Increment;

import java.util.Map;
import java.util.NavigableMap;


/**
 * Adapter for HBase Increment operations to Anviltop IncrementRowRequest.Builder.
 */
public class IncrementAdapter implements OperationAdapter<Increment, IncrementRowRequest.Builder>{

  @Override
  public Builder adapt(Increment operation) {
    if (!operation.getTimeRange().isAllTime()) {
      throw new UnsupportedOperationException(
          "Setting the time range in an Increment is not implemented");
    }

    IncrementRowRequest.Builder result = IncrementRowRequest.newBuilder();
    RowIncrement.Builder builder = result.getIncrementBuilder();

    builder.setRowKey(ByteString.copyFrom(operation.getRow()));
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : operation.getFamilyMapOfLongs().entrySet()) {
      ByteString familyByteString = ByteString.copyFrom(familyEntry.getKey());

      for (Map.Entry<byte[], Long> qualifierEntry : familyEntry.getValue().entrySet()){
        builder.addIncrsBuilder()
            .setAmount(qualifierEntry.getValue())
            .setColumnName(
                ByteString.copyFrom(
                    ImmutableList.of(
                        familyByteString,
                        AnviltopConstants.ANVILTOP_COLUMN_SEPARATOR_BYTE_STRING,
                        ByteString.copyFrom(qualifierEntry.getKey()))));
      }
    }
    return result;
  }
}
