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
package com.google.cloud.bigtable.hbase.adapters;


import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRule;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;


/**
 * Adapter for HBase Increment operations to Bigtable ReadModifyWriteRowRequest.Builder.
 */
public class IncrementAdapter
    implements OperationAdapter<Increment, ReadModifyWriteRowRequest.Builder>{

  @Override
  public ReadModifyWriteRowRequest.Builder adapt(Increment operation) {
    if (!operation.getTimeRange().isAllTime()) {
      throw new UnsupportedOperationException(
          "Setting the time range in an Increment is not implemented");
    }

    ReadModifyWriteRowRequest.Builder result = ReadModifyWriteRowRequest.newBuilder();

    result.setRowKey(ByteStringer.wrap(operation.getRow()));

    for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry :
        operation.getFamilyMapOfLongs().entrySet()) {
      String familyName = Bytes.toString(familyEntry.getKey());
      // Bigtable applies all increments present in a single RPC. HBase applies only the last
      // mutation present, if any. We remove all but the last mutation for each qualifier here:
      List<Cell> mutationCells =
          CellDeduplicationHelper.deduplicateFamily(operation, familyEntry.getKey());

      for (Cell cell : mutationCells){
        ReadModifyWriteRule.Builder rule = result.addRulesBuilder();
        rule.setIncrementAmount(
            Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        rule.setFamilyName(familyName);
        rule.setColumnQualifier(
            ByteString.copyFrom(
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength()));
      }
    }
    return result;
  }
}
