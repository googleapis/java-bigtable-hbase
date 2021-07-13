/*
 * Copyright 2015 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Adapter for HBase Appends operations to Bigtable ReadModifyWriteRowRequest.Builder.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class AppendAdapter implements OperationAdapter<Append, ReadModifyWriteRow> {

  /** {@inheritDoc} */
  @Override
  public void adapt(Append operation, ReadModifyWriteRow readModifyWriteRow) {
    for (Map.Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()) {
      String familyName = Bytes.toString(entry.getKey());
      // Bigtable applies all appends present in a single RPC. HBase applies only the last
      // mutation present, if any. We remove all but the last mutation for each qualifier here:
      List<Cell> cells = CellDeduplicationHelper.deduplicateFamily(operation, entry.getKey());

      for (Cell cell : cells) {
        readModifyWriteRow.append(
            familyName,
            ByteString.copyFrom(
                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
            ByteString.copyFrom(
                cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      }
    }
  }
}
