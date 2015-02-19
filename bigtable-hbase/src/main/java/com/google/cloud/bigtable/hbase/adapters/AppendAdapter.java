package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRule;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

/**
 * Adapter for HBase Appends operations to Bigtable ReadModifyWriteRowRequest.Builder.
 */
public class AppendAdapter implements OperationAdapter<Append, ReadModifyWriteRowRequest.Builder> {

  @Override
  public ReadModifyWriteRowRequest.Builder adapt(Append operation) {
    ReadModifyWriteRowRequest.Builder result = ReadModifyWriteRowRequest.newBuilder();
    result.setRowKey(ByteString.copyFrom(operation.getRow()));

    for (Map.Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()){
      String familyName = Bytes.toString(entry.getKey());
      // Bigtable applies all appends present in a single RPC. HBase applies only the last
      // mutation present, if any. We remove all but the last mutation for each qualifier here:
      List<Cell> cells = CellDeduplicationHelper.deduplicateFamily(operation, entry.getKey());

      for (Cell cell : cells) {
        ReadModifyWriteRule.Builder rule = result.addRulesBuilder();
        rule.setFamilyName(familyName);
        rule.setColumnQualifier(
            ByteString.copyFrom(
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength()));
        rule.setAppendValue(
            ByteString.copyFrom(
                cell.getValueArray(),
                cell.getValueOffset(),
                cell.getValueLength()));
      }
    }
    return result;
  }
}
