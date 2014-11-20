package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServiceMessages.AppendRowRequest;
import com.google.cloud.anviltop.hbase.AnviltopConstants;
import com.google.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;

import java.util.List;
import java.util.Map;

/**
 * Adapter for HBase Appends operations to Anviltop AppendRowRequest.Builder.
 */
public class AppendAdapter implements OperationAdapter<Append, AppendRowRequest.Builder> {

  @Override
  public AppendRowRequest.Builder adapt(Append operation) {
    AppendRowRequest.Builder result = AppendRowRequest.newBuilder();
    AnviltopData.RowAppend.Builder append = result.getAppendBuilder();
    append.setRowKey(ByteString.copyFrom(operation.getRow()));
    append.setReturnResults(operation.isReturnResults());

    for (Map.Entry<byte[], List<Cell>> entry : operation.getFamilyCellMap().entrySet()){
      for (Cell cell : entry.getValue()) {
        ByteString columnName = ByteString.copyFrom(
            ImmutableList.of(
                ByteString.copyFrom(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()),
                AnviltopConstants.ANVILTOP_COLUMN_SEPARATOR_BYTE_STRING,
                ByteString.copyFrom(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())));

        append.addAppendsBuilder()
            .setColumnName(columnName)
            .setValue(ByteString.copyFrom(CellUtil.cloneValue(cell)));

      }
    }
    return result;
  }
}
