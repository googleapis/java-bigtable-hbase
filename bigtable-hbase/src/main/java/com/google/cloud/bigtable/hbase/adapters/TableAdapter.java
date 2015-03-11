package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.Table;
import com.google.cloud.bigtable.hbase.BigtableOptions;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.util.Map;
import java.util.Map.Entry;

public class TableAdapter {

  private final ClusterMetadataSetter clusterMetadataSetter;
  private final ColumnDescriptorAdapter columnDescriptorAdapter;

  public TableAdapter(BigtableOptions options, ColumnDescriptorAdapter columnDescriptorAdapter) {
    this.clusterMetadataSetter = ClusterMetadataSetter.from(options);
    this.columnDescriptorAdapter = columnDescriptorAdapter;
  }

  public Table adapt(HTableDescriptor desc) {
    Table.Builder tableBuilder = Table.newBuilder();
    Map<String, ColumnFamily> columnFamilies = tableBuilder.getMutableColumnFamilies();
    for (HColumnDescriptor column : desc.getColumnFamilies()) {
      ColumnFamily columnFamily = columnDescriptorAdapter.adapt(column).build();
      String columnName = column.getNameAsString();
      columnFamilies.put(columnName, columnFamily);
    }
    return tableBuilder.build();
  }

  public HTableDescriptor adapt(Table table) {
    String tableNameStr = clusterMetadataSetter.toHBaseTableName(table.getName());
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableNameStr));
    for (Entry<String, ColumnFamily> entry : table.getColumnFamilies().entrySet()) {
      tableDescriptor.addFamily(columnDescriptorAdapter.adapt(entry.getKey(), entry.getValue()));
    }
    return tableDescriptor;
  }
}
