package com.google.cloud.bigtable.hbase.adapters;

import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.hbase.BigtableOptions;
import com.google.common.base.Preconditions;

public class ColumnFamilyFormatter {

  final static String COLUMN_FAMILIES_SEPARARATOR = "columnFamilies";

  public static ColumnFamilyFormatter from(TableName tableName, BigtableOptions options) {
    return new ColumnFamilyFormatter(TableMetadataSetter.from(tableName, options));
  }

  private final TableMetadataSetter tableMetadataSetter;

  public ColumnFamilyFormatter(TableMetadataSetter tableMetadataSetter) {
    this.tableMetadataSetter = tableMetadataSetter;
  }

  public String formatForBigtable(String hbaseColumnFamily) {
    return String.format("%s/%s/%s", tableMetadataSetter.getFormattedV1TableName(),
      COLUMN_FAMILIES_SEPARARATOR, hbaseColumnFamily);
  }

  public String formatForHBase(String bigtableColumnFamily) {
    String tableName = tableMetadataSetter.getFormattedV1TableName();
    String familyPrefix = String.format("%s/%s", tableName, COLUMN_FAMILIES_SEPARARATOR);
    Preconditions.checkState(bigtableColumnFamily.startsWith(familyPrefix),
      "Column family '%s' needs to start with '%s'", bigtableColumnFamily, tableName);
    return bigtableColumnFamily.substring(familyPrefix.length());
  }
}
