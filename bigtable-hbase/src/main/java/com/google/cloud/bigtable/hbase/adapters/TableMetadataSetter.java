package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest;
import com.google.bigtable.admin.table.v1.Table;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.cloud.bigtable.hbase.BigtableOptions;

import org.apache.hadoop.hbase.TableName;

/**
 * Utility class that will set given project, cluster and table name within
 * service messages.
 */
public class TableMetadataSetter {
  public static final String TABLE_SEPARATOR = "tables";
  public static final String BIGTABLE_V1_TABLENAME_FMT =
      ClusterMetadataSetter.BIGTABLE_V1_CLUSTER_FMT + "/" + TABLE_SEPARATOR + "/%s";

  public static TableMetadataSetter from(TableName tableName, BigtableOptions options) {
    return new TableMetadataSetter(getBigtableName(tableName, options));
  }

  public static String getBigtableName(TableName tableName, BigtableOptions options) {
    return getName(options.getProjectId(), options.getZone(), options.getCluster(), tableName);
  }

  @VisibleForTesting
  static String getName(String projectId, String zone, String clusterName, TableName tableName) {
    return String.format(BIGTABLE_V1_TABLENAME_FMT,
      projectId,
      zone,
      clusterName,
      tableName.getQualifierAsString());
  }

  private final String formattedV1TableName;

  public TableMetadataSetter( String formattedV1TableName) {
    this.formattedV1TableName = formattedV1TableName;
  }

  // Table Admin builders
  public void setMetadata(Table.Builder builder) {
    builder.setName(formattedV1TableName);
  }

  public void setMetadata(DeleteTableRequest.Builder builder) {
    builder.setName(formattedV1TableName);
  }

  public void setMetadata(CreateColumnFamilyRequest.Builder builder) {
    builder.setName(formattedV1TableName);
  }

  // Table read/write builders.
  public void setMetadata(MutateRowRequest.Builder builder) {
    builder.setTableName(formattedV1TableName);
  }

  public void setMetadata(CheckAndMutateRowRequest.Builder builder) {
    builder.setTableName(formattedV1TableName);
  }

  public void setMetadata(ReadModifyWriteRowRequest.Builder builder) {
    builder.setTableName(formattedV1TableName);
  }

  public void setMetadata(SampleRowKeysRequest.Builder builder) {
    builder.setTableName(formattedV1TableName);
  }

  public void setMetadata(ReadRowsRequest.Builder builder) {
    builder.setTableName(formattedV1TableName);
  }

  public String getFormattedV1TableName() {
    return formattedV1TableName;
  }
}
