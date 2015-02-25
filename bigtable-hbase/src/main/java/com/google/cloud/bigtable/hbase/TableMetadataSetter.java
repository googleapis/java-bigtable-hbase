package com.google.cloud.bigtable.hbase;

import com.google.bigtable.anviltop.AnviltopServiceMessages;
import com.google.bigtable.anviltop.AnviltopServiceMessages.IncrementRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.SampleRowKeysRequest;

import org.apache.hadoop.hbase.TableName;

/**
 * Utility class that will set given project, cluster and table name within
 * service messages.
 */
public class TableMetadataSetter {
  public static final String BIGTABLE_V1_TABLENAME_FMT =
      "projects/%s/zones/%s/clusters/%s/tables/%s";

  public static TableMetadataSetter from(TableName tableName, BigtableOptions options) {
    return new TableMetadataSetter(
        tableName, options.getProjectId(), options.getZone(), options.getCluster());
  }

  private final TableName tableName;
  private final String projectId;
  private final String formattedV1TableName;

  public TableMetadataSetter(
      TableName tableName, String projectId, String zone, String clusterName) {
    this.tableName = tableName;
    this.projectId = projectId;
    this.formattedV1TableName = String.format(BIGTABLE_V1_TABLENAME_FMT,
        projectId, zone, clusterName, tableName.getQualifierAsString());
  }

  public void setMetadata(AnviltopServiceMessages.GetRowRequest.Builder builder) {
    builder.setTableName(tableName.getQualifierAsString());
    builder.setProjectId(projectId);
  }

  public void setMetadata(IncrementRowRequest.Builder builder) {
    builder.setTableName(tableName.getQualifierAsString());
    builder.setProjectId(projectId);
  }

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
}
