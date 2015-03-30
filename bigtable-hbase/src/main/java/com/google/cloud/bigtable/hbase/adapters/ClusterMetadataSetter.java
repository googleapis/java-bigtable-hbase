 package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.ListTablesRequest;
import com.google.cloud.bigtable.hbase.BigtableOptions;
import com.google.common.base.Preconditions;

/**
 * Utility class that will set given project and cluster name within service messages.
 */
public class ClusterMetadataSetter {
  public static final String BIGTABLE_V1_CLUSTER_FMT = "projects/%s/zones/%s/clusters/%s";

  public static ClusterMetadataSetter from(BigtableOptions options) {
    return new ClusterMetadataSetter(
        options.getProjectId(), options.getZone(), options.getCluster());
  }

  private final String formattedV1ClusterName;

  public ClusterMetadataSetter(String projectId, String zone, String clusterName) {
    this.formattedV1ClusterName =
        String.format(BIGTABLE_V1_CLUSTER_FMT, projectId, zone, clusterName);
  }

  public void setMetadata(ListTablesRequest.Builder builder) {
    builder.setName(formattedV1ClusterName);
  }

  public void setMetadata(CreateTableRequest.Builder builder) {
    builder.setName(formattedV1ClusterName);
  }

  public String getFormattedV1ClusterName() {
    return formattedV1ClusterName;
  }

  public String toHBaseTableName(String fullyQualifiedBigtableName) {
    Preconditions.checkNotNull(fullyQualifiedBigtableName, "table name cannot be null");
    String tablesPrefix =
        String.format("%s/%s/", formattedV1ClusterName, TableMetadataSetter.TABLE_SEPARATOR);
    Preconditions.checkState(fullyQualifiedBigtableName.startsWith(tablesPrefix),
      "'%s' does not start with '%s'", fullyQualifiedBigtableName, tablesPrefix);
    String tableName = fullyQualifiedBigtableName.substring(tablesPrefix.length());
    Preconditions.checkState(tableName.length() > 0, "TableName is blank");
    return tableName;
  }

}
