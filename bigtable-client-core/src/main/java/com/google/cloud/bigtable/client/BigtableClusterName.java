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
package com.google.cloud.bigtable.client;

import com.google.common.base.Preconditions;

/**
 * This class encapsulates a Bigtable cluster name.  A clusterName is of the form
 * projects/(projectId)/zones/(zoneId)/clusters/(clusterId).  It also has convenience methods
 * to create a tableName and a tableId.  TableName is (clusterName)/tables/(tableId).
 */
public class BigtableClusterName {
  public static final String BIGTABLE_V1_CLUSTER_FMT = "projects/%s/zones/%s/clusters/%s";
  public static final String TABLE_SEPARATOR = "tables";

  private final String clusterName;

  public BigtableClusterName(String projectId, String zone, String clusterName) {
    this.clusterName = String.format(BIGTABLE_V1_CLUSTER_FMT, projectId, zone, clusterName);
  }

  /**
   * Get the cluster name.
   */
  @Override
  public String toString() {
    return clusterName;
  }

  /**
   * Transforms a tableName within this cluster of the form
   * projects/(projectId)/zones/(zoneId)/clusters/(clusterId)/tables/(tableId) to (tableId).
   */
  public String toTableId(String tableName) {
    Preconditions.checkNotNull(tableName, "Table name cannot be null");
    String tablesPrefix = clusterName + "/" + TABLE_SEPARATOR + "/";
    Preconditions.checkState(tableName.startsWith(tablesPrefix),
        "'%s' does not start with '%s'", tableName, tablesPrefix);
    String tableId = tableName.substring(tablesPrefix.length()).trim();
    Preconditions.checkState(!tableId.isEmpty(), "Table id is blank");
    return tableId;
  }

  public String toTableNameStr(String tableId) {
    return clusterName +  "/" + TABLE_SEPARATOR + "/" + tableId;
  }

  public BigtableTableName toTableName(String tableId) {
    return new BigtableTableName(toTableNameStr(tableId));
  }
}
