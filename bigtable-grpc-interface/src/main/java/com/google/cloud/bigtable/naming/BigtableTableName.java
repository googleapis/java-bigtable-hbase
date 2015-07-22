/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.naming;

import com.google.cloud.bigtable.config.BigtableOptions;

/**
 * Utility class that will set given project, cluster and table name within
 * service messages.
 */
public class BigtableTableName {
  public static final String TABLE_SEPARATOR = "tables";
  public static final String BIGTABLE_V1_TABLENAME_FMT =
      BigtableClusterName.BIGTABLE_V1_CLUSTER_FMT + "/" + TABLE_SEPARATOR + "/%s";

  public static BigtableTableName from(String tableName, BigtableOptions options) {
    return new BigtableTableName(getName(options.getProjectId(), options.getZone(),
      options.getCluster(), tableName));
  }

  public static String getName(String projectId, String zone, String clusterName,
      String tableName) {
    return String.format(BIGTABLE_V1_TABLENAME_FMT,
      projectId,
      zone,
      clusterName,
      tableName);
  }

  private final String name;

  public BigtableTableName(String formattedV1TableName) {
    this.name = formattedV1TableName;
  }

  public String getFullName() {
    return name;
  }
}
