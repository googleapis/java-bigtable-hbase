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
package com.google.cloud.bigtable.grpc;

import java.io.Serializable;

import com.google.api.client.util.Strings;
import com.google.common.base.Preconditions;

/**
 * This class encapsulates a Bigtable instance name.  An instance name is of the form
 * projects/(projectId)/instances/(instanceId).  It also has convenience methods
 * to create a tableName and a tableId.  TableName is (instanceName)/tables/(tableId).
 */
public class BigtableInstanceName implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String BIGTABLE_V2_INSTANCE_FMT = "projects/%s/instances/%s";
  public static final String TABLE_SEPARATOR = "tables";

  private final String instanceName;

  public BigtableInstanceName(String projectId, String instanceId) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId), "projectId must be supplied");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId), "instanceId must be supplied");
    this.instanceName = String.format(BIGTABLE_V2_INSTANCE_FMT, projectId, instanceId);
  }

  /**
   * Get the instance name.
   */
  @Override
  public String toString() {
    return instanceName;
  }

  /**
   * Transforms a tableName within this instance of the form
   * projects/(projectId)/instances/(instanceId)/tables/(tableId) to (tableId).
   */
  public String toTableId(String tableName) {
    Preconditions.checkNotNull(tableName, "Table name cannot be null");
    String tablesPrefix = instanceName + "/" + TABLE_SEPARATOR + "/";
    Preconditions.checkState(tableName.startsWith(tablesPrefix),
        "'%s' does not start with '%s'", tableName, tablesPrefix);
    String tableId = tableName.substring(tablesPrefix.length()).trim();
    Preconditions.checkState(!tableId.isEmpty(), "Table id is blank");
    return tableId;
  }

  public String toTableNameStr(String tableId) {
    return instanceName +  "/" + TABLE_SEPARATOR + "/" + tableId;
  }

  public BigtableTableName toTableName(String tableId) {
    return new BigtableTableName(toTableNameStr(tableId));
  }
}