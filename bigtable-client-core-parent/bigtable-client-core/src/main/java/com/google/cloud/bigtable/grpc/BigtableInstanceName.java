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
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableInstanceName implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Constant <code>BIGTABLE_V2_INSTANCE_FMT="projects/%s/instances/%s"</code> */
  public static final String BIGTABLE_V2_INSTANCE_FMT = "projects/%s/instances/%s";
  /** Constant <code>TABLE_SEPARATOR="/tables/"</code> */
  public static final String TABLE_SEPARATOR = "/tables/";

  private final String instanceName;

  private final String projectId;
  private final String instanceId;

  /**
   * <p>Constructor for BigtableInstanceName.</p>
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   */
  public BigtableInstanceName(String projectId, String instanceId) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId), "projectId must be supplied");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(instanceId), "instanceId must be supplied");
    this.instanceName = String.format(BIGTABLE_V2_INSTANCE_FMT, projectId, instanceId);
    this.projectId = projectId;
    this.instanceId = instanceId;
  }

  /**
   * {@inheritDoc}
   *
   * Get the instance name.
   */
  @Override
  public String toString() {
    return instanceName;
  }

  /**
   * Transforms a tableName within this instance of the form
   * projects/(projectId)/instances/(instanceId)/tables/(tableId) to (tableId).
   *
   * @param tableName a {@link java.lang.String} object.
   * @return a {@link java.lang.String} object.
   */
  public String toTableId(String tableName) {
    Preconditions.checkNotNull(tableName, "Table name cannot be null");
    String tablesPrefix = instanceName + TABLE_SEPARATOR;
    Preconditions.checkState(tableName.startsWith(tablesPrefix),
        "'%s' does not start with '%s'", tableName, tablesPrefix);
    String tableId = tableName.substring(tablesPrefix.length()).trim();
    Preconditions.checkState(!tableId.isEmpty(), "Table id is blank");
    return tableId;
  }


  /**
   * <p>toTableNameStr.</p>
   *
   * @param tableId a {@link java.lang.String} object.
   * @return a {@link java.lang.String} object.
   */
  public String toTableNameStr(String tableId) {
    return instanceName +  TABLE_SEPARATOR + tableId;
  }

  /**
   * <p>toTableName.</p>
   *
   * @param tableId a {@link java.lang.String} object.
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   */
  public BigtableTableName toTableName(String tableId) {
    return new BigtableTableName(toTableNameStr(tableId));
  }

  /** @return the projectId */
  public String getProjectId() {
    return projectId;
  }

  /** @return the instanceId */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * @return the fully qualified instanceName with the form
   *         'projects/{projectId}/instances/{instanceId}'.
   */
  public String getInstanceName() {
    return instanceName;
  }
}

