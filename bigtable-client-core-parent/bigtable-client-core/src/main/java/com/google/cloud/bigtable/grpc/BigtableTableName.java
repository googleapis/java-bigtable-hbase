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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/**
 * This class encapsulates a tableName.  A tableName is of the form
 * projects/(projectId)/zones/(zoneId)/clusters/(clusterId)/tables/(tableId).
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableTableName {
  // Use a very loose pattern so we don't validate more strictly than the server.
  private static final Pattern PATTERN =
      Pattern.compile("projects/[^/]+/instances/([^/]+)/tables/([^/]+)");

  private final String tableName;
  private final String instanceId;
  private final String tableId;

  public BigtableTableName(String tableName) {
    this.tableName = tableName;
    Matcher matcher = PATTERN.matcher(tableName);
    Preconditions.checkArgument(matcher.matches(), "Malformed snapshot name");
    this.instanceId = matcher.group(1);
    this.tableId = matcher.group(2);
  }

  /**
   * @return The id of the instance that contains this table. It's the second group in the table name
   *         name: "projects/{projectId}/instances/{instanceId}/tables/{tableId}".
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * @return The id of the table. It's the third group in the table name
   *         name: "projects/{projectId}/instances/{instanceId}/tables/{tableId}".
   */
  public String getTableId() {
    return tableId;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return tableName;
  }
}
