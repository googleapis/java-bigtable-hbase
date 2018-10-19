/*
 * Copyright 2018 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;

/**
 * Common interface for BigtableConnection & BigtableAsyncConnection
 */
public interface CommonConnection extends Closeable {
  
  /**
   * <p>
   * Getter for the field <code>session</code>.
   * </p>
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableSession} object.
   */
  BigtableSession getSession();

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance. The
   * reference returned is not a copy, so any change made to it will affect this instance.
   */
  Configuration getConfiguration();

  /**
   * <p>
   * Getter for the field <code>options</code>.
   * </p>
   * @return a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  BigtableOptions getOptions();

  /**
   * <p>
   * Getter for the field <code>disabledTables</code>.
   * </p>
   * @return a {@link java.util.Set} object.
   */
  Set<TableName> getDisabledTables();

  /**
   * Retrieve a region information on a table.
   * @param tableName Name of the table who's region is to be examined
   * @return A {@link java.util.List} HRegionInfo object
   */
  List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException;
}