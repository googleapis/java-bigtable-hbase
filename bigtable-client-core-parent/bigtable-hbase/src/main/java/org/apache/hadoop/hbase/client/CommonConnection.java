/*
 * Copyright 2018 Google Inc.
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
package org.apache.hadoop.hbase.client;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;

/**
 * Common interface for {@link AbstractBigtableConnection} and HBase 2's BigtableAsyncConnection.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface CommonConnection extends Closeable {

  /** Returns {@link BigtableApi} object to access bigtable data and admin client APIs. */
  BigtableApi getBigtableApi();

  /**
   * Returns the {@link Configuration} object used by this instance. The reference returned is not a
   * copy, so any change made to it will affect this instance.
   *
   * @return a {@link Configuration} object.
   */
  Configuration getConfiguration();

  /**
   * Returns instance of bigtable settings for classic or veneer client.
   *
   * @return a {@link BigtableHBaseSettings} instance.
   */
  BigtableHBaseSettings getBigtableSettings();

  /**
   * Getter for the field <code>disabledTables</code>.
   *
   * @return a {@link Set} object that are disabled.
   */
  Set<TableName> getDisabledTables();

  /**
   * Retrieve a region information on a table.
   *
   * @param tableName Name of the table for which to return region info.
   * @return A {@link java.util.List} HRegionInfo object
   * @throws java.io.IOException if any.
   */
  List<HRegionInfo> getAllRegionInfos(TableName tableName) throws IOException;
}
