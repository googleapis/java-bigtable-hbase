/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;

public interface CommonConnection {

  /**
   * <p>Getter for the field <code>session</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableSession} object.
   */
  BigtableSession getSession() throws IOException;

  /**
   * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
   *
   * The reference returned is not a copy, so any change made to it will affect this instance.
   */
  Configuration getConfiguration() throws IOException;

  /**
   * <p>Getter for the field <code>options</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  BigtableOptions getOptions() throws IOException;

  /**
   * <p>Getter for the field <code>disabledTables</code>.</p>
   *
   * @return a {@link java.util.Set} object.
   */
  Set<TableName> getDisabledTables() throws IOException;

}
