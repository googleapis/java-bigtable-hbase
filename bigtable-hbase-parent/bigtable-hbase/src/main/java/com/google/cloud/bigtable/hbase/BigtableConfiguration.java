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
package com.google.cloud.bigtable.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;

/**
 * This class provides a simplified mechanism of creating a programmatic Bigtable Connection.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableConfiguration {
  private static final String[] CONNECTION_CLASS_NAMES = {
    "com.google.cloud.bigtable.hbase1_0.BigtableConnection",
    "com.google.cloud.bigtable.hbase1_1.BigtableConnection",
    "com.google.cloud.bigtable.hbase1_2.BigtableConnection",
  };

  private static final Class<? extends Connection> CONNECTION_CLASS = chooseConnectionClass();
  
  @SuppressWarnings("unchecked")
  private static Class<? extends Connection> chooseConnectionClass() {
    for (String className : CONNECTION_CLASS_NAMES) {
      try {
        return (Class<? extends Connection>) Class.forName(className);
      } catch (ClassNotFoundException ignored) {
        // This class is not on the classpath, so move on to the next className.
      }
    }
    return null;
  }

  /**
   * @return the default bigtable {@link Connection} implementation class found in the classpath.
   */
  public static Class<? extends Connection> getConnectionClass() {
    return CONNECTION_CLASS;
  }

  /**
   * <p>configure.</p>
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static Configuration configure(String projectId, String instanceId) {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.PROJECT_ID_KEY, projectId);
    config.set(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId);
    return config;
  }

  /**
   * <p>connect.</p>
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.hbase.client.Connection} object.
   */
  public static Connection connect(String projectId, String instanceId) {
    return connect(configure(projectId, instanceId));
  }

  /**
   * <p>connect.</p>
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object.
   * @return a {@link org.apache.hadoop.hbase.client.Connection} object.
   */
  public static Connection connect(Configuration conf) {
    Preconditions.checkState(CONNECTION_CLASS != null,
        "Could not find an appropriate BigtableConnection class");
    try {
      return CONNECTION_CLASS.getConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e) {
      throw new IllegalStateException("Could not find an appropriate constructor for "
          + CONNECTION_CLASS.getCanonicalName(), e);
    }
  }
}
