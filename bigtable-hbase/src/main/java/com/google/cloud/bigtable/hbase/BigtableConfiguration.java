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
 */
public class BigtableConfiguration {
  private static final String[] CONNECTION_CLASS_NAMES = {
    "com.google.cloud.bigtable.hbase1_0.BigtableConnection",
    "com.google.cloud.bigtable.hbase1_1.BigtableConnection",
    "com.google.cloud.bigtable.hbase1_2.BigtableConnection",
  };

  private static final Class<? extends Connection> CONNECTION_CLASS = getConnectionClass();
  
  @SuppressWarnings("unchecked")
  private static Class<? extends Connection> getConnectionClass() {
    for (String className : CONNECTION_CLASS_NAMES) {
      try {
        return (Class<? extends Connection>) Class.forName(className);
      } catch (ClassNotFoundException ignored) {
        // This class is not on the classpath, so move on to the next className.
      }
    }
    return null;
  }

  public static Configuration configure(String projectId, String zoneName, String clusterName) {
    Configuration config = new Configuration(false);
    config.set(BigtableOptionsFactory.PROJECT_ID_KEY, projectId);
    config.set(BigtableOptionsFactory.ZONE_KEY, zoneName);
    config.set(BigtableOptionsFactory.CLUSTER_KEY, clusterName);
    return config;
  }

  public static Connection connect(String projectId, String zoneName, String clusterName) {
    return connect(configure(projectId, zoneName, clusterName));
  }

  public static Connection connect(Configuration conf) {
    Preconditions.checkState(CONNECTION_CLASS != null,
        "Could not find an appropriate BigtableConnection class");
    try {
      return CONNECTION_CLASS.getConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e) {
      throw new IllegalStateException("Could not find an apporpriate constructor for "
          + CONNECTION_CLASS.getCanonicalName(), e);
    }
  }
}
