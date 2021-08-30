/*
 * Copyright 2015 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.api.core.InternalExtensionOnly;
import com.google.auth.Credentials;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/** This class provides a simplified mechanism of creating a programmatic Bigtable Connection. */
@InternalExtensionOnly
public class BigtableConfiguration {
  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String HBASE_CLIENT_ASYNC_CONNECTION_IMPL =
      "hbase.client.async.connection.impl";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String HBASE_CLIENT_ASYNC_REGISTRY_IMPL = "hbase.client.registry.impl";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_HBASE_CLIENT_ASYNC_CONNECTION_CLASS =
      "org.apache.hadoop.hbase.client.BigtableAsyncConnection";

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String BIGTABLE_HBASE_CLIENT_ASYNC_REGISTRY_CLASS =
      "org.apache.hadoop.hbase.client.BigtableAsyncRegistry";

  private static final String[] CONNECTION_CLASS_NAMES = {
    "com.google.cloud.bigtable.hbase1_x.BigtableConnection",
    "com.google.cloud.bigtable.hbase2_x.BigtableConnection",
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
   * For internal use only - public for technical reasons.
   *
   * @return the default bigtable {@link Connection} implementation class found in the classpath.
   */
  @InternalApi("For internal usage only")
  public static Class<? extends Connection> getConnectionClass() {
    Preconditions.checkState(
        CONNECTION_CLASS != null,
        "Could not load a concrete implementation of BigtableTableConnection: "
            + "failed to find bigtable-hbase-1.x on the classpath.");
    return CONNECTION_CLASS;
  }

  /**
   * Create and configure a new {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static Configuration configure(String projectId, String instanceId) {
    Configuration config = new Configuration(false);
    return configure(config, projectId, instanceId);
  }

  /**
   * Create and configure a new {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @param appProfileId a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static Configuration configure(String projectId, String instanceId, String appProfileId) {
    Configuration config = new Configuration(false);
    return configure(config, projectId, instanceId, appProfileId);
  }

  /**
   * Configure and return an existing {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object to configure.
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @return the modified {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static Configuration configure(Configuration conf, String projectId, String instanceId) {
    conf.set(BigtableOptionsFactory.PROJECT_ID_KEY, projectId);
    conf.set(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId);
    conf.set(HBASE_CLIENT_CONNECTION_IMPL, getConnectionClass().getCanonicalName());
    return conf;
  }

  /**
   * Configure and return an existing {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object to configure.
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @param appProfileId a {@link java.lang.String} object.
   * @return the modified {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static Configuration configure(
      Configuration conf, String projectId, String instanceId, String appProfileId) {
    conf.set(BigtableOptionsFactory.PROJECT_ID_KEY, projectId);
    conf.set(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId);
    conf.set(BigtableOptionsFactory.APP_PROFILE_ID_KEY, appProfileId);
    conf.set(HBASE_CLIENT_CONNECTION_IMPL, getConnectionClass().getCanonicalName());
    return conf;
  }

  /**
   * Sets a reference to a {@link Credentials} in a {@link Configuration} object.
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object to configure.
   * @param credentials a {@link Credentials} object;
   * @return a {@link Configuration} object.
   */
  public static Configuration withCredentials(Configuration conf, Credentials credentials) {
    return new BigtableExtendedConfiguration(conf, credentials);
  }

  /**
   * Configuration for getting a org.apache.hadoop.hbase.client.AsyncConnection.
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object to configure.
   * @return the modified {@link org.apache.hadoop.conf.Configuration} object.
   */
  public static Configuration asyncConfigure(Configuration conf) {
    conf.set(HBASE_CLIENT_ASYNC_CONNECTION_IMPL, BIGTABLE_HBASE_CLIENT_ASYNC_CONNECTION_CLASS);
    conf.set(HBASE_CLIENT_ASYNC_REGISTRY_IMPL, BIGTABLE_HBASE_CLIENT_ASYNC_REGISTRY_CLASS);
    return conf;
  }

  /**
   * connect.
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.hbase.client.Connection} object.
   */
  public static Connection connect(String projectId, String instanceId) {
    return connect(configure(projectId, instanceId));
  }

  /**
   * connect.
   *
   * @param projectId a {@link java.lang.String} object.
   * @param instanceId a {@link java.lang.String} object.
   * @param appProfileId a {@link java.lang.String} object.
   * @return a {@link org.apache.hadoop.hbase.client.Connection} object.
   */
  public static Connection connect(String projectId, String instanceId, String appProfileId) {
    return connect(configure(projectId, instanceId, appProfileId));
  }

  /**
   * connect.
   *
   * @param conf a {@link org.apache.hadoop.conf.Configuration} object.
   * @return a {@link org.apache.hadoop.hbase.client.Connection} object.
   */
  public static Connection connect(Configuration conf) {
    Class<? extends Connection> connectionClass = getConnectionClass();
    try {
      return connectionClass.getConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Could not find an appropriate constructor for " + CONNECTION_CLASS.getCanonicalName(),
          e);
    }
  }
}
