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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.VersionInfo;

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

  // The value of this field can implement 2 different interfaces depending on the HBase version
  // For 2.0 - 2.2 the value must implement AsyncRegistry
  // For 2.3 onwards the value must implement ConnectionRegistry
  // bigable-hbase-2x contains implementations for both, this helper will use classpath probing to
  // guess the correct impl.
  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final String HBASE_CLIENT_ASYNC_REGISTRY_IMPL = "hbase.client.registry.impl";

  /**
   * For internal use only - public for technical reasons.
   *
   * @return the default bigtable {@link Connection} implementation class found in the classpath.
   */
  @SuppressWarnings("unchecked")
  @InternalApi("For internal usage only")
  public static Class<? extends Connection> getConnectionClass() {
    String hbaseVersion = VersionInfo.getVersion();

    final String connectionClassName;

    if (VersionInfo.compareVersion(hbaseVersion, "2.0") >= 0) {
      connectionClassName = "com.google.cloud.bigtable.hbase2_x.BigtableConnection";
    } else {
      connectionClassName = "com.google.cloud.bigtable.hbase1_x.BigtableConnection";
    }
    try {
      return (Class<? extends Connection>) Class.forName(connectionClassName);
    } catch (Throwable t) {
      throw new IllegalStateException(
          "Failed to load Bigtable connection adapter for HBase version " + hbaseVersion, t);
    }
  }

  private static Class<?> getConnectionRegistryClass() {
    String hbaseVersion = VersionInfo.getVersion();

    final String registryClassName;

    if (VersionInfo.compareVersion(hbaseVersion, "2.3") >= 0) {
      registryClassName = "org.apache.hadoop.hbase.client.BigtableConnectionRegistry";
    } else if (VersionInfo.compareVersion(hbaseVersion, "2.0") >= 0) {
      registryClassName = "org.apache.hadoop.hbase.client.BigtableAsyncRegistry";
    } else {
      registryClassName = null;
    }

    if (registryClassName != null) {
      try {
        return Class.forName(registryClassName);
      } catch (Throwable e) {
        throw new IllegalStateException(
            "Failed to load the Bigtable Async/ConnectionRegistry adapter for Hbase version "
                + hbaseVersion,
            e);
      }
    }
    return null;
  }

  private static Class<?> getAsyncConnectionClass() {
    try {
      // Make sure HBase interface is present
      Class.forName("org.apache.hadoop.hbase.client.AsyncConnection");
    } catch (ClassNotFoundException e) {
      return null;
    }

    try {
      // then try loading our implementation
      return Class.forName("org.apache.hadoop.hbase.client.BigtableAsyncConnection");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /**
   * Set up connection impl classes. If Bigtable and HBase async connection classes exist, set up
   * async connection impl class as well.
   */
  private static Configuration injectBigtableImpls(Configuration configuration) {
    // HBase 1x & 2x sync connection
    configuration.set(HBASE_CLIENT_CONNECTION_IMPL, getConnectionClass().getCanonicalName());

    // HBase 2x async
    Class<?> asyncConnectionClass = getAsyncConnectionClass();
    Class<?> connectionRegistryClass = getConnectionRegistryClass();

    if (asyncConnectionClass != null && connectionRegistryClass != null) {
      configuration.set(HBASE_CLIENT_ASYNC_CONNECTION_IMPL, asyncConnectionClass.getName());
      configuration.set(HBASE_CLIENT_ASYNC_REGISTRY_IMPL, connectionRegistryClass.getName());
    }

    return configuration;
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
    return injectBigtableImpls(conf);
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
    return injectBigtableImpls(conf);
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
          "Could not find an appropriate constructor for "
              + getConnectionClass().getCanonicalName(),
          e);
    }
  }
}
