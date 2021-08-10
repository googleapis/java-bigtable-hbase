/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;

public class MirroringConfiguration extends Configuration {
  Configuration primaryConfiguration;
  Configuration secondaryConfiguration;
  MirroringOptions mirroringOptions;

  /**
   * Key to set to a name of Connection class that should be used to connect to primary database. It
   * is used as hbase.client.connection.impl when creating connection to primary database.
   */
  public static final String MIRRORING_PRIMARY_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.primary-client.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect to secondary database.
   * It is used as hbase.client.connection.impl when creating connection to secondary database.
   */
  public static final String MIRRORING_SECONDARY_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.secondary-client.connection.impl";

  /**
   * By default all parameters from the Configuration object passed to
   * ConnectionFactory#createConnection are passed to Connection instances. If this key is set, then
   * only parameters that start with given prefix are passed to primary Connection. Use it if
   * primary and secondary connections' configurations share a key that should have different value
   * passed to each of connections, e.g. zookeeper url.
   *
   * <p>Prefixes should not contain dot at the end.
   */
  public static final String MIRRORING_PRIMARY_CONFIG_PREFIX_KEY =
      "google.bigtable.mirroring.primary-client.prefix";

  /**
   * If this key is set, then only parameters that start with given prefix are passed to secondary
   * Connection.
   */
  public static final String MIRRORING_SECONDARY_CONFIG_PREFIX_KEY =
      "google.bigtable.mirroring.secondary-client.prefix";

  public MirroringConfiguration(
      Configuration primaryConfiguration,
      Configuration secondaryConfiguration,
      Configuration mirroringConfiguration) {
    super.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    this.primaryConfiguration = primaryConfiguration;
    this.secondaryConfiguration = secondaryConfiguration;
    this.mirroringOptions = new MirroringOptions(mirroringConfiguration);
  }

  public MirroringConfiguration(Configuration conf) {
    super(conf); // Copy-constructor
    // In case the user constructed MirroringConfiguration by hand.
    if (conf instanceof MirroringConfiguration) {
      MirroringConfiguration mirroringConfiguration = (MirroringConfiguration) conf;
      this.primaryConfiguration = new Configuration(mirroringConfiguration.primaryConfiguration);
      this.secondaryConfiguration =
          new Configuration(mirroringConfiguration.secondaryConfiguration);
      this.mirroringOptions = mirroringConfiguration.mirroringOptions;
    } else {
      checkParameters(conf);
      this.primaryConfiguration = constructPrimaryConfiguration(conf);
      this.secondaryConfiguration = constructSecondaryConfiguration(conf);
      this.mirroringOptions = new MirroringOptions(conf);
    }
  }

  private Configuration constructPrimaryConfiguration(Configuration conf) {
    return constructConnectionConfiguration(
        conf, MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, MIRRORING_PRIMARY_CONFIG_PREFIX_KEY);
  }

  private Configuration constructSecondaryConfiguration(Configuration conf) {
    return constructConnectionConfiguration(
        conf, MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, MIRRORING_SECONDARY_CONFIG_PREFIX_KEY);
  }

  private Configuration constructConnectionConfiguration(
      Configuration conf, String connectionClassKey, String prefixKey) {
    String connectionClassName = conf.get(connectionClassKey);
    String prefix = conf.get(prefixKey, "");
    Configuration connectionConfig = extractPrefixedConfig(prefix, conf);
    connectionConfig.set("hbase.client.connection.impl", connectionClassName);
    return connectionConfig;
  }

  private static void checkParameters(Configuration conf) {
    String primaryConnectionClassName = conf.get(MIRRORING_PRIMARY_CONNECTION_CLASS_KEY);
    String secondaryConnectionClassName = conf.get(MIRRORING_SECONDARY_CONNECTION_CLASS_KEY);
    String primaryConnectionConfigPrefix = conf.get(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "");
    String secondaryConnectionConfigPrefix = conf.get(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "");

    checkArgument(
        primaryConnectionClassName != null,
        String.format("Specify %s.", MIRRORING_PRIMARY_CONNECTION_CLASS_KEY));
    checkArgument(
        secondaryConnectionClassName != null,
        String.format("Specify %s.", MIRRORING_SECONDARY_CONNECTION_CLASS_KEY));

    if (Objects.equals(primaryConnectionClassName, secondaryConnectionClassName)
        && Objects.equals(primaryConnectionConfigPrefix, secondaryConnectionConfigPrefix)) {
      if (primaryConnectionConfigPrefix.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Mirroring connections using the same client class requires a separate "
                    + "configuration for one of them. Specify either %s or %s and use its value "
                    + "as a prefix for configuration options.",
                MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, MIRRORING_SECONDARY_CONFIG_PREFIX_KEY));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Values of %s and %s should be different.",
                MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, MIRRORING_SECONDARY_CONFIG_PREFIX_KEY));
      }
    }
  }

  private static Configuration extractPrefixedConfig(String prefix, Configuration conf) {
    if (prefix.isEmpty()) {
      return new Configuration(conf);
    }

    return stripPrefixFromConfiguration(prefix, conf);
  }

  private static Configuration stripPrefixFromConfiguration(String prefix, Configuration config) {
    Map<String, String> matchingConfigs =
        config.getValByRegex("^" + Pattern.quote(prefix) + "\\..*");
    Configuration newConfig = new Configuration(false);
    for (Map.Entry<String, String> entry : matchingConfigs.entrySet()) {
      newConfig.set(entry.getKey().substring(prefix.length() + 1), entry.getValue());
    }
    return newConfig;
  }
}
