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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class MirroringConfigurationHelper {
  /**
   * Key to set to a name of Connection class that should be used to connect to primary database. It
   * is used as hbase.client.connection.impl when creating connection to primary database. Set to
   * {@code default} to use default HBase connection class.
   */
  public static final String MIRRORING_PRIMARY_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.primary-client.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect to secondary database.
   * It is used as hbase.client.connection.impl when creating connection to secondary database. Set
   * to an {@code default} to use default HBase connection class.
   */
  public static final String MIRRORING_SECONDARY_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.secondary-client.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect asynchronously to
   * primary database. It is used as hbase.client.async.connection.impl when creating connection to
   * primary database. Set to {@code default} to use default HBase connection class.
   */
  public static final String MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.primary-client.async.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect asynchronously to
   * secondary database. It is used as hbase.client.async.connection.impl when creating connection
   * to secondary database. Set to {@code default} to use default HBase connection class.
   */
  public static final String MIRRORING_SECONDARY_ASYNC_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.secondary-client.async.connection.impl";

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

  public static final String MIRRORING_MISMATCH_DETECTOR_CLASS =
      "google.bigtable.mirroring.mismatch-detector.impl";

  public static final String MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS =
      "google.bigtable.mirroring.flow-controller.impl";

  public static final String MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS =
      "google.bigtable.mirroring.flow-controller.max-outstanding-requests";
  public static final String MIRRORING_FLOW_CONTROLLER_MAX_USED_BYTES =
      "google.bigtable.mirroring.flow-controller.max-used-bytes";

  public static final String MIRRORING_WRITE_ERROR_CONSUMER_CLASS =
      "google.bigtable.mirroring.write-error-consumer.impl";

  public static final String MIRRORING_WRITE_ERROR_LOG_APPENDER_CLASS =
      "google.bigtable.mirroring.write-error-log.appender.impl";
  public static final String MIRRORING_WRITE_ERROR_LOG_SERIALIZER_CLASS =
      "google.bigtable.mirroring.write-error-log.serializer.impl";

  /**
   * Integer value representing percentage of read operations performed on primary database that
   * should be verified against secondary. Each call to {@link Table#get(Get)}, {@link
   * Table#get(List)}, {@link Table#exists(Get)}, {@link Table#existsAll(List)} {@link
   * Table#batch(List, Object[])} (with overloads) and {@link Table#getScanner(Scan)} (with
   * overloads) is counted as a single operation, independent of size of their arguments and
   * results.
   *
   * <p>Correct values are a integers ranging from 0 to 100 inclusive.
   */
  public static final String MIRRORING_READ_VERIFICATION_RATE_PERCENT =
      "google.bigtable.mirroring.read-verification-rate-percent";

  /**
   * Number of bytes that {@link MirroringBufferedMutator} should buffer before flushing underlying
   * primary BufferedMutator and performing a write to secondary database.
   *
   * <p>If not set uses the value of {@code hbase.client.write.buffer}, which by default is 2MB.
   * When those values are kept in sync, mirroring client should perform flush operation on primary
   * BufferedMutator right after it schedules a new asynchronous write to the database.
   */
  public static final String MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH =
      "google.bigtable.mirroring.buffered-mutator.bytes-to-flush";

  /**
   * When set to {@code true} writes to primary and secondary databases will be performed
   * concurrently. This option reduces write-latency to secondary database, but can cause additional
   * inconsistency - some writes might complete successfully on secondary database and fail on
   * primary database. Defaults to {@code false}.
   */
  public static final String MIRRORING_CONCURRENT_WRITES =
      "google.bigtable.mirroring.concurrent-writes";

  /**
   * When set to {@code true} mirroring client will wait for operations to be performed on secondary
   * database before returning to the user. In this mode exceptions thrown by mirroring operations
   * reflect errors that happened on one of the databases. Types of thrown exceptions are not
   * changed, but a {@link com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException}
   * is added as a root cause for thrown exceptions (For more details see {@link
   * com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException}).
   *
   * <p>Defaults to {@code false}.
   */
  public static final String MIRRORING_SYNCHRONOUS_WRITES =
      "google.bigtable.mirroring.synchronous-writes";

  public static void fillConnectionConfigWithClassImplementation(
      Configuration connectionConfig,
      Configuration config,
      String connectionClassKey,
      String connectionConfigImplementationKey) {
    String connectionClassName = config.get(connectionClassKey);
    if (!connectionClassName.equalsIgnoreCase("default")) {
      connectionConfig.set(connectionConfigImplementationKey, connectionClassName);
    } else {
      connectionConfig.unset(connectionConfigImplementationKey);
    }
  }

  public static void checkParameters(
      Configuration conf, String primaryConnectionClassKey, String secondaryConnectionClassKey) {
    String primaryConnectionClassName = conf.get(primaryConnectionClassKey);
    String secondaryConnectionClassName = conf.get(secondaryConnectionClassKey);
    String primaryConnectionConfigPrefix = conf.get(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "");
    String secondaryConnectionConfigPrefix = conf.get(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "");

    checkArgument(
        primaryConnectionClassName != null,
        String.format("Specify %s.", primaryConnectionClassKey));
    checkArgument(
        secondaryConnectionClassName != null,
        String.format("Specify %s.", secondaryConnectionClassKey));

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
                "Values of %s and %s should be different. Prefixes are used to differentiate "
                    + "between primary and secondary configurations. If you want to use the same "
                    + "configuration for both databases then you shouldn't use prefixes at all.",
                MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, MIRRORING_SECONDARY_CONFIG_PREFIX_KEY));
      }
    }
  }

  public static Configuration extractPrefixedConfig(String prefixKey, Configuration conf) {
    String prefix = conf.get(prefixKey, "");
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
