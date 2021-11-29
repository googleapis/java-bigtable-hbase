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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringResultScanner;
import com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutator;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Appender;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.DefaultAppender;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.DefaultSerializer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Serializer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestCountingFlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector;
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
   *
   * <p>Required.
   */
  public static final String MIRRORING_PRIMARY_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.primary-client.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect to secondary database.
   * It is used as hbase.client.connection.impl when creating connection to secondary database. Set
   * to an {@code default} to use default HBase connection class.
   *
   * <p>Required.
   */
  public static final String MIRRORING_SECONDARY_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.secondary-client.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect asynchronously to
   * primary database. It is used as hbase.client.async.connection.impl when creating connection to
   * primary database. Set to {@code default} to use default HBase connection class.
   *
   * <p>Required when using HBase 2.x.
   */
  public static final String MIRRORING_PRIMARY_ASYNC_CONNECTION_CLASS_KEY =
      "google.bigtable.mirroring.primary-client.async.connection.impl";

  /**
   * Key to set to a name of Connection class that should be used to connect asynchronously to
   * secondary database. It is used as hbase.client.async.connection.impl when creating connection
   * to secondary database. Set to {@code default} to use default HBase connection class.
   *
   * <p>Required when using HBase 2.x.
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
   *
   * <p>default: empty
   */
  public static final String MIRRORING_PRIMARY_CONFIG_PREFIX_KEY =
      "google.bigtable.mirroring.primary-client.prefix";

  /**
   * If this key is set, then only parameters that start with given prefix are passed to secondary
   * Connection.
   *
   * <p>default: empty
   */
  public static final String MIRRORING_SECONDARY_CONFIG_PREFIX_KEY =
      "google.bigtable.mirroring.secondary-client.prefix";

  /**
   * Path to {@link
   * com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector.Factory} of
   * MismatchDetector.
   *
   * <p>default: {@link DefaultMismatchDetector.Factory}, logs detected mismatches to stdout and
   * reports them as OpenCensus metrics.
   */
  public static final String MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS =
      "google.bigtable.mirroring.mismatch-detector.factory-impl";

  /**
   * Path to class to be used as FlowControllerStrategy.
   *
   * <p>default: {@link RequestCountingFlowControlStrategy}.
   */
  public static final String MIRRORING_FLOW_CONTROLLER_STRATEGY_FACTORY_CLASS =
      "google.bigtable.mirroring.flow-controller.factory-impl";

  /**
   * Maximal number of outstanding secondary database requests before throttling requests to primary
   * database.
   *
   * <p>default: 500.
   */
  public static final String MIRRORING_FLOW_CONTROLLER_STRATEGY_MAX_OUTSTANDING_REQUESTS =
      "google.bigtable.mirroring.flow-controller-strategy.max-outstanding-requests";

  /**
   * Maximal number of bytes used by internal buffers for asynchronous requests before throttling
   * requests to primary database.
   *
   * <p>default: 256MB.
   */
  public static final String MIRRORING_FLOW_CONTROLLER_STRATEGY_MAX_USED_BYTES =
      "google.bigtable.mirroring.flow-controller-strategy.max-used-bytes";

  /**
   * Integer value representing how many first bytes of binary values (such as row) should be
   * converted to hex and then logged in case of error.
   *
   * <p>default: 32.
   */
  public static final String MIRRORING_WRITE_ERROR_LOG_MAX_BINARY_VALUE_LENGTH =
      "google.bigtable.mirroring.write-error-log.max-binary-value-bytes-logged";

  /**
   * Path to class to be used as a {@link SecondaryWriteErrorConsumer.Factory} for consumer of
   * secondary database write errors.
   *
   * <p>default: {@link DefaultSecondaryWriteErrorConsumer.Factory}, forwards errors to faillog
   * using Appender and Serializer.
   */
  public static final String MIRRORING_WRITE_ERROR_CONSUMER_FACTORY_CLASS =
      "google.bigtable.mirroring.write-error-consumer.factory-impl";

  /**
   * Faillog Appender {@link Appender.Factory} implementation.
   *
   * <p>default: {@link DefaultAppender.Factory}, writes data serialized by Serializer
   * implementation to file on disk.
   */
  public static final String MIRRORING_WRITE_ERROR_LOG_APPENDER_FACTORY_CLASS =
      "google.bigtable.mirroring.write-error-log.appender.factory-impl";

  /**
   * Faillog {@link Serializer.Factory} implementation, responsible for serializing write errors
   * reported by the Logger to binary representation, which is later appended to resulting file by
   * the {@link Appender}.
   *
   * <p>default: {@link DefaultSerializer}, dumps supplied mutation along with error stacktrace as
   * JSON.
   */
  public static final String MIRRORING_WRITE_ERROR_LOG_SERIALIZER_FACTORY_CLASS =
      "google.bigtable.mirroring.write-error-log.serializer.factory-impl";

  /**
   * Integer value representing percentage of read operations performed on primary database that
   * should be verified against secondary. Each call to {@link Table#get(Get)}, {@link
   * Table#get(List)}, {@link Table#exists(Get)}, {@link Table#existsAll(List)} {@link
   * Table#batch(List, Object[])} (with overloads) and {@link Table#getScanner(Scan)} (with
   * overloads) is counted as a single operation, independent of size of their arguments and
   * results.
   *
   * <p>Correct values are a integers ranging from 0 to 100 inclusive.
   *
   * <p>default: 100
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

  /**
   * Determines the path prefix used for generating the failed mutations log file names.
   *
   * <p>In default mode secondary mutations are executed asynchronously, so their status is not
   * reported to the user. Instead, they are logged to a failed mutation log, which can be inspected
   * manually, collected or read programatically to retry the mutations.
   *
   * <p>This property should not be empty. Example value: {@code
   * "/tmp/hbase_mirroring_client_failed_mutations"}.
   */
  public static final String MIRRORING_FAILLOG_PREFIX_PATH_KEY =
      "google.bigtable.mirroring.write-error-log.appender.prefix-path";

  /**
   * Maximum size of the buffer holding failed mutations before they are logged to persistent
   * storage.
   *
   * <p>Defaults to {@code 20 * 1024 * 1024}.
   */
  public static final String MIRRORING_FAILLOG_MAX_BUFFER_SIZE_KEY =
      "google.bigtable.mirroring.write-error-log.appender.max-buffer-size";

  /**
   * Controls the behavior of the failed mutation log on persistent storage not keeping up with
   * writing the mutations.
   *
   * <p>If set to {@code true}, mutations will be dropped, otherwise they will block the thread
   * until the storage catches up.
   *
   * <p>Defaults to {@code false}.
   */
  public static final String MIRRORING_FAILLOG_DROP_ON_OVERFLOW_KEY =
      "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow";

  /**
   * Number of milliseconds that {@link
   * com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection} should wait synchronously for
   * pending operations before terminating connection with secondary database.
   *
   * <p>If the timeout is reached, some of the operations on secondary database are still be
   * in-flight and would be lost if we closed the secondary connection immediately. Those requests
   * are not cancelled and will be performed asynchronously until the program terminates.
   *
   * <p>Defaults to 60000.
   */
  public static final String MIRRORING_CONNECTION_CONNECTION_TERMINATION_TIMEOUT =
      "google.bigtable.mirroring.connection.termination-timeout";

  /**
   * Number of previous unmatched results that {@link MirroringResultScanner} should check before
   * declaring scan's results erroneous.
   *
   * <p>If not set uses default value of 5. Matching to one of buffered results removes earlier
   * entries from the buffer.
   */
  public static final String MIRRORING_SCANNER_BUFFERED_MISMATCHED_READS =
      "google.bigtable.mirroring.result-scanner.buffered-mismatched-reads";

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
