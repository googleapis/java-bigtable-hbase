/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.api.client.util.Strings.isNullOrEmpty;
import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_TABLE_ADMIN_HOST_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableOptions.DEFAULT_BIGTABLE_PORT;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Static methods to convert an instance of {@link Configuration}
 * to a {@link BigtableOptions} instance.
 */
public class BigtableOptionsFactory {
  protected static final Logger LOG = new Logger(BigtableOptionsFactory.class);

  public static final String BIGTABLE_PORT_KEY = "google.bigtable.endpoint.port";
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_KEY =
      "google.bigtable.cluster.admin.endpoint.host";
  public static final String BIGTABLE_TABLE_ADMIN_HOST_KEY =
      "google.bigtable.admin.endpoint.host";
  public static final String BIGTABLE_HOST_KEY = "google.bigtable.endpoint.host";

  public static final String PROJECT_ID_KEY = "google.bigtable.project.id";
  public static final String CLUSTER_KEY = "google.bigtable.cluster.name";
  public static final String ZONE_KEY = "google.bigtable.zone.name";
  public static final String CALL_REPORT_DIRECTORY_KEY = "google.bigtable.call.report.directory";

  /**
   * If set, bypass DNS host lookup and use the given IP address.
   */
  public static final String IP_OVERRIDE_KEY = "google.bigtable.endpoint.ip.address.override";

  /**
   * Key to set to enable service accounts to be used, either metadata server-based or P12-based.
   * Defaults to enabled.
   */
  public static final String BIGTABE_USE_SERVICE_ACCOUNTS_KEY =
      "google.bigtable.auth.service.account.enable";
  public static final boolean BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT = true;

  /**
   * Key to allow unit tests to proceed with an invalid credential configuration.
   */
  public static final String BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY =
      "google.bigtable.auth.null.credential.enable";
  public static final boolean BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT = false;

  /**
   * Key to set when using P12 keyfile authentication. The value should be the service account email
   * address as displayed. If this value is not set and using service accounts is enabled, a
   * metadata server account will be used.
   */
  public static final String BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY =
      "google.bigtable.auth.service.account.email";

  /**
   * Key to set to a location where a P12 keyfile can be found that corresponds to the provided
   * service account email address.
   */
  public static final String BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY =
      "google.bigtable.auth.service.account.keyfile";

  /**
   * Key to set to a location where a json security credentials file can be found.
   */
  public static final String BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY =
      "google.bigtable.auth.json.keyfile";

  /**
   * Key to set to a boolean flag indicating whether or not grpc retries should be enabled.
   * The default is to enable retries on failed idempotent operations.
   */
  public static final String ENABLE_GRPC_RETRIES_KEY = "google.bigtable.grpc.retry.enable";
  /**
   * Key to set to a boolean flag indicating whether or not to retry grpc call on deadline exceeded.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY =
      "google.bigtable.grpc.retry.deadlineexceeded.enable";
  /**
   * Key to set to a boolean flag indicating the maximum amount of time to wait for retries, given
   * a backoff policy on errors.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String MAX_ELAPSED_BACKOFF_MILLIS_KEY =
      "google.bigtable.grpc.retry.max.elapsed.backoff.ms";
  /**
   * The number of grpc channels to open for asynchronous processing such as puts.
   */
  public static final String BIGTABLE_DATA_CHANNEL_COUNT_KEY = "google.bigtable.grpc.channel.count";
  /**
   * The maximum length of time to keep a Bigtable grpc channel open.
   */
  public static final String BIGTABLE_CHANNEL_TIMEOUT_MS_KEY =
      "google.bigtable.grpc.channel.timeout.ms";

  public static BigtableOptions fromConfiguration(final Configuration configuration)
      throws IOException {

    BigtableOptions.Builder bigtableOptionsBuilder = new BigtableOptions.Builder();

    bigtableOptionsBuilder.setProjectId(getValue(configuration, PROJECT_ID_KEY, "Project ID"));
    bigtableOptionsBuilder.setZoneId(getValue(configuration, ZONE_KEY, "Zone"));
    bigtableOptionsBuilder.setClusterId(getValue(configuration, CLUSTER_KEY, "Cluster"));

    String overrideIp = configuration.get(IP_OVERRIDE_KEY);
    if (!isNullOrEmpty(overrideIp)) {
      LOG.debug("Using override IP address %s", overrideIp);
      bigtableOptionsBuilder.setOverrideIp(overrideIp);
    }

    bigtableOptionsBuilder.setDataHost(
        getHost(configuration, BIGTABLE_HOST_KEY, BIGTABLE_DATA_HOST_DEFAULT, "API Data"));

    bigtableOptionsBuilder.setTableAdminHost(getHost(
      configuration, BIGTABLE_TABLE_ADMIN_HOST_KEY, BIGTABLE_TABLE_ADMIN_HOST_DEFAULT,
      "Table Admin"));

    bigtableOptionsBuilder.setClusterAdminHost(getHost(configuration,
        BIGTABLE_CLUSTER_ADMIN_HOST_KEY, BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT, "Cluster Admin"));

    int port = configuration.getInt(BIGTABLE_PORT_KEY, DEFAULT_BIGTABLE_PORT);
    bigtableOptionsBuilder.setPort(port);
    setChannelOptions(bigtableOptionsBuilder, configuration);

    return bigtableOptionsBuilder.build();
  }

  private static String getValue(final Configuration configuration, String key, String type) {
    String value = configuration.get(key);
    Preconditions.checkArgument(
        !isNullOrEmpty(value),
        String.format("%s must be supplied via %s", type, key));
    LOG.debug("%s %s", type, value);
    return value;
  }

  private static String getHost(Configuration configuration, String key,
      String defaultVal, String type) {
    String hostName = configuration.get(key, defaultVal);
    LOG.debug("%s endpoint host %s.", type, hostName);
    return hostName;
  }

  private static void
      setChannelOptions(BigtableOptions.Builder builder, Configuration configuration)
          throws IOException {
    setCredentialOptions(builder, configuration);

    // TODO(sduskis): I think that this call report directory mechanism doesn't work anymore as is.
    // We need to make sure that if we want this feature, that we have only a single interceptor
    // for all BigtableChannels.  Right now, we create one interceptor per channel, so there
    // are at least 2: 1 for admin and 1 for data.

    // Set up aggregate performance and call error rate logging:
    if (!isNullOrEmpty(configuration.get(CALL_REPORT_DIRECTORY_KEY))) {
      String reportDirectory = configuration.get(CALL_REPORT_DIRECTORY_KEY);
      Path reportDirectoryPath = FileSystems.getDefault().getPath(reportDirectory);
      if (Files.exists(reportDirectoryPath)) {
        Preconditions.checkState(
            Files.isDirectory(reportDirectoryPath), "Report path %s must be a directory");
      } else {
        Files.createDirectories(reportDirectoryPath);
      }
      String callStatusReport =
          reportDirectoryPath.resolve("call_status.txt").toAbsolutePath().toString();
      String callTimingReport =
          reportDirectoryPath.resolve("call_timing.txt").toAbsolutePath().toString();
      LOG.debug("Logging call status aggregates to %s", callStatusReport);
      LOG.debug("Logging call timing aggregates to %s", callTimingReport);
      builder.setCallStatusReportPath(callStatusReport);
      builder.setCallTimingReportPath(callTimingReport);
    }

    builder.setRetryOptions(createRetryOptions(configuration));

    int channelCount = configuration.getInt(
        BIGTABLE_DATA_CHANNEL_COUNT_KEY, BigtableOptions.BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT);
    builder.setDataChannelCount(channelCount);

    int channelTimeout = configuration.getInt(
        BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, BigtableOptions.BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT);

    // Connection refresh takes a couple of seconds. 1 minute is the bare minimum that this should
    // be allowed to be set at.
    Preconditions.checkArgument(channelTimeout == 0 || channelTimeout >= 60000,
      BIGTABLE_CHANNEL_TIMEOUT_MS_KEY + " has to be 0 (no timeout) or 1 minute+ (60000)");
    builder.setTimeoutMs(channelTimeout);

    builder.setUserAgent(BigtableConstants.USER_AGENT);
  }

  private static void setCredentialOptions(BigtableOptions.Builder builder,
      Configuration configuration) throws FileNotFoundException {
    if (configuration.getBoolean(
        BIGTABE_USE_SERVICE_ACCOUNTS_KEY, BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT)) {
      LOG.debug("Using service accounts");

      if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY) != null) {
        String keyfileLocation =
            configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
        LOG.debug("Using json keyfile: %s", keyfileLocation);
        builder.setCredentialOptions(CredentialOptions.jsonCredentials(new FileInputStream(
            keyfileLocation)));
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY) != null) {
        String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
        LOG.debug("Service account %s specified.", serviceAccount);
        String keyfileLocation =
            configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
        Preconditions.checkState(!isNullOrEmpty(keyfileLocation),
          "Key file location must be specified when setting service account email");
        LOG.debug("Using p12 keyfile: %s", keyfileLocation);
        builder.setCredentialOptions(CredentialOptions.p12Credential(serviceAccount,
          keyfileLocation));
      } else {
        LOG.debug("Using default credentials.");
        builder.setCredentialOptions(CredentialOptions.defaultCredentials());
      }
    } else if (configuration.getBoolean(
        BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT)) {
      builder.setCredentialOptions(CredentialOptions.nullCredential());
      LOG.info("Enabling the use of null credentials. This should not be used in production.");
    } else {
      throw new IllegalStateException(
          "Either service account or null credentials must be enabled");
    }
  }

  private static RetryOptions createRetryOptions(Configuration configuration) {
    RetryOptions.Builder retryOptionsBuilder = new RetryOptions.Builder();
    boolean enableRetries = configuration.getBoolean(
        ENABLE_GRPC_RETRIES_KEY, RetryOptions.ENABLE_GRPC_RETRIES_DEFAULT);
    LOG.debug("gRPC retries enabled: %s", enableRetries);
    retryOptionsBuilder.setEnableRetries(enableRetries);

    boolean retryOnDeadlineExceeded = configuration.getBoolean(
        ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY,
        RetryOptions.ENABLE_GRPC_RETRY_DEADLINE_EXCEEDED_DEFAULT);
    LOG.debug("gRPC retry on deadline exceeded enabled: %s", retryOnDeadlineExceeded);
    retryOptionsBuilder.setRetryOnDeadlineExceeded(retryOnDeadlineExceeded);

    int maxElapsedBackoffMillis = configuration.getInt(
        MAX_ELAPSED_BACKOFF_MILLIS_KEY,
        RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS);
    LOG.debug("gRPC retry maxElapsedBackoffMillis: %d", maxElapsedBackoffMillis);
    retryOptionsBuilder.setMaxElapsedBackoffMillis(maxElapsedBackoffMillis);

    return retryOptionsBuilder.build();
  }
}
