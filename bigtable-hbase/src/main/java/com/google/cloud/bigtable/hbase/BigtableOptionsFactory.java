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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.conf.Configuration;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.ChannelOptions;
import com.google.cloud.bigtable.grpc.RetryOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Static methods to convert an instance of {@link Configuration}
 * to a {@link BigtableOptions} instance.
 */
public class BigtableOptionsFactory {
  protected static final Logger LOG = new Logger(BigtableOptionsFactory.class);

  // TODO(sduskis): all default options should be moved to their appropriate *Options classes
  // This class is specific to HBase's configuration approach.  The defaults ought to be used in
  // the context of a stand alone java client, eventually.

  public static final String GRPC_EVENTLOOP_GROUP_NAME = "bigtable-grpc-elg";
  public static final String RETRY_THREADPOOL_NAME = "bigtable-rpc-retry";
  public static final int RETRY_THREAD_COUNT = 4;

  public static final String BIGTABLE_PORT_KEY = "google.bigtable.endpoint.port";
  public static final int DEFAULT_BIGTABLE_PORT = 443;
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_KEY =
      "google.bigtable.cluster.admin.endpoint.host";
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT =
      "bigtableclusteradmin.googleapis.com";
  public static final String BIGTABLE_TABLE_ADMIN_HOST_KEY =
      "google.bigtable.admin.endpoint.host";
  public static final String BIGTABLE_TABLE_ADMIN_HOST_DEFAULT =
      "bigtabletableadmin.googleapis.com";
  public static final String BIGTABLE_HOST_KEY = "google.bigtable.endpoint.host";
  public static final String BIGTABLE_HOST_DEFAULT = "bigtable.googleapis.com";
  public static final String PROJECT_ID_KEY = "google.bigtable.project.id";
  public static final String CLUSTER_KEY = "google.bigtable.cluster.name";
  public static final String ZONE_KEY = "google.bigtable.zone.name";
  public static final String CALL_REPORT_DIRECTORY_KEY = "google.bigtable.call.report.directory";
  public static final String SERVICE_ACCOUNT_JSON_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";

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
   * Key to set to a boolean flag indicating whether or not grpc retries should be enabled.
   * The default is to enable retries on failed idempotent operations.
   */
  public static final String ENABLE_GRPC_RETRIES_KEY = "google.bigtable.grpc.retry.enable";
  public static final boolean ENABLE_GRPC_RETRIES_DEFAULT = true;

  /**
   * Key to set to a boolean flag indicating whether or not to retry grpc call on deadline exceeded.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY =
      "google.bigtable.grpc.retry.deadlineexceeded.enable";
  public static final boolean ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_DEFAULT = true;

  /**
   * Key to set to a boolean flag indicating the maximum amount of time to wait for retries, given
   * a backoff policy on errors.
   * This flag is used only when grpc retries is enabled.
   */
  public static final String MAX_ELAPSED_BACKOFF_MILLIS_KEY =
      "google.bigtable.grpc.retry.max.elapsed.backoff.ms";
  public static final int MAX_ELAPSED_BACKOFF_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes

  /**
   * The number of grpc channels to open for asynchronous processing such as puts.
   */
  public static final String BIGTABLE_CHANNEL_COUNT_KEY = "google.bigtable.grpc.channel.count";
  public static final int BIGTABLE_CHANNEL_COUNT_DEFAULT = 4;

  /**
   * The maximum length of time to keep a Bigtable grpc channel open.
   */
  public static final String BIGTABLE_CHANNEL_TIMEOUT_MS_KEY =
      "google.bigtable.grpc.channel.timeout.ms";
  public static final long BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT = 30 * 60 * 1000;

  public static BigtableOptions fromConfiguration(Configuration configuration) throws IOException {
    BigtableOptions.Builder bigtableOptionsBuilder = new BigtableOptions.Builder();
    String projectId = configuration.get(PROJECT_ID_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId),
        String.format("Project ID must be supplied via %s", PROJECT_ID_KEY));
    bigtableOptionsBuilder.setProjectId(projectId);
    LOG.debug("Project ID %s", projectId);

    String zone = configuration.get(ZONE_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zone),
        String.format("Zone must be supplied via %s", ZONE_KEY));
    bigtableOptionsBuilder.setZone(zone);
    LOG.debug("Zone %s", zone);

    String cluster = configuration.get(CLUSTER_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(cluster),
        String.format("Cluster must be supplied via %s", CLUSTER_KEY));
    bigtableOptionsBuilder.setCluster(cluster);
    LOG.debug("Cluster %s", cluster);

    // TODO(sduskis): InetAddress.getByName is expensive.  This can be parallelized to speed up
    // startup.

    String overrideIp = configuration.get(IP_OVERRIDE_KEY);
    InetAddress overrideIpAddress = null;
    if (!Strings.isNullOrEmpty(overrideIp)) {
      LOG.debug("Using override IP address %s", overrideIp);
      overrideIpAddress = InetAddress.getByName(overrideIp);
    }

    String dataHost = configuration.get(BIGTABLE_HOST_KEY, BIGTABLE_HOST_DEFAULT);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(dataHost),
        String.format("API Data endpoint host must be supplied via %s", BIGTABLE_HOST_KEY));
    if (overrideIpAddress == null) {
      LOG.debug("Data endpoint host %s", dataHost);
      bigtableOptionsBuilder.setDataHost(InetAddress.getByName(dataHost));
    } else {
      LOG.debug("Data endpoint host %s. Using override IP address.", dataHost);
      bigtableOptionsBuilder.setDataHost(
          InetAddress.getByAddress(dataHost, overrideIpAddress.getAddress()));
    }

    String tableAdminHost = configuration.get(
        BIGTABLE_TABLE_ADMIN_HOST_KEY, BIGTABLE_TABLE_ADMIN_HOST_DEFAULT);
    if (overrideIpAddress == null) {
      LOG.debug("Table admin endpoint host %s", tableAdminHost);
      bigtableOptionsBuilder.setTableAdminHost(InetAddress.getByName(tableAdminHost));
    } else {
      LOG.debug("Table admin endpoint host %s. Using override IP address.", tableAdminHost);
      bigtableOptionsBuilder.setTableAdminHost(
          InetAddress.getByAddress(tableAdminHost, overrideIpAddress.getAddress()));
    }

    String clusterAdminHost = configuration.get(
        BIGTABLE_CLUSTER_ADMIN_HOST_KEY, BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT);
    if (overrideIpAddress == null) {
      LOG.debug("Cluster admin endpoint host %s", clusterAdminHost);
      bigtableOptionsBuilder.setClusterAdminHost(InetAddress.getByName(clusterAdminHost));
    } else {
      LOG.debug("Cluster admin endpoint host %s. Using override IP address.", clusterAdminHost);
      bigtableOptionsBuilder.setClusterAdminHost(
          InetAddress.getByAddress(clusterAdminHost, overrideIpAddress.getAddress()));
    }

    int port = configuration.getInt(BIGTABLE_PORT_KEY, DEFAULT_BIGTABLE_PORT);
    bigtableOptionsBuilder.setPort(port);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(GRPC_EVENTLOOP_GROUP_NAME + "-%d").build();
    final EventLoopGroup elg = new NioEventLoopGroup(0, threadFactory);
    bigtableOptionsBuilder.setCustomEventLoopGroup(elg);

    bigtableOptionsBuilder.setChannelOptions(createChannelOptions(configuration));
    createChannelOptions(configuration).addClientCloseHandler(new Closeable() {
      @Override
      public void close() throws IOException {
        elg.shutdownGracefully();
      }
    });

    return bigtableOptionsBuilder.build();
  }

  private static ChannelOptions createChannelOptions(Configuration configuration)
      throws IOException {
    ChannelOptions.Builder channelOptionsBuilder = new ChannelOptions.Builder();
    try {
      // TODO(sduskis): creating credentials is expensive.  Creating this can be parallelized.
      if (configuration.getBoolean(
          BIGTABE_USE_SERVICE_ACCOUNTS_KEY, BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT)) {
        LOG.debug("Using service accounts");

        String serviceAccountJson = System.getenv().get(SERVICE_ACCOUNT_JSON_ENV_VARIABLE);
        String serviceAccountEmail = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
        if (!Strings.isNullOrEmpty(serviceAccountJson)) {
          LOG.debug("Using JSON file: %s", serviceAccountJson);
          channelOptionsBuilder.setCredential(CredentialFactory.getApplicationDefaultCredential());
        } else if (!Strings.isNullOrEmpty(serviceAccountEmail)) {
          LOG.debug("Service account %s specified.", serviceAccountEmail);
          String keyfileLocation =
              configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
          Preconditions.checkState(
              !Strings.isNullOrEmpty(keyfileLocation),
              "Key file location must be specified when setting service account email");
          LOG.debug("Using p12 keyfile: %s", keyfileLocation);
          channelOptionsBuilder.setCredential(
              CredentialFactory.getCredentialFromPrivateKeyServiceAccount(
                  serviceAccountEmail, keyfileLocation));
        } else {
          channelOptionsBuilder.setCredential(CredentialFactory
              .getCredentialFromMetadataServiceAccount());
        }
      } else if (configuration.getBoolean(
          BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT)) {
        channelOptionsBuilder.setCredential(null); // Intended for testing purposes only.
        LOG.info("Enabling the use of null credentials. This should not be used in production.");
      } else {
        throw new IllegalStateException(
            "Either service account or null credentials must be enabled");
      }
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to acquire credential.", gse);
    }

    final ScheduledExecutorService retryExecutor =
        Executors.newScheduledThreadPool(
            RETRY_THREAD_COUNT,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(RETRY_THREADPOOL_NAME + "-%d")
                .build());
    channelOptionsBuilder.setScheduledExecutorService(retryExecutor);

    // TODO(sduskis): I think that this call report directory mechanism doesn't work anymore as is.
    // We need to make sure that if we want this feature, that we have only a single interceptor
    // for all BigtableChannels.  Right now, we create one interceptor per channel, so there
    // are at least 2: 1 for admin and 1 for data.

    // Set up aggregate performance and call error rate logging:
    if (!Strings.isNullOrEmpty(configuration.get(CALL_REPORT_DIRECTORY_KEY))) {
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
      channelOptionsBuilder.setCallStatusReportPath(callStatusReport);
      channelOptionsBuilder.setCallTimingReportPath(callTimingReport);
    }

    channelOptionsBuilder.setRetryOptions(createRetryOptions(configuration));

    int channelCount =
        configuration.getInt(BIGTABLE_CHANNEL_COUNT_KEY, BIGTABLE_CHANNEL_COUNT_DEFAULT);
    channelOptionsBuilder.setChannelCount(channelCount);

    long channelTimeout =
        configuration.getLong(BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT);

    // Connection refresh takes a couple of seconds. 1 minute is the bare minimum that this should
    // be allowed to be set at.
    Preconditions.checkArgument(channelTimeout == 0 || channelTimeout >= 60000,
      BIGTABLE_CHANNEL_TIMEOUT_MS_KEY + " has to be 0 (no timeout) or 1 minute+ (60000)");
    channelOptionsBuilder.setTimeoutMs(channelTimeout);

    channelOptionsBuilder.setUserAgent(BigtableConstants.USER_AGENT);

    ChannelOptions channelOptions = channelOptionsBuilder.build();

    channelOptions.addClientCloseHandler(new Closeable() {
      @Override
      public void close() throws IOException {
        retryExecutor.shutdownNow();
      }
    });

    return channelOptions;
  }

  private static RetryOptions createRetryOptions(Configuration configuration) {
    RetryOptions.Builder retryOptionsBuilder = new RetryOptions.Builder();
    boolean enableRetries = configuration.getBoolean(
        ENABLE_GRPC_RETRIES_KEY, ENABLE_GRPC_RETRIES_DEFAULT);
    LOG.debug("gRPC retries enabled: %s", enableRetries);
    retryOptionsBuilder.setEnableRetries(enableRetries);

    boolean retryOnDeadlineExceeded = configuration.getBoolean(
        ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY, ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_DEFAULT);
    LOG.debug("gRPC retry on deadline exceeded enabled: %s", retryOnDeadlineExceeded);
    retryOptionsBuilder.setRetryOnDeadlineExceeded(retryOnDeadlineExceeded);

    int maxElapsedBackoffMillis = configuration.getInt(
        MAX_ELAPSED_BACKOFF_MILLIS_KEY, MAX_ELAPSED_BACKOFF_MS_DEFAULT);
    LOG.debug("gRPC retry maxElapsedBackoffMillis: %d", maxElapsedBackoffMillis);
    retryOptionsBuilder.setMaxElapsedBackoffMillis(maxElapsedBackoffMillis);

    return retryOptionsBuilder.build();
  }
}
