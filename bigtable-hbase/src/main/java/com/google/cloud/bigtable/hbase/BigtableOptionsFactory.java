/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ExecutorServiceFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Static methods to convert an instance of {@link Configuration}
 * to a {@link BigtableOptions} instance.
 */
public class BigtableOptionsFactory {
  protected static final Logger LOG = new Logger(BigtableOptionsFactory.class);

  public static final String GRPC_EVENTLOOP_GROUP_NAME = "bigtable-grpc-elg";
  public static final String RETRY_THREADPOOL_NAME = "bigtable-rpc-retry";
  public static final int RETRY_THREAD_COUNT = 2;

  public static final String BIGTABLE_PORT_KEY = "google.bigtable.endpoint.port";
  public static final int DEFAULT_BIGTABLE_PORT = 443;
  public static final String BIGTABLE_ADMIN_HOST_KEY = "google.bigtable.admin.endpoint.host";
  public static final String BIGTABLE_HOST_KEY = "google.bigtable.endpoint.host";
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
   * Key to set to the service account email address. If this value is not set
   * and using service accounts is enabled, a metadata server account will be used.
   */
  public static final String BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY =
      "google.bigtable.auth.service.account.email";

  /**
   * Key to set to a location where a P12 keyfile can be found that corresponds to the provided
   * service account email address. If not set, the application default credential is used.
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
   * When enabled, RPC related thread pools will be constructed to use daemon threads which will
   * allow the JVM to shutdown without properly closing connections, etc.
   *
   * Setting this to true is not recommended for production systems.
   */
  public static final String ENABLE_DAEMONIZED_THREADS_KEY =
      "google.bigtable.not_recommended.daemonized.io.threadpools.enable";
  public static final boolean ENABLE_DAEMONIZED_THREADS_DEFAULT = false;

  public static BigtableOptions fromConfiguration(Configuration configuration) throws IOException {
    BigtableOptions.Builder optionsBuilder = new BigtableOptions.Builder();

    String projectId = configuration.get(PROJECT_ID_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId),
        String.format("Project ID must be supplied via %s", PROJECT_ID_KEY));
    optionsBuilder.setProjectId(projectId);
    LOG.debug("Project ID %s", projectId);

    String zone = configuration.get(ZONE_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zone),
        String.format("Zone must be supplied via %s", ZONE_KEY));
    optionsBuilder.setZone(zone);
    LOG.debug("Zone %s", zone);

    String cluster = configuration.get(CLUSTER_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(cluster),
        String.format("Cluster must be supplied via %s", CLUSTER_KEY));
    optionsBuilder.setCluster(cluster);
    LOG.debug("Cluster %s", cluster);


    String overrideIp = configuration.get(IP_OVERRIDE_KEY);
    InetAddress overrideIpAddress = null;
    if (!Strings.isNullOrEmpty(overrideIp)) {
      LOG.debug("Using override IP address %s", overrideIp);
      overrideIpAddress = InetAddress.getByName(overrideIp);
    }

    String host = configuration.get(BIGTABLE_HOST_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(host),
        String.format("API endpoint host must be supplied via %s", BIGTABLE_HOST_KEY));
    if (overrideIpAddress == null) {
      LOG.debug("Data endpoint host %s", host);
      optionsBuilder.setHost(InetAddress.getByName(host));
    } else {
      LOG.debug("Data endpoint host %s. Using override IP address.", host);
      optionsBuilder.setHost(InetAddress.getByAddress(host, overrideIpAddress.getAddress()));
    }

    String adminHost = configuration.get(BIGTABLE_ADMIN_HOST_KEY);
    if (Strings.isNullOrEmpty(adminHost)) {
      LOG.debug("Admin endpoint host not configured, assuming we should use data endpoint.");
      adminHost = host;
    }

    if (overrideIpAddress == null) {
      LOG.debug("Admin endpoint host %s", host);
      optionsBuilder.setAdminHost(InetAddress.getByName(adminHost));
    } else {
      LOG.debug("Admin endpoint host %s. Using override IP address.", host);
      optionsBuilder.setAdminHost(
          InetAddress.getByAddress(adminHost, overrideIpAddress.getAddress()));
    }

    int port = configuration.getInt(BIGTABLE_PORT_KEY, DEFAULT_BIGTABLE_PORT);
    optionsBuilder.setPort(port);

    try {
      if (configuration.getBoolean(
          BIGTABE_USE_SERVICE_ACCOUNTS_KEY, BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT)) {
        LOG.debug("Using service accounts");

        String serviceAccountJson = System.getenv().get(SERVICE_ACCOUNT_JSON_ENV_VARIABLE);
        String serviceAccountEmail = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
        if (!Strings.isNullOrEmpty(serviceAccountJson)) {
          LOG.debug("Using JSON file: %s", serviceAccountJson);
          optionsBuilder.setCredential(CredentialFactory.getApplicationDefaultCredential());
        } else if (!Strings.isNullOrEmpty(serviceAccountEmail)) {
          LOG.debug("Service account %s specified.", serviceAccountEmail);
          String keyfileLocation =
              configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
          Preconditions.checkState(
              !Strings.isNullOrEmpty(keyfileLocation),
              "Key file location must be specified when setting service account email");
          LOG.debug("Using p12 keyfile: %s", keyfileLocation);
          optionsBuilder.setCredential(
              CredentialFactory.getCredentialFromPrivateKeyServiceAccount(
                  serviceAccountEmail, keyfileLocation));
        } else {
          optionsBuilder.setCredential(CredentialFactory.getCredentialFromMetadataServiceAccount());
        }
      } else if (configuration.getBoolean(
          BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT)) {
        optionsBuilder.setCredential(null); // Intended for testing purposes only.
        LOG.info("Enabling the use of null credentials. This should not be used in production.");
      } else {
        throw new IllegalStateException(
            "Either service account or null credentials must be enabled");
      }
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to acquire credential.", gse);
    }

    if (configuration.getBoolean(
        ENABLE_DAEMONIZED_THREADS_KEY, ENABLE_DAEMONIZED_THREADS_DEFAULT)) {
      LOG.info("Enabling daemonized threads.");
      EventLoopGroup elg = new NioEventLoopGroup(0, new ExecutorServiceFactory() {
        @Override
        public ExecutorService newExecutorService(int parallelism) {
          return Executors.newFixedThreadPool(
              parallelism,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat(GRPC_EVENTLOOP_GROUP_NAME + "-%d").build());
        }
      });
      optionsBuilder.setCustomEventLoopGroup(elg);

      ScheduledExecutorService retryExecutor =
          Executors.newScheduledThreadPool(
              RETRY_THREAD_COUNT,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat(RETRY_THREADPOOL_NAME + "-%d")
                  .build());
      optionsBuilder.setRpcRetryExecutorService(retryExecutor);
    }

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
      optionsBuilder.setCallStatusReportPath(callStatusReport);
      optionsBuilder.setCallTimingReportPath(callTimingReport);
    }

    boolean enableRetries = configuration.getBoolean(
        ENABLE_GRPC_RETRIES_KEY, ENABLE_GRPC_RETRIES_DEFAULT);
    LOG.debug("gRPC retries enabled: %s", enableRetries);
    optionsBuilder.setRetriesEnabled(enableRetries);

    return optionsBuilder.build();
  }
}
