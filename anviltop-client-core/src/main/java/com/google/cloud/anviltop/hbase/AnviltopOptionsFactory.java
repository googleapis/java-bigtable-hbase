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
package com.google.cloud.anviltop.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.net.stubby.transport.AbstractClientStream;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;

/**
 * Static methods to convert an instance of {@link Configuration}
 * to a {@link AnviltopOptions} instance.
 */
public class AnviltopOptionsFactory {
  protected static final Logger LOG = new Logger(AnviltopOptionsFactory.class);

  public static final String ANVILTOP_PORT_KEY = "google.anviltop.endpoint.port";
  public static final int DEFAULT_ANVILTOP_PORT = 443;
  public static final String ANVILTOP_ADMIN_HOST_KEY = "google.anviltop.admin.endpoint.host";
  public static final String ANVILTOP_HOST_KEY = "google.anviltop.endpoint.host";
  public static final String PROJECT_ID_KEY = "google.anviltop.project.id";
  public static final String CALL_REPORT_DIRECTORY_KEY = "google.anviltop.call.report.directory";

  /**
   * If set, bypass DNS host lookup and use the given IP address.
   */
  public static final String IP_OVERRIDE_KEY = "google.anviltop.endpoint.ip.address.override";

  /**
   * Key to set to enable service accounts to be used, either metadata server-based or P12-based.
   * Defaults to enabled.
   */
  public static final String ANVILTOP_USE_SERVICE_ACCOUNTS_KEY =
      "google.anviltop.auth.service.account.enable";
  public static final boolean ANVILTOP_USE_SERVICE_ACCOUNTS_DEFAULT = true;

  /**
   * Key to allow unit tests to proceed with an invalid credential configuration.
   */
  public static final String ANVILTOP_NULL_CREDENTIAL_ENABLE_KEY =
      "google.anviltop.auth.null.credential.enable";
  public static final boolean ANVILTOP_NULL_CREDENTIAL_ENABLE_DEFAULT = false;

  /**
   * Key to set when using P12 keyfile authentication. The value should be the service account email
   * address as displayed. If this value is not set and using service accounts is enabled, a
   * metadata server account will be used.
   */
  public static final String ANVILTOP_SERVICE_ACCOUNT_EMAIL_KEY =
      "google.anviltop.auth.service.account.email";

  /**
   * Key to set to a location where a P12 keyfile can be found that corresponds to the provided
   * service account email address.
   */
  public static final String ANVILTOP_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY =
      "google.anviltop.auth.service.account.keyfile";

  /**
   * Key to set to a boolean flag indicating whether or not grpc retries should be enabled.
   * The default is to enable retries on failed idempotent operations.
   */
  public static final String ENABLE_GRPC_RETRIES_KEY = "google.anviltop.grpc.retry.enable";
  public static final boolean ENABLE_GRPC_RETRIES_DEFAULT = true;

  public static AnviltopOptions fromConfiguration(Configuration configuration) throws IOException {
    AnviltopOptions.Builder optionsBuilder = new AnviltopOptions.Builder();

    String projectId = configuration.get(PROJECT_ID_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId),
        String.format("Project ID must be supplied via %s", PROJECT_ID_KEY));
    optionsBuilder.setProjectId(projectId);
    LOG.debug("Project ID %s", projectId);


    String overrideIp = configuration.get(IP_OVERRIDE_KEY);
    InetAddress overrideIpAddress = null;
    if (!Strings.isNullOrEmpty(overrideIp)) {
      LOG.debug("Using override IP address %s", overrideIp);
      overrideIpAddress = InetAddress.getByName(overrideIp);
    }

    String host = configuration.get(ANVILTOP_HOST_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(host),
        String.format("API endpoint host must be supplied via %s", ANVILTOP_HOST_KEY));
    if (overrideIpAddress == null) {
      LOG.debug("Data endpoint host %s", host);
      optionsBuilder.setHost(InetAddress.getByName(host));
    } else {
      LOG.debug("Data endpoint host %s. Using override IP address.", host);
      optionsBuilder.setHost(InetAddress.getByAddress(host, overrideIpAddress.getAddress()));
    }

    String adminHost = configuration.get(ANVILTOP_ADMIN_HOST_KEY);
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

    int port = configuration.getInt(ANVILTOP_PORT_KEY, DEFAULT_ANVILTOP_PORT);
    optionsBuilder.setPort(port);

    try {
      if (configuration.getBoolean(
          ANVILTOP_USE_SERVICE_ACCOUNTS_KEY, ANVILTOP_USE_SERVICE_ACCOUNTS_DEFAULT)) {
        LOG.debug("Using service accounts");

        String serviceAccountEmail = configuration.get(ANVILTOP_SERVICE_ACCOUNT_EMAIL_KEY);

        if (!Strings.isNullOrEmpty(serviceAccountEmail)) {
          LOG.debug(
              "Service account %s specified, using p12 authentication flow.", serviceAccountEmail);
          // Using P12 keyfile based OAuth:
          String keyfileLocation =
              configuration.get(ANVILTOP_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);

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
          ANVILTOP_NULL_CREDENTIAL_ENABLE_KEY, ANVILTOP_NULL_CREDENTIAL_ENABLE_DEFAULT)) {
        optionsBuilder.setCredential(null); // Intended for testing purposes only.
        LOG.info("Enabling the use of null credentials. This should not be used in production.");
      } else {
        throw new IllegalStateException(
            "Either service account or null credentials must be enabled");
      }
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to acquire credential.", gse);
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
      String callTimignReport =
          reportDirectoryPath.resolve("call_timing.txt").toAbsolutePath().toString();
      LOG.debug("Logging call status aggregates to %s", callStatusReport);
      LOG.debug("Logging call timing aggregates to %s", callTimignReport);
      optionsBuilder.setCallStatusReportPath(callStatusReport);
      optionsBuilder.setCallTimingReportPath(callTimignReport);
    }

    boolean enableRetries = configuration.getBoolean(
        ENABLE_GRPC_RETRIES_KEY, ENABLE_GRPC_RETRIES_DEFAULT);
    LOG.debug("gRPC retries enabled: %s", enableRetries);
    optionsBuilder.setRetriesEnabled(enableRetries);

    return optionsBuilder.build();
  }
}
