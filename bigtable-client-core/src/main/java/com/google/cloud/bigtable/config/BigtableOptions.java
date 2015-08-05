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
package com.google.cloud.bigtable.config;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.api.client.util.Strings;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * An immutable class providing access to configuration options for Bigtable.
 */
public class BigtableOptions {

  public static final String BIGTABLE_TABLE_ADMIN_HOST_DEFAULT =
      "bigtabletableadmin.googleapis.com";
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT =
      "bigtableclusteradmin.googleapis.com";
  public static final String BIGTABLE_DATA_HOST_DEFAULT = "bigtable.googleapis.com";
  public static final int DEFAULT_BIGTABLE_PORT = 443;

  public static final int BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT = 4;
  public static final int BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT =
      (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);

  private static final Logger LOG = new Logger(BigtableOptions.class);

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {
     // Configuration that a user is required to set.
    private String projectId;
    private String zoneId;
    private String clusterId;
    private String userAgent;

    // Optional configuration for hosts - useful for the Bigtable team, more than anything else.
    private String dataHost = BIGTABLE_DATA_HOST_DEFAULT;
    private String tableAdminHost = BIGTABLE_TABLE_ADMIN_HOST_DEFAULT;
    private String clusterAdminHost = BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT;
    private int port = DEFAULT_BIGTABLE_PORT;
    private String overrideIp;

    // The default credentials get credential from well known locations, such as the GCE
    // metdata service or gcloud configuration in other environments. A user can also override
    // the default behavior with P12 or JSon configuration.
    private CredentialOptions credentialOptions = CredentialOptions.defaultCredentials();

    // Performance tuning options.
    private RetryOptions retryOptions = new RetryOptions.Builder().build();
    private int timeoutMs = BIGTABLE_CHANNEL_TIMEOUT_MS_DEFAULT;
    private int dataChannelCount = BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT;

    // Debugging information about the types of calls that were made and their success rates.
    private String callStatusReportPath;
    private String callTimingReportPath;


    public Builder setTableAdminHost(String tableAdminHost) {
      this.tableAdminHost = tableAdminHost;
      return this;
    }

    public Builder setClusterAdminHost(String clusterAdminHost) {
      this.clusterAdminHost = clusterAdminHost;
      return this;
    }

    public Builder setDataHost(String dataHost) {
      this.dataHost = dataHost;
      return this;
    }

    /**
     * Override IP the for all *Hosts.  This is used for Cloud Bigtable to point to a developer's
     * Cloud Bigtable server.
     */
    public Builder setOverrideIp(String overrideIp) {
      this.overrideIp = overrideIp;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder setZoneId(String zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }

    public Builder setCredentialOptions(CredentialOptions credentialOptions) {
      this.credentialOptions = credentialOptions;
      return this;
    }

    public Builder setUserAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public Builder setCallStatusReportPath(String callStatusReportPath) {
      this.callStatusReportPath = callStatusReportPath;
      return this;
    }

    public Builder setCallTimingReportPath(String callTimingReportPath) {
      this.callTimingReportPath = callTimingReportPath;
      return this;
    }

    public Builder setDataChannelCount(int dataChannelCount) {
      this.dataChannelCount = dataChannelCount;
      return this;
    }

    public Builder setRetryOptions(RetryOptions retryOptions) {
      this.retryOptions = retryOptions;
      return this;
    }

    public Builder setTimeoutMs(int timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public BigtableOptions build() {
      return new BigtableOptions(
          clusterAdminHost,
          tableAdminHost,
          dataHost,
          overrideIp,
          port,
          projectId,
          zoneId,
          clusterId,
          credentialOptions,
          userAgent,
          callTimingReportPath,
          callStatusReportPath,
          retryOptions,
          timeoutMs,
          dataChannelCount);
    }
  }

  private final String clusterAdminHost;
  private final String tableAdminHost;
  private final String dataHost;
  private final String overrideIp;
  private final int port;
  private final String projectId;
  private final String zoneId;
  private final String clusterId;
  private final CredentialOptions credentialOptions;
  private final String userAgent;
  private final String callTimingReportPath;
  private final String callStatusReportPath;
  private final RetryOptions retryOptions;
  private final int timeoutMs;
  private final int channelCount;

  @VisibleForTesting
  BigtableOptions() {
      clusterAdminHost = null;
      tableAdminHost = null;
      dataHost = null;
      overrideIp = null;
      port = 0;
      projectId = null;
      zoneId = null;
      clusterId = null;
      credentialOptions = null;
      userAgent = null;
      callTimingReportPath = null;
      callStatusReportPath = null;
      retryOptions = null;
      timeoutMs = 0;
      channelCount = 1;
  }

  private BigtableOptions(
      String clusterAdminHost,
      String tableAdminHost,
      String dataHost,
      String overrideIp,
      int port,
      String projectId,
      String zoneId,
      String clusterId,
      CredentialOptions credentialOptions,
      String userAgent,
      String callTimingReportPath,
      String callStatusReportPath,
      RetryOptions retryOptions,
      int timeoutMs,
      int channelCount) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "ProjectId must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zoneId), "ZoneId must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(clusterId), "ClusterId must not be empty or null.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(userAgent),
        "UserAgent must not be empty or null");
    Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");
    Preconditions.checkArgument(timeoutMs >= -1,
      "ChannelTimeoutMs has to be positive, or -1 for none.");

    this.tableAdminHost = Preconditions.checkNotNull(tableAdminHost);
    this.clusterAdminHost = Preconditions.checkNotNull(clusterAdminHost);
    this.dataHost = Preconditions.checkNotNull(dataHost);
    this.overrideIp = overrideIp;
    this.port = port;
    this.projectId = projectId;
    this.zoneId = zoneId;
    this.clusterId = clusterId;
    this.credentialOptions = credentialOptions;
    this.userAgent = userAgent;
    this.callTimingReportPath = callTimingReportPath;
    this.callStatusReportPath = callStatusReportPath;
    this.retryOptions = retryOptions;
    this.timeoutMs = timeoutMs;
    this.channelCount = channelCount;

    LOG.debug("Connection Configuration: projectId: %s, zoneId: %s, clusterId: %s, data host %s, "
        + "table admin host %s, cluster admin host %s.",
        projectId,
        zoneId,
        clusterId,
        dataHost,
        tableAdminHost,
        clusterAdminHost);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getZoneId() {
    return zoneId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getDataHost() {
    return dataHost;
  }

  public String getTableAdminHost() {
    return tableAdminHost;
  }

  public String getClusterAdminHost() {
    return clusterAdminHost;
  }

  /**
   * Returns an override IP for all *Hosts.  This is used for Cloud Bigtable to point to a
   * developer's Cloud Bigtable server.
   */
  public String getOverrideIp() {
    return overrideIp;
  }

  public int getPort() {
    return port;
  }

  /**
   * Get the credential this object was constructed with. May be null.
   * @return Null to indicate no credentials, otherwise, the Credentials object.
   */
  public CredentialOptions getCredentialOptions() {
    return credentialOptions;
  }

  /**
   * Gets the user-agent to be appended to User-Agent header when creating new streams
   * for the channel.
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Get the client-local file to which a report of call timings will be appended.
   */
  public String getCallTimingReportPath() {
    return callTimingReportPath;
  }

  /**
   * Get the client-local file to which a report of call statuses will be appended.
   */
  public String getCallStatusReportPath() {
    return callStatusReportPath;
  }

  /**
   * Options controlling retries.
   */
  public RetryOptions getRetryOptions() { return retryOptions; }

  /**
   * The timeout for a channel.
   */
  public long getTimeoutMs() {
    return timeoutMs;
  }

  /**
   * The number of channels to create.
   */
  public int getChannelCount() {
    return channelCount;
  }

  public BigtableClusterName getClusterName() {
    return new BigtableClusterName(getProjectId(), getZoneId(), getClusterId());
  }
}
