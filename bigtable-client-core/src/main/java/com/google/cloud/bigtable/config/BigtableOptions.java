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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.google.api.client.util.Objects;
import com.google.cloud.bigtable.grpc.BigtableClusterName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * An immutable class providing access to configuration options for Bigtable.
 */
public class BigtableOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String BIGTABLE_TABLE_ADMIN_HOST_DEFAULT =
      "bigtabletableadmin.googleapis.com";
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT =
      "bigtableclusteradmin.googleapis.com";
  public static final String BIGTABLE_DATA_HOST_DEFAULT = "bigtable.googleapis.com";
  public static final int DEFAULT_BIGTABLE_PORT = 443;

  public static final int BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT = 6;
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

    public Builder() {
    }

    private Builder(BigtableOptions original) {
      this.projectId = original.projectId;
      this.zoneId = original.zoneId;
      this.clusterId = original.clusterId;
      this.userAgent = original.userAgent;
      this.dataHost = original.dataHost;
      this.tableAdminHost = original.tableAdminHost;
      this.clusterAdminHost = original.clusterAdminHost;
      this.port = original.port;
      this.overrideIp = original.overrideIp;
      this.credentialOptions = original.credentialOptions;
      this.retryOptions = original.retryOptions;
      this.timeoutMs = original.timeoutMs;
      this.dataChannelCount = original.dataChannelCount;
    }

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
  private final RetryOptions retryOptions;
  private final int timeoutMs;
  private final int dataChannelCount;
  private final BigtableClusterName clusterName;

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
      retryOptions = null;
      timeoutMs = 0;
      dataChannelCount = 1;
      clusterName = null;
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
      RetryOptions retryOptions,
      int timeoutMs,
      int channelCount) {
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
    this.retryOptions = retryOptions;
    this.timeoutMs = timeoutMs;
    this.dataChannelCount = channelCount;

    if (!Strings.isNullOrEmpty(projectId)
        && !Strings.isNullOrEmpty(zoneId)
        && !Strings.isNullOrEmpty(clusterId)) {
      this.clusterName = new BigtableClusterName(getProjectId(), getZoneId(), getClusterId());
    } else {
      this.clusterName = null;
    }

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
   * The number of data channels to create.
   */
  public int getChannelCount() {
    return dataChannelCount;
  }

  public BigtableClusterName getClusterName() {
    return clusterName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != BigtableOptions.class) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    BigtableOptions other = (BigtableOptions) obj;
    return (port == other.port) && (timeoutMs == other.timeoutMs)
        && (dataChannelCount == other.dataChannelCount)
        && Objects.equal(clusterAdminHost, other.clusterAdminHost)
        && Objects.equal(tableAdminHost, other.tableAdminHost)
        && Objects.equal(dataHost, other.dataHost)
        && Objects.equal(overrideIp, other.overrideIp)
        && Objects.equal(projectId, other.projectId)
        && Objects.equal(zoneId, other.zoneId)
        && Objects.equal(clusterId, other.clusterId)
        && Objects.equal(userAgent, other.userAgent)
        && Objects.equal(credentialOptions, other.credentialOptions)
        && Objects.equal(retryOptions, other.retryOptions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("dataHost", dataHost)
        .add("tableAdminHost", tableAdminHost)
        .add("clusterAdminHost", clusterAdminHost)
        .add("overrideIp", overrideIp)
        .add("projectId", projectId)
        .add("zoneId", zoneId)
        .add("clusterId", clusterId)
        .add("userAgent", userAgent)
        .add("credentialType", credentialOptions.getCredentialType())
        .toString();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }
}
