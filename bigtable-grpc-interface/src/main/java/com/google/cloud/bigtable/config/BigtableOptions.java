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
package com.google.cloud.bigtable.config;

import java.net.InetAddress;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.api.client.util.Strings;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.grpc.RetryOptions;
import com.google.cloud.bigtable.grpc.TransportOptions;
import com.google.common.base.Preconditions;

/**
 * An immutable class providing access to configuration options for Bigtable.
 */
public class BigtableOptions {

  public static final String BIGTABLE_TABLE_ADMIN_HOST_DEFAULT =
      "bigtabletableadmin.googleapis.com";
  public static final String BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT =
      "bigtableclusteradmin.googleapis.com";
  public static final String BIGTABLE_HOST_DEFAULT = "bigtable.googleapis.com";
  public static final int DEFAULT_BIGTABLE_PORT = 443;

  private static final Logger LOG = new Logger(BigtableOptions.class);

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {
    private String projectId;
    private String zone;
    private String cluster;
    private InetAddress dataHost;
    private InetAddress tableAdminHost;
    private InetAddress clusterAdminHost;
    private int port;
    private Credentials credential;
    private String authority;
    private String userAgent;
    private String callStatusReportPath;
    private String callTimingReportPath;
    private RetryOptions retryOptions = null;
    private long timeoutMs = 0;
    private int channelCount = 1;

    public Builder setTableAdminHost(InetAddress tableAdminHost) {
      this.tableAdminHost = tableAdminHost;
      return this;
    }

    public Builder setClusterAdminHost(InetAddress clusterAdminHost) {
      this.clusterAdminHost = clusterAdminHost;
      return this;
    }

    public Builder setDataHost(InetAddress dataHost) {
      this.dataHost = dataHost;
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

    public Builder setZone(String zone) {
      this.zone = zone;
      return this;
    }

    public Builder setCluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder setCredential(Credentials credential) {
      this.credential = credential;
      return this;
    }

    public Builder setAuthority(String authority) {
      this.authority = authority;
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

    public Builder setChannelCount(int channelCount) {
      this.channelCount  = channelCount;
      return this;
    }

    public Builder setRetryOptions(RetryOptions retryOptions) {
      this.retryOptions = retryOptions;
      return this;
    }

    public Builder setTimeoutMs(long timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public BigtableOptions build() {
      return new BigtableOptions(
          clusterAdminHost,
          tableAdminHost,
          dataHost,
          port,
          projectId,
          zone,
          cluster,
          credential,
          authority,
          userAgent,
          callTimingReportPath,
          callStatusReportPath,
          retryOptions,
          timeoutMs,
          channelCount);
    }
  }

  private final InetAddress clusterAdminHost;
  private final InetAddress tableAdminHost;
  private final InetAddress dataHost;
  private final int port;
  private final String projectId;
  private final String zone;
  private final String cluster;
  private final Credentials credential;
  private final String authority;
  private final String userAgent;
  private final String callTimingReportPath;
  private final String callStatusReportPath;
  private final RetryOptions retryOptions;
  private final long timeoutMs;
  private final int channelCount;

  @VisibleForTesting
  BigtableOptions() {
      clusterAdminHost = null;
      tableAdminHost = null;
      dataHost = null;
      port = 0;
      projectId = null;
      zone = null;
      cluster = null;
      credential = null;
      authority = null;
      userAgent = null;
      callTimingReportPath = null;
      callStatusReportPath = null;
      retryOptions = null;
      timeoutMs = 0;
      channelCount = 1;
  }

  private BigtableOptions(
      InetAddress clusterAdminHost,
      InetAddress tableAdminHost,
      InetAddress dataHost,
      int port,
      String projectId,
      String zone,
      String cluster,
      Credentials credential,
      String authority,
      String userAgent,
      String callTimingReportPath,
      String callStatusReportPath,
      RetryOptions retryOptions,
      long timeoutMs,
      int channelCount) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "ProjectId must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zone), "Zone must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(cluster), "Cluster must not be empty or null.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(userAgent),
        "UserAgent must not be empty or null");
    Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");
    Preconditions.checkArgument(timeoutMs >= -1,
      "ChannelTimeoutMs has to be positive, or -1 for none.");

    this.tableAdminHost = Preconditions.checkNotNull(tableAdminHost);
    this.clusterAdminHost = Preconditions.checkNotNull(clusterAdminHost);
    this.dataHost = Preconditions.checkNotNull(dataHost);
    this.port = port;
    this.projectId = projectId;
    this.zone = zone;
    this.cluster = cluster;
    this.credential = credential;
    this.authority = authority;
    this.userAgent = userAgent;
    this.callTimingReportPath = callTimingReportPath;
    this.callStatusReportPath = callStatusReportPath;
    this.retryOptions = retryOptions;
    this.timeoutMs = timeoutMs;
    this.channelCount = channelCount;

    LOG.debug("Connection Configuration: project: %s, cluster: %s, data host %s, "
        + "table admin host %s, cluster admin host %s using transport %s.",
        projectId,
        cluster,
        dataHost,
        tableAdminHost,
        clusterAdminHost,
        TransportOptions.BigtableTransports.HTTP2_NETTY_TLS);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getZone() {
    return zone;
  }

  public String getCluster() {
    return cluster;
  }

  public InetAddress getDataHost() {
    return dataHost;
  }

  public InetAddress getTableAdminHost() {
    return tableAdminHost;
  }

  public InetAddress getClusterAdminHost() {
    return clusterAdminHost;
  }

  public int getPort() {
    return port;
  }

  /**
   * Get the credential this object was constructed with. May be null.
   * @return Null to indicate no credentials, otherwise, the Credentials object.
   */
  public Credentials getCredential() {
    return credential;
  }

  /**
   * Gets the authority to be passed in the HTTP/2 headers when creating new streams
   * for the channel.
   */
  public String getAuthority() {
    return authority;
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
}
