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

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.hbase.ServerName;

import com.google.api.client.util.Strings;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.hbase.ChannelOptions;
import com.google.cloud.hadoop.hbase.TransportOptions;
import com.google.common.base.Preconditions;

/**
 * An immutable class providing access to configuration options for Bigtable.
 */
public class BigtableOptions {

  private static final Logger LOG = new Logger(BigtableOptions.class);

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {
    private String projectId;
    private String zone;
    private String cluster;
    private Credentials credential;
    private InetAddress host;
    private InetAddress adminHost;
    private InetAddress clusterAdminHost;
    private int port;
    private String callTimingReportPath;
    private String callStatusReportPath;
    private boolean retriesEnabled;
    private ScheduledExecutorService rpcRetryExecutorService;
    private EventLoopGroup customEventLoopGroup;

    public Builder setAdminHost(InetAddress host) {
      this.adminHost = host;
      return this;
    }

    public Builder setClusterAdminHost(InetAddress host) {
      this.clusterAdminHost = host;
      return this;
    }

    public Builder setHost(InetAddress host) {
      this.host = host;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setCredential(Credentials credential) {
      this.credential = credential;
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

    public Builder setCallTimingReportPath(String callTimingReportPath) {
      this.callTimingReportPath = callTimingReportPath;
      return this;
    }

    public Builder setCallStatusReportPath(String callStatusReportPath) {
      this.callStatusReportPath = callStatusReportPath;
      return this;
    }

    public Builder setRetriesEnabled(boolean retriesEnabled) {
      this.retriesEnabled = retriesEnabled;
      return this;
    }

    public Builder setRpcRetryExecutorService(ScheduledExecutorService scheduledExecutorService) {
      this.rpcRetryExecutorService = scheduledExecutorService;
      return this;
    }

    public Builder setCustomEventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.customEventLoopGroup = eventLoopGroup;
      return this;
    }

    public BigtableOptions build() {
      if (adminHost == null) {
        adminHost = host;
      }

      return new BigtableOptions(
          clusterAdminHost,
          adminHost,
          host,
          port,
          credential,
          projectId,
          zone,
          cluster,
          retriesEnabled,
          callTimingReportPath,
          callStatusReportPath,
          rpcRetryExecutorService,
          customEventLoopGroup);
    }
  }

  private final InetAddress clusterAdminHost;
  private final InetAddress adminHost;
  private final InetAddress host;
  private final int port;
  private final Credentials credential;
  private final String projectId;
  private final String zone;
  private final String cluster;
  private final boolean retriesEnabled;
  private final String callTimingReportPath;
  private final String callStatusReportPath;
  private final ScheduledExecutorService rpcRetryExecutorService;
  private final EventLoopGroup customEventLoopGroup;


  public BigtableOptions(
      InetAddress clusterAdminHost,
      InetAddress adminHost,
      InetAddress host,
      int port,
      Credentials credential,
      String projectId,
      String zone,
      String cluster,
      boolean retriesEnabled,
      String callTimingReportPath,
      String callStatusReportPath,
      ScheduledExecutorService rpcRetryExecutorService,
      EventLoopGroup customEventLoopGroup) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "ProjectId must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zone), "Zone must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(cluster), "Cluster must not be empty or null.");
    this.adminHost = Preconditions.checkNotNull(adminHost);
    this.clusterAdminHost = clusterAdminHost;
    this.host = Preconditions.checkNotNull(host);
    this.port = port;
    this.credential = credential;
    this.projectId = projectId;
    this.retriesEnabled = retriesEnabled;
    this.callTimingReportPath = callTimingReportPath;
    this.callStatusReportPath = callStatusReportPath;
    this.zone = zone;
    this.cluster = cluster;
    this.rpcRetryExecutorService = rpcRetryExecutorService;
    this.customEventLoopGroup = customEventLoopGroup;

    LOG.info("Connection Configuration: project: %s, cluster: %s, host:port %s:%s, "
        + "admin host:port %s:%s, using transport %s.",
        getProjectId(),
        cluster,
        host,
        port,
        adminHost,
        443,
        TransportOptions.AnviltopTransports.HTTP2_NETTY_TLS);
    if (clusterAdminHost != null) {
      LOG.info("Cluster API host: %s" , clusterAdminHost);
    }
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

  public ChannelOptions getChannelOptions() {
    ChannelOptions.Builder optionsBuilder = new ChannelOptions.Builder();
    optionsBuilder.setCallTimingReportPath(callTimingReportPath);
    optionsBuilder.setCallStatusReportPath(callStatusReportPath);
    optionsBuilder.setCredential(credential);
    optionsBuilder.setEnableRetries(retriesEnabled);
    optionsBuilder.setScheduledExecutorService(rpcRetryExecutorService);
    return optionsBuilder.build();
  }

  public TransportOptions getTransportOptions() throws IOException {
    return createTransportOptions(this.host);
  }

  public TransportOptions getAdminTransportOptions() throws IOException {
    return createTransportOptions(this.adminHost);
  }

  public TransportOptions getClusterAdminTransportOptions() throws IOException {
    return createTransportOptions(this.clusterAdminHost);
  }

  private TransportOptions createTransportOptions(InetAddress host) throws IOException {
    // TODO: Should sslContext be cached?
    SslContext sslContext = SslContext.newClientContext();
    return new TransportOptions(
        TransportOptions.AnviltopTransports.HTTP2_NETTY_TLS,
        host,
        port,
        sslContext,
        customEventLoopGroup);
  }

  public ServerName getServerName() {
    return ServerName.valueOf(host.getHostName(), port, 0);
  }
}
