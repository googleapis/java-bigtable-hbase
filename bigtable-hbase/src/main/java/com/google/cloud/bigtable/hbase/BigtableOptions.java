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

import com.google.api.client.util.Strings;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.grpc.ChannelOptions;
import com.google.cloud.bigtable.grpc.TransportOptions;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.ServerName;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;

import javax.net.ssl.SSLException;

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
    private InetAddress dataHost;
    private InetAddress tableAdminHost;
    private InetAddress clusterAdminHost;
    private int port;
    private String callTimingReportPath;
    private String callStatusReportPath;
    private boolean retriesEnabled;
    private boolean retryOnDeadlineExceeded;
    private ScheduledExecutorService rpcRetryExecutorService;
    private EventLoopGroup customEventLoopGroup;
    private int channelCount = 1;
    private long timeoutMs = -1L;

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

    public Builder setRetryOnDeadlineExceeded(boolean retryOnDeadlineExceeded) {
      this.retryOnDeadlineExceeded = retryOnDeadlineExceeded;
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

    public Builder setChannelCount(int channelCount) {
      Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");
      this.channelCount = channelCount;
      return this;
    }

    public Builder setChannelTimeoutMs(long timeoutMs) {
      Preconditions.checkArgument(timeoutMs >= -1,
        "ChannelTimeoutMs has to be positive, or -1 for none.");
      this.timeoutMs = timeoutMs;
      return this;
    }

    public BigtableOptions build() {
      return new BigtableOptions(
          clusterAdminHost,
          tableAdminHost,
          dataHost,
          port,
          credential,
          projectId,
          zone,
          cluster,
          retriesEnabled,
          retryOnDeadlineExceeded,
          callTimingReportPath,
          callStatusReportPath,
          rpcRetryExecutorService,
          customEventLoopGroup,
          channelCount,
          timeoutMs);
    }
  }

  private final InetAddress clusterAdminHost;
  private final InetAddress tableAdminHost;
  private final InetAddress dataHost;
  private final int port;
  private final Credentials credential;
  private final String projectId;
  private final String zone;
  private final String cluster;
  private final boolean retriesEnabled;
  private final boolean retryOnDeadlineExceeded;
  private final String callTimingReportPath;
  private final String callStatusReportPath;
  private final ScheduledExecutorService rpcRetryExecutorService;
  private final EventLoopGroup customEventLoopGroup;
  private final int channelCount;
  private final long timeoutMs;

  private BigtableOptions(
      InetAddress clusterAdminHost,
      InetAddress tableAdminHost,
      InetAddress dataHost,
      int port,
      Credentials credential,
      String projectId,
      String zone,
      String cluster,
      boolean retriesEnabled,
      boolean retryOnDeadlineExceeded,
      String callTimingReportPath,
      String callStatusReportPath,
      ScheduledExecutorService rpcRetryExecutorService,
      EventLoopGroup customEventLoopGroup,
      int channelCount,
      long timeoutMs) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "ProjectId must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zone), "Zone must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(cluster), "Cluster must not be empty or null.");
    this.tableAdminHost = Preconditions.checkNotNull(tableAdminHost);
    this.clusterAdminHost = Preconditions.checkNotNull(clusterAdminHost);
    this.dataHost = Preconditions.checkNotNull(dataHost);
    this.port = port;
    this.credential = credential;
    this.projectId = projectId;
    this.retriesEnabled = retriesEnabled;
    this.retryOnDeadlineExceeded = retryOnDeadlineExceeded;
    this.callTimingReportPath = callTimingReportPath;
    this.callStatusReportPath = callStatusReportPath;
    this.zone = zone;
    this.cluster = cluster;
    this.rpcRetryExecutorService = rpcRetryExecutorService;
    this.customEventLoopGroup = customEventLoopGroup;
    this.channelCount = channelCount;
    this.timeoutMs = timeoutMs;

    LOG.debug("Connection Configuration: project: %s, cluster: %s, data host %s, "
        + "table admin host %s, cluster admin host %s using transport %s.",
        getProjectId(),
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

  public ChannelOptions getChannelOptions() {
    ChannelOptions.Builder optionsBuilder = new ChannelOptions.Builder();
    optionsBuilder.setCallTimingReportPath(callTimingReportPath);
    optionsBuilder.setCallStatusReportPath(callStatusReportPath);
    optionsBuilder.setCredential(credential);
    optionsBuilder.setEnableRetries(retriesEnabled);
    optionsBuilder.setRetryOnDeadlineExceeded(retryOnDeadlineExceeded);
    optionsBuilder.setScheduledExecutorService(rpcRetryExecutorService);
    optionsBuilder.setChannelCount(channelCount);
    optionsBuilder.setTimeoutMs(timeoutMs);
    return optionsBuilder.build();
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

  public TransportOptions getDataTransportOptions() throws IOException {
    return createTransportOptions(this.dataHost);
  }

  public TransportOptions getTableAdminTransportOptions() throws IOException {
    return createTransportOptions(this.tableAdminHost);
  }

  public TransportOptions getClusterAdminTransportOptions() throws IOException {
    return createTransportOptions(this.clusterAdminHost);
  }

  private TransportOptions createTransportOptions(InetAddress host) throws IOException {
    return new TransportOptions(
        TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
        host,
        port,
        new TransportOptions.SslContextFactory() {
          @Override
          public SslContext create() {
            try {
              // We create multiple channels via refreshing and pooling channel implementation.
              // Each one needs its own SslContext.
              return SslContext.newClientContext();
            } catch (SSLException e) {
              throw new IllegalStateException("Could not create an ssl context.", e);
            }
          }
        },
        customEventLoopGroup);
  }

  public ServerName getServerName() {
    return ServerName.valueOf(dataHost.getHostName(), port, 0);
  }

  public int getChannelCount() {
    return channelCount;
  }
}
