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

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.net.InetAddress;

import javax.net.ssl.SSLException;

import com.google.api.client.util.Strings;
import com.google.cloud.bigtable.grpc.ChannelOptions;
import com.google.cloud.bigtable.grpc.TransportOptions;
import com.google.common.base.Preconditions;

/**
 * An immutable class providing access to configuration options for Bigtable.
 */
public class BigtableOptions {

  public static final TransportOptions.SslContextFactory SSL_CONTEXT_FACTORY =
      new TransportOptions.SslContextFactory() {
        @SuppressWarnings("deprecation")
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
      };

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
    private EventLoopGroup customEventLoopGroup;
    private ChannelOptions channelOptions;

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

    public Builder setCustomEventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.customEventLoopGroup = eventLoopGroup;
      return this;
    }

    public Builder setChannelOptions(ChannelOptions channelOptions) {
      this.channelOptions = channelOptions;
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
          customEventLoopGroup,
          channelOptions);
    }
  }

  private final InetAddress clusterAdminHost;
  private final InetAddress tableAdminHost;
  private final InetAddress dataHost;
  private final int port;
  private final String projectId;
  private final String zone;
  private final String cluster;
  private final EventLoopGroup customEventLoopGroup;
  private final ChannelOptions channelOptions;

  private BigtableOptions(
      InetAddress clusterAdminHost,
      InetAddress tableAdminHost,
      InetAddress dataHost,
      int port,
      String projectId,
      String zone,
      String cluster,
      EventLoopGroup customEventLoopGroup,
      ChannelOptions channelOptions) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "ProjectId must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(zone), "Zone must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(cluster), "Cluster must not be empty or null.");
    Preconditions.checkNotNull(channelOptions, "ChannelCoptions must not be empty or null.");
    this.tableAdminHost = Preconditions.checkNotNull(tableAdminHost);
    this.clusterAdminHost = Preconditions.checkNotNull(clusterAdminHost);
    this.dataHost = Preconditions.checkNotNull(dataHost);
    this.port = port;
    this.projectId = projectId;
    this.zone = zone;
    this.cluster = cluster;
    this.customEventLoopGroup = customEventLoopGroup;
    this.channelOptions = channelOptions;

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

  public ChannelOptions getChannelOptions() {
    return channelOptions;
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

  public TransportOptions getDataTransportOptions() {
    return createTransportOptions(this.dataHost);
  }

  public TransportOptions getTableAdminTransportOptions() {
    return createTransportOptions(this.tableAdminHost);
  }

  public TransportOptions getClusterAdminTransportOptions() {
    return createTransportOptions(this.clusterAdminHost);
  }

  private TransportOptions createTransportOptions(InetAddress host) {
    return new TransportOptions(
        TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
        host,
        port,
        SSL_CONTEXT_FACTORY,
        customEventLoopGroup);
  }

  public int getPort() {
    return port;
  }
}
