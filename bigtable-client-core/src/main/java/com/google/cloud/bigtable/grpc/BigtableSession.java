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

package com.google.cloud.bigtable.grpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.api.client.util.Strings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.cloud.bigtable.grpc.io.HeaderInterceptor;
import com.google.cloud.bigtable.grpc.io.UserAgentInterceptor;
import com.google.cloud.bigtable.util.ThreadPoolUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * <p>Encapsulates the creation of Bigtable Grpc services.</p>
 *
 * <p>The following functionality is handled by this class:
 * <ol>
 *   <li> Created Executors
 *   <li> Creates Channels - netty ChannelImpls, ReconnectingChannel and ChannelPools
 *   <li> Creates ChannelInterceptors - auth headers, performance interceptors.
 *   <li> Close anything above that needs to be closed (ExecutorService, CahnnelImpls)
 * </ol>
 */
public class BigtableSession implements Closeable {

  private static final Logger LOG = new Logger(BigtableSession.class);
  private static SslContextBuilder sslBuilder;

  // 256 MB, server has 256 MB limit.
  private final static int MAX_MESSAGE_SIZE = 1 << 28; 

  // 1 MB -- TODO(sduskis): make this configurable
  private final static int FLOW_CONTROL_WINDOW = 1 << 20;

  @VisibleForTesting
  static final String PROJECT_ID_EMPTY_OR_NULL = "ProjectId must not be empty or null.";
  @VisibleForTesting
  static final String ZONE_ID_EMPTY_OR_NULL = "ZoneId must not be empty or null.";
  @VisibleForTesting
  static final String CLUSTER_ID_EMPTY_OR_NULL = "ClusterId must not be empty or null.";
  @VisibleForTesting
  static final String USER_AGENT_EMPTY_OR_NULL = "UserAgent must not be empty or null";

  static {
    performWarmup();
  }

  private synchronized static SslContext createSslContext() throws SSLException {
    if (sslBuilder == null) {
      sslBuilder = GrpcSslContexts.forClient().ciphers(null);
      // gRPC uses tcnative / OpenSsl by default, if it's available.  It defaults to alpn-boot
      // if tcnative is not in the classpath.
      if (OpenSsl.isAvailable()) {
        LOG.info("gRPC is using the OpenSSL provider (tcnactive jar - Open Ssl version: %s)",
          OpenSsl.versionString());
      } else {
        if (isJettyAlpnConfigured()) {
          // gRPC uses jetty ALPN as a backup to tcnative.
          LOG.info("gRPC is using the JDK provider (alpn-boot jar)");
        } else {
          LOG.info("gRPC cannot be configured.  Neither OpenSsl nor Alpn are available.");
        }
      }
    }
    return sslBuilder.build();
  }

  public static boolean isAlpnProviderEnabled() {
    final boolean openSslAvailable = OpenSsl.isAvailable();
    final boolean jettyAlpnConfigured = isJettyAlpnConfigured();
    LOG.debug("OpenSSL available: %s", openSslAvailable);
    LOG.debug("Jetty ALPN available: %s", jettyAlpnConfigured);
    return openSslAvailable || jettyAlpnConfigured;
  }

  /**
   * Indicates whether or not the Jetty ALPN jar is installed in the boot classloader.
   */
  private static boolean isJettyAlpnConfigured() {
    final String alpnClassName = "org.eclipse.jetty.alpn.ALPN";
    try {
      Class.forName(alpnClassName, true, null);
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Could not resolve alpn class: %s", e, alpnClassName);
      return false;
    }
  }

  private static void performWarmup() {
    // Initialize some core dependencies in parallel.  This can speed up startup by 150+ ms.
    ExecutorService connectionStartupExecutor = Executors
        .newCachedThreadPool(ThreadPoolUtil.createThreadFactory("BigtableSession-startup"));

    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of createSslContext() is expensive.
        // Create a throw away object in order to speed up the creation of the first
        // BigtableConnection which uses SslContexts under the covers.
        if (isAlpnProviderEnabled()) {
          try {
            // We create multiple channels via refreshing and pooling channel implementation.
            // Each one needs its own SslContext.
            createSslContext();
          } catch (SSLException e) {
            LOG.warn("Could not asynchronously create the ssl context", e);
          }
        }
      }
    });
    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of CredentialFactory.getHttpTransport() is expensive.
        // Reference it so that it gets constructed asynchronously.
        try {
          CredentialFactory.getHttpTransport();
        } catch (IOException | GeneralSecurityException e) {
          LOG.warn("Could not asynchronously initialze httpTransport", e);
        }
      }
    });
    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of BigtableSessionSharedThreadPools.getInstance() is expensive.
        // Reference it so that it gets constructed asynchronously.
        BigtableSessionSharedThreadPools.getInstance();
      }
    });
    for (final String host : Arrays.asList(BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT,
      BigtableOptions.BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT,
      BigtableOptions.BIGTABLE_CLUSTER_ADMIN_HOST_DEFAULT)) {
      connectionStartupExecutor.execute(new Runnable() {
        @Override
        public void run() {
          // The first invocation of InetAddress retrieval is expensive.
          // Reference it so that it gets constructed asynchronously.
          try {
            InetAddress.getByName(host);
          } catch (UnknownHostException e) {
            LOG.warn("Could not asynchronously initialze host: " + host, e);
          }
        }
      });
    }
    connectionStartupExecutor.shutdown();
  }

  private BigtableDataClient dataClient;
  private BigtableTableAdminClient tableAdminClient;
  private BigtableClusterAdminClient clusterAdminClient;

  private final BigtableOptions options;
  private final List<ManagedChannel> managedChannels = Collections
      .synchronizedList(new ArrayList<ManagedChannel>());
  private final ImmutableList<HeaderInterceptor> headerInterceptors;

  public BigtableSession(BigtableOptions options) throws IOException {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getProjectId()), PROJECT_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getZoneId()), ZONE_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getClusterId()), CLUSTER_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getUserAgent()), USER_AGENT_EMPTY_OR_NULL);
    LOG.info(
        "Opening connection for projectId %s, zoneId %s, clusterId %s, "
        + "on data host %s, table admin host %s.",
        options.getProjectId(), options.getZoneId(), options.getClusterId(), options.getDataHost(),
        options.getTableAdminHost());
    if (!isAlpnProviderEnabled()) {
      LOG.error(
          "Neither Jetty ALPN nor OpenSSL are available. "
          + "OpenSSL unavailability cause:\n%s",
          OpenSsl.unavailabilityCause().toString());
      throw new IllegalStateException("Neither Jetty ALPN nor OpenSSL via "
          + "netty-tcnative were properly configured.");
    }
    this.options = options;

    Builder<HeaderInterceptor> headerInterceptorBuilder = new ImmutableList.Builder<>();

    // Looking up Credentials takes time. Creating the retry executor and the EventLoopGroup don't
    // take as long, but still take time. Get the credentials on one thread, and start up the elg
    // and scheduledRetries thread pools on another thread.
    CredentialInterceptorCache credentialsCache = CredentialInterceptorCache.getInstance();
    RetryOptions retryOptions = options.getRetryOptions();
    CredentialOptions credentialOptions = options.getCredentialOptions();
    try {
      HeaderInterceptor headerInterceptor =
          credentialsCache.getCredentialsInterceptor(credentialOptions, retryOptions);
      if (headerInterceptor != null) {
        headerInterceptorBuilder.add(headerInterceptor);
      }
    } catch (GeneralSecurityException e) {
      throw new IOException("Could not initialize credentials.", e);
    }

    headerInterceptorBuilder.add(new UserAgentInterceptor(options.getUserAgent()));
    headerInterceptors = headerInterceptorBuilder.build();

    ChannelPool dataChannel = createChannelPool(options.getDataHost());

    BigtableSessionSharedThreadPools sharedPools = BigtableSessionSharedThreadPools.getInstance();

    // More often than not, users want the dataClient. Create a new one in the constructor.
    this.dataClient =
        new BigtableDataGrpcClient(dataChannel, sharedPools.getRetryExecutor(), options);

    // Defer the creation of both the tableAdminClient and clusterAdminClient until we need them.
  }

  /**
   * Use {@link BigtableSession#BigtableSession(BigtableOptions)} instead;
   */
  @Deprecated
  public BigtableSession(
      BigtableOptions options, @SuppressWarnings("unused") ExecutorService batchPool)
      throws IOException {
    this(options);
  }

  /**
   * Use {@link BigtableSession#BigtableSession(BigtableOptions)} instead;
   */
  @Deprecated
  public BigtableSession(
      BigtableOptions options,
      @SuppressWarnings("unused") @Nullable ExecutorService batchPool,
      @Nullable EventLoopGroup elg,
      @Nullable ScheduledExecutorService scheduledRetries)
      throws IOException {
    this(options);
    if (elg != null) {
      // There used to be a default EventLoopGroup if the elg is not passed in.
      // AbstractBigtableConnection never sent one in, but some users may have. With the shared
      // threadpools, the user supplied EventLoopGroup is no longer used. Previously, the elg would
      // have been shut down in BigtableSession.close(), so shut it here.
      elg.shutdown();
    }
    if (scheduledRetries != null) {
      // Just like the EventLoopGroup, the schedule retries threadpool used to be a user feature
      // that wasn't often used. It was shutdown in the close() method. Since it's no longer used,
      // shut it down immediately.
      scheduledRetries.shutdown();
    }
  }

  public BigtableDataClient getDataClient() {
    return dataClient;
  }

  public synchronized BigtableTableAdminClient getTableAdminClient() throws IOException {
    if (tableAdminClient == null) {
      ManagedChannel channel = createChannelPool(options.getTableAdminHost());
      tableAdminClient = new BigtableTableAdminGrpcClient(channel);
    }
    return tableAdminClient;
  }

  public synchronized BigtableClusterAdminClient getClusterAdminClient() throws IOException {
    if (this.clusterAdminClient == null) {
      ManagedChannel channel = createChannelPool(options.getClusterAdminHost());
      this.clusterAdminClient = new BigtableClusterAdminGrpcClient(channel);
    }

    return clusterAdminClient;
  }

  /**
   * <p>
   * Create a new {@link ChannelPool} of the given size, with auth headers and user agent interceptors.
   * </p>
   */
  protected ChannelPool createChannelPool(final String hostString) throws IOException {
    ChannelPool.ChannelFactory channelFactory = new ChannelPool.ChannelFactory() {
      @Override
      public ManagedChannel create() throws IOException {
        return createNettyChannel(hostString);
      }
    };
    ChannelPool channelPool = new ChannelPool(headerInterceptors, channelFactory);
    managedChannels.add(channelPool);
    return channelPool;
  }

  protected ManagedChannel createNettyChannel(final String host) throws IOException {
    // TODO Go back to using host names once grpc 0.14.0 is out, which fixes bug
    // when ipv6 address is available but not reachable.
    InetAddress address = InetAddress.getByName(host);
    BigtableSessionSharedThreadPools sharedPools = BigtableSessionSharedThreadPools.getInstance();
    return NettyChannelBuilder
        .forAddress(new InetSocketAddress(address, options.getPort()))
        .maxMessageSize(MAX_MESSAGE_SIZE)
        .sslContext(createSslContext())
        .eventLoopGroup(sharedPools.getElg())
        .executor(sharedPools.getBatchThreadPool())
        .negotiationType(NegotiationType.TLS)
        .flowControlWindow(FLOW_CONTROL_WINDOW)
        .build();
  }

  @Override
  public synchronized void close() throws IOException {
    if (managedChannels.isEmpty()) {
      return;
    }
    long timeoutNanos = TimeUnit.SECONDS.toNanos(10);
    long endTimeNanos = System.nanoTime() + timeoutNanos;
    for (ManagedChannel channel : managedChannels) {
      channel.shutdown();
    }
    for (ManagedChannel channel : managedChannels) {
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      try {
        channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing the channelPools");
      }
    }
    for (ManagedChannel channel : managedChannels) {
      if (!channel.isTerminated()) {
        // Sometimes, gRPC channels don't close properly. We cannot explain why that happens,
        // nor can we reproduce the problem reliably. However, that doesn't actually cause
        // problems. Synchronous RPCs will throw exceptions right away. Buffered Mutator based
        // async operations are already logged. Direct async operations may have some trouble,
        // but users should not currently be using them directly.
        //
        // NOTE: We haven't seen this problem since removing the RefreshingChannel
        LOG.info("Could not close the channel after 10 seconds.");
        break;
      }
    }
    managedChannels.clear();
  }
}
