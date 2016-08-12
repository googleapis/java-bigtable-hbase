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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
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

import com.google.api.client.util.Strings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.async.ResourceLimiter;
import com.google.cloud.bigtable.grpc.async.RpcThrottler;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor;
import com.google.cloud.bigtable.grpc.io.HeaderInterceptor;
import com.google.cloud.bigtable.util.ThreadPoolUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import io.grpc.Attributes;
import io.grpc.ManagedChannel;
import io.grpc.NameResolver;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

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
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableSession implements Closeable {

  private static final Logger LOG = new Logger(BigtableSession.class);
  private static SslContextBuilder sslBuilder;
  private static ResourceLimiter resourceLimiter;

  // 256 MB, server has 256 MB limit.
  private final static int MAX_MESSAGE_SIZE = 1 << 28;

  // 1 MB -- TODO(sduskis): make this configurable
  private final static int FLOW_CONTROL_WINDOW = 1 << 20;

  @VisibleForTesting
  static final String PROJECT_ID_EMPTY_OR_NULL = "ProjectId must not be empty or null.";
  @VisibleForTesting
  static final String INSTANCE_ID_EMPTY_OR_NULL = "InstanceId must not be empty or null.";
  @VisibleForTesting
  static final String USER_AGENT_EMPTY_OR_NULL = "UserAgent must not be empty or null";

  // This is needed when the host address is 
//  private static final DnsNameResolverProvider DNS_NAME_RESOLVER_PROVIDER = new DnsNameResolverProvider();
//  private static final NameResolver.Factory NAMESPACE_FACTORY = new NameResolver.Factory() {
//    
//    @Override
//    public NameResolver newNameResolver(URI targetUri, Attributes params) {
//      return DNS_NAME_RESOLVER_PROVIDER.newNameResolver(targetUri, params);
//    }
//    
//    @Override
//    public String getDefaultScheme() {
//      return DNS_NAME_RESOLVER_PROVIDER.getDefaultScheme();
//    }
//  };

  static {
    performWarmup();
  }

  private synchronized static SslContext createSslContext() throws SSLException {
    if (sslBuilder == null) {
      sslBuilder = GrpcSslContexts.forClient().ciphers(null);
      // gRPC uses tcnative / OpenSsl by default, if it's available.  It defaults to alpn-boot
      // if tcnative is not in the classpath.
      if (OpenSsl.isAvailable()) {
        LOG.info("SslContext: gRPC is using the OpenSSL provider (tcnactive jar - Open Ssl version: %s)",
          OpenSsl.versionString());
      } else {
        if (isJettyAlpnConfigured()) {
          // gRPC uses jetty ALPN as a backup to tcnative.
          LOG.info("SslContext: gRPC is using the JDK provider (alpn-boot jar)");
        } else {
          LOG.info("SslContext: gRPC cannot be configured.  Neither OpenSsl nor Alpn are available.");
        }
      }
    }
    return sslBuilder.build();
  }

  /**
   * <p>isAlpnProviderEnabled.</p>
   *
   * @return a boolean.
   */
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
      BigtableOptions.BIGTABLE_TABLE_ADMIN_HOST_DEFAULT)) {
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

  private synchronized static void initializeResourceLimiter(BigtableOptions options) {
    if (resourceLimiter == null) {
      int maxInflightRpcs = options.getBulkOptions().getMaxInflightRpcs();
      long maxMemory = options.getBulkOptions().getMaxMemory();
      resourceLimiter = new ResourceLimiter(maxMemory, maxInflightRpcs);
    }
  }

  private final BigtableDataClient dataClient;
  private BigtableTableAdminClient tableAdminClient;
  private BigtableInstanceGrpcClient instanceAdminClient;

  private final BigtableOptions options;
  private final List<ManagedChannel> managedChannels = Collections
      .synchronizedList(new ArrayList<ManagedChannel>());
  private final ImmutableList<HeaderInterceptor> headerInterceptors;

  /**
   * <p>Constructor for BigtableSession.</p>
   *
   * @param options a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @throws java.io.IOException if any.
   */
  public BigtableSession(BigtableOptions options) throws IOException {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getProjectId()), PROJECT_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getInstanceId()), INSTANCE_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getUserAgent()), USER_AGENT_EMPTY_OR_NULL);
    LOG.info(
        "Opening connection for projectId %s, instanceId %s, "
        + "on data host %s, table admin host %s.",
        options.getProjectId(), options.getInstanceId(), options.getDataHost(),
        options.getTableAdminHost());
    LOG.info("Bigtable options: %s.", options);
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
    headerInterceptorBuilder.add(
        new GoogleCloudResourcePrefixInterceptor(options.getInstanceName().toString()));
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

    headerInterceptors = headerInterceptorBuilder.build();

    ChannelPool dataChannel = createChannelPool(options.getDataHost());

    BigtableSessionSharedThreadPools sharedPools = BigtableSessionSharedThreadPools.getInstance();

    // More often than not, users want the dataClient. Create a new one in the constructor.
    this.dataClient =
        new BigtableDataGrpcClient(dataChannel, sharedPools.getRetryExecutor(), options);
    dataClient.setCallOptionsFactory(
      new CallOptionsFactory.ConfiguredCallOptionsFactory(options.getCallOptionsConfig()));
    
    // Defer the creation of both the tableAdminClient until we need them.

    initializeResourceLimiter(options);
  }

  /**
   * Use {@link com.google.cloud.bigtable.grpc.BigtableSession#BigtableSession(BigtableOptions)} instead;
   *
   * @param options a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @param batchPool a {@link java.util.concurrent.ExecutorService} object.
   * @throws java.io.IOException if any.
   */
  @Deprecated
  public BigtableSession(
      BigtableOptions options, @SuppressWarnings("unused") ExecutorService batchPool)
      throws IOException {
    this(options);
  }

  /**
   * Use {@link com.google.cloud.bigtable.grpc.BigtableSession#BigtableSession(BigtableOptions)} instead;
   *
   * @param options a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @param batchPool a {@link java.util.concurrent.ExecutorService} object.
   * @param elg a {@link io.netty.channel.EventLoopGroup} object.
   * @param scheduledRetries a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @throws java.io.IOException if any.
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

  /**
   * <p>Getter for the field <code>dataClient</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableDataClient} object.
   */
  public BigtableDataClient getDataClient() {
    return dataClient;
  }

  /**
   * <p>createAsyncExecutor.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor} object.
   */
  public AsyncExecutor createAsyncExecutor() {
    return new AsyncExecutor(dataClient, new RpcThrottler(resourceLimiter));
  }

  /**
   * <p>createBulkMutation.</p>
   *
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   * @param asyncExecutor a {@link com.google.cloud.bigtable.grpc.async.AsyncExecutor} object.
   * @return a {@link com.google.cloud.bigtable.grpc.async.BulkMutation} object.
   */
  public BulkMutation createBulkMutation(BigtableTableName tableName, AsyncExecutor asyncExecutor) {
    return new BulkMutation(
        tableName,
        asyncExecutor,
        options.getRetryOptions(),
        BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(),
        options.getBulkOptions().getBulkMaxRowKeyCount(),
        options.getBulkOptions().getBulkMaxRequestSize());
  }

  /**
   * <p>createBulkRead.</p>
   *
   * @param tableName a {@link com.google.cloud.bigtable.grpc.BigtableTableName} object.
   * @return a {@link com.google.cloud.bigtable.grpc.async.BulkRead} object.
   */
  public BulkRead createBulkRead(BigtableTableName tableName) {
    return new BulkRead(dataClient, tableName);
  }

  /**
   * <p>Getter for the field <code>tableAdminClient</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableTableAdminClient} object.
   * @throws java.io.IOException if any.
   */
  public synchronized BigtableTableAdminClient getTableAdminClient() throws IOException {
    if (tableAdminClient == null) {
      ManagedChannel channel = createChannelPool(options.getTableAdminHost());
      tableAdminClient = new BigtableTableAdminGrpcClient(channel);
    }
    return tableAdminClient;
  }

  /**
   * <p>Getter for the field <code>instanceAdminClient</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.grpc.BigtableInstanceClient} object.
   * @throws java.io.IOException if any.
   */
  public synchronized BigtableInstanceClient getInstanceAdminClient() throws IOException {
    if (instanceAdminClient == null) {
      ManagedChannel channel = createChannelPool(options.getInstanceAdminHost());
      instanceAdminClient = new BigtableInstanceGrpcClient(channel);
    }
    return instanceAdminClient;
  }

  /**
   * <p>
   * Create a new {@link com.google.cloud.bigtable.grpc.io.ChannelPool} of the given size, with auth headers and user agent interceptors.
   * </p>
   *
   * @param hostString a {@link java.lang.String} object.
   * @return a {@link com.google.cloud.bigtable.grpc.io.ChannelPool} object.
   * @throws java.io.IOException if any.
   */
  protected ChannelPool createChannelPool(final String hostString) throws IOException {
    // TODO Go back to using host names once more extensive testing of the IPv6 issues.
    final InetSocketAddress serverAddress =
        new InetSocketAddress(InetAddress.getByName(hostString), options.getPort());
    ChannelPool.ChannelFactory channelFactory = new ChannelPool.ChannelFactory() {
      @Override
      public ManagedChannel create() throws IOException {
        return createNettyChannel(serverAddress, options);
      }
    };
    ChannelPool channelPool = new ChannelPool(headerInterceptors, channelFactory);
    managedChannels.add(channelPool);
    return channelPool;
  }

  /**
   * <p>createNettyChannel.</p>
   *
   * @param host a {@link java.lang.String} object.
   * @param options a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   * @return a {@link io.grpc.ManagedChannel} object.
   * @throws java.io.IOException if any.
   */
  public static ManagedChannel createNettyChannel(String host, BigtableOptions options) throws IOException {
    // TODO Go back to using host names once more extensive testing of the IPv6 issues.
    InetAddress address = InetAddress.getByName(host);
    InetSocketAddress serverAddress = new InetSocketAddress(address, options.getPort());
    return createNettyChannel(serverAddress, options);
  }

  private static ManagedChannel createNettyChannel(InetSocketAddress serverAddress,
      BigtableOptions options) throws SSLException {
    NegotiationType negotiationType = options.usePlaintextNegotiation() ?
        NegotiationType.PLAINTEXT : NegotiationType.TLS;
    BigtableSessionSharedThreadPools sharedPools = BigtableSessionSharedThreadPools.getInstance();
    return NettyChannelBuilder
        .forAddress(serverAddress)
        .maxMessageSize(MAX_MESSAGE_SIZE)
        .sslContext(createSslContext())
        .eventLoopGroup(sharedPools.getElg())
        .executor(sharedPools.getBatchThreadPool())
        .negotiationType(negotiationType)
        .userAgent(options.getUserAgent())
        .flowControlWindow(FLOW_CONTROL_WINDOW)
        .build();
  }

  /** {@inheritDoc} */
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

  /**
   * <p>Getter for the field <code>options</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BigtableOptions} object.
   */
  public BigtableOptions getOptions() {
    return this.options;
  }
}
