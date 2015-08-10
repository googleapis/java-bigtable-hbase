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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.auth.Credentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.bigtable.admin.table.v1.BigtableTableServiceGrpc;
import com.google.bigtable.admin.table.v1.BigtableTableServiceGrpc.BigtableTableServiceServiceDescriptor;
import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.BigtableServiceGrpc.BigtableServiceServiceDescriptor;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;
import io.grpc.ChannelImpl;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.MethodDescriptor;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.transport.netty.NegotiationType;
import io.grpc.transport.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

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
public class BigtableSession implements AutoCloseable {

  public static final String BATCH_POOL_THREAD_NAME = "bigtable-batch-pool";

  /** Entry in call reports that indicates an entry is from before retries */
  private static final String PRE_RETRY_REPORT_ENTRY = "PreRetry";
  /** Entry in call reports that indicates an entry is from after retries */
  private static final String POST_RETRY_REPORT_ENTRY = "PostRetry";
  public static final String RETRY_THREADPOOL_NAME = "bigtable-rpc-retry";
  /** Number of threads to use to initiate retry calls */
  public static final int RETRY_THREAD_COUNT = 4;
  public static final String GRPC_EVENTLOOP_GROUP_NAME = "bigtable-grpc-elg";
  /** Number of milliseconds to wait for a termination before trying again. */
  public static final long CHANNEL_TERMINATE_WAIT_MS = 5000;
  private static final Map<MethodDescriptor<?, ?>, Predicate<?>> methodsToRetryMap =
      createMethodRetryMap();
  private static final Logger LOG = new Logger(BigtableSession.class);
//  private static final SslContextBuilder sslBuilder = createGrpcSslBuilder();

  private static ExecutorService connectionStartupExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("BigtableSession-startup-%s")
              .setDaemon(true)
              .build());

  static {
    performWarmup();
  }

  // TODO: This, or at least some variant of this, is recommended by the gRPC team. A client project
  // broke with this option, so we need to debug that issue before enabling this approach.
//  private static SslContextBuilder createGrpcSslBuilder() {
//    SslContextBuilder sslBuilder = GrpcSslContexts.forClient();
//    if (System.getProperty("java.version").startsWith("1.7.")) {
//      // The grpc cyphers only work in JDK 1.8+.  Use the default system cyphers for JDK 1.7.
//      sslBuilder.ciphers(null);
//      LOG.info("Java 7 detected.  Consider Using JDK 1.8+ which has more secure SSL cyphers. "
//          + "If you upgrade, you'll have to change your version of ALPN as per "
//          + "http://www.eclipse.org/jetty/documentation/current/alpn-chapter.html#alpn-versions");
//    }
//    return sslBuilder;
//  }

  // The deprecation is caused by SslContext.newClientContext(). We won't need the @SuppressWarnings
  // once we get GrpcSslContexts to work properly.
  @SuppressWarnings("deprecation")
  private static SslContext createSslContext() throws SSLException {
    // return sslBuilder.build();
    return SslContext.newClientContext();
  }

  private static void performWarmup() {
    // Initialize some core dependencies in parallel.  This can speed up startup by 150+ ms.

    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of BigtableOptions.SSL_CONTEXT_FACTORY.create() is expensive.
        // Create a throw away object in order to speed up the creation of the first
        // BigtableConnection which uses SslContexts under the covers.
        try {
          // We create multiple channels via refreshing and pooling channel implementation.
          // Each one needs its own SslContext.
          @SuppressWarnings("unused")
          SslContext warmup = createSslContext();
        } catch (SSLException e) {
          throw new IllegalStateException("Could not create an ssl context.", e);
        }
      }
    });
    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of BigtableServiceGrpc.CONFIG is expensive.
        // Reference it so that it gets constructed asynchronously.
        @SuppressWarnings("unused")
        BigtableServiceServiceDescriptor warmup = BigtableServiceGrpc.CONFIG;
      }
    });
    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of BigtableTableServiceGrpcs.CONFIG is expensive.
        // Reference it so that it gets constructed asynchronously.
        @SuppressWarnings("unused")
        BigtableTableServiceServiceDescriptor warmup = BigtableTableServiceGrpc.CONFIG;
      }
    });
    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of CredentialFactory.getHttpTransport() is expensive.
        // Reference it so that it gets constructed asynchronously.
        try {
          @SuppressWarnings("unused")
          HttpTransport warmup = CredentialFactory.getHttpTransport();
        } catch (IOException | GeneralSecurityException e) {
          new Logger(BigtableSession.class).warn(
            "Could not asynchronously initialze httpTransport", e);
        }
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
            @SuppressWarnings("unused")
            InetAddress warmup = InetAddress.getByName(host);
          } catch (UnknownHostException e) {
            new Logger(BigtableSession.class).warn(
              "Could not asynchronously initialze host: " + host, e);
          }
        }
      });
    }
    connectionStartupExecutor.shutdown();
  }

  protected static ExecutorService createDefaultBatchPool(){
    return Executors.newCachedThreadPool(
      new ThreadFactoryBuilder()
          .setNameFormat(BATCH_POOL_THREAD_NAME + "-%d")
          .setDaemon(true)
          .build());
  }

  protected static EventLoopGroup createDefaultEventLoopGroup() {
    return new NioEventLoopGroup(0,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(GRPC_EVENTLOOP_GROUP_NAME + "-%d")
            .build());
  }

  protected static ScheduledExecutorService createDefaultRetryExecutor() {
    return Executors.newScheduledThreadPool(
        RETRY_THREAD_COUNT,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(RETRY_THREADPOOL_NAME + "-%d")
            .build());
  }

  private BigtableClient dataClient;
  private BigtableTableAdminClient tableAdminClient;
  private BigtableClusterAdminClient clusterAdminClient;

  private final BigtableOptions options;
  private final ExecutorService batchPool;
  private final boolean terminateBatchPool;
  private final EventLoopGroup elg;
  private final ScheduledExecutorService scheduledRetries;
  private final List<Closeable> clientCloseHandlers = Collections
      .synchronizedList(new ArrayList<Closeable>());
  private ClientInterceptor clientAuthInterceptor;

  public BigtableSession(BigtableOptions options) throws IOException {
    this (options, null, null, null);
  }

  public BigtableSession(BigtableOptions options, ExecutorService batchPool) throws IOException {
    this(options, batchPool, null, null);
  }

  public BigtableSession(BigtableOptions options, @Nullable ExecutorService batchPool,
      @Nullable EventLoopGroup elg, @Nullable ScheduledExecutorService scheduledRetries)
      throws IOException {
    if (batchPool == null) {
      this.terminateBatchPool = true;
      this.batchPool = createDefaultBatchPool();
    } else {
      this.terminateBatchPool = false;
      this.batchPool = batchPool;
    }
    this.options = options;
    Future<Credentials> credentialsFuture = this.batchPool.submit(new Callable<Credentials>() {
      public Credentials call() throws IOException {
        try {
          return CredentialFactory.getCredentials(BigtableSession.this.options
              .getCredentialOptions());
        } catch (GeneralSecurityException e) {
          throw new IOException("Could not load auth credentials", e);
        }
      }
    });
    this.elg = (elg == null) ? createDefaultEventLoopGroup() : elg;
    LOG.info("Opening connection for projectId %s, zoneId %s, clusterId %s, " +
        "on data host %s, table admin host %s.",
        options.getProjectId(), options.getZoneId(), options.getClusterId(),
        options.getDataHost(), options.getTableAdminHost());

    this.scheduledRetries =
        (scheduledRetries == null) ? createDefaultRetryExecutor() : scheduledRetries;

    Future<Void> credentialRefreshingFuture = initializeCredentials(credentialsFuture);

    Future<BigtableClient> dataClientFuture = this.batchPool.submit(new Callable<BigtableClient>() {
      @Override
      public BigtableClient call() throws Exception {
        return initializeDataClient();
      }
    });

    Future<BigtableTableAdminClient> tableAdminFuture =
        this.batchPool.submit(new Callable<BigtableTableAdminClient>() {
          @Override
          public BigtableTableAdminClient call() throws Exception {
            return initializeAdminClient();
          }
        });

    if (credentialRefreshingFuture != null) {
      get(credentialRefreshingFuture, "Cloud not initialize credentials");
    }

    this.dataClient = get(dataClientFuture, "Could not initialize the data API client");
    this.tableAdminClient = get(tableAdminFuture, "Could not initialize the table Admin client");
    awaiteTerminated(connectionStartupExecutor);
  }

  private Future<Void> initializeCredentials(Future<Credentials> credentialsFuture)
      throws IOException {
    final Credentials credentials = get(credentialsFuture, "Cloud not initialize credentials");
    Future<Void> credentialRefreshFuture = null;
    if (credentials != null) {
      if (credentials instanceof OAuth2Credentials) {
        final RefreshingOAuth2CredentialsInterceptor oauth2Interceptor =
            new RefreshingOAuth2CredentialsInterceptor(batchPool, (OAuth2Credentials) credentials);
        credentialRefreshFuture = batchPool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            oauth2Interceptor.syncRefresh();
            return null;
          }
        });
        this.clientAuthInterceptor = oauth2Interceptor;
      } else {
        this.clientAuthInterceptor = new ClientAuthInterceptor(credentials, batchPool);
        credentialRefreshFuture = batchPool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            credentials.refresh();
            return null;
          }
        });
      }
    } else {
      this.clientAuthInterceptor = null;
    }
    return credentialRefreshFuture;
  }

  private BigtableClient initializeDataClient() throws IOException {
    Channel channel = createChannel(options.getDataHost(), options.getChannelCount());
    RetryOptions retryOptions = options.getRetryOptions();
    return new BigtableGrpcClient(channel, batchPool, retryOptions);
  }

  private BigtableTableAdminClient initializeAdminClient() throws IOException {
    Channel channel = createChannel(options.getTableAdminHost(), 1);
    return new BigtableTableAdminGrpcClient(channel);
  }

  private static <T> T get(Future<T> future, String errorMessage) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw new IOException(errorMessage, e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(errorMessage, e);
      }
    }
  }

  public BigtableClient getDataClient() throws IOException {
    return dataClient;
  }

  public BigtableTableAdminClient getTableAdminClient() throws IOException {
    return tableAdminClient;
  }

  public synchronized BigtableClusterAdminClient getClusterAdminClient() throws IOException {
    if (this.clusterAdminClient == null) {
      Channel channel = createChannel(options.getClusterAdminHost(), 1);
      this.clusterAdminClient = new BigtableClusterAdminGrpcClient(channel);
    }

    return clusterAdminClient;
  }

  /**
   * <p>
   * Create a new Channel, optionally adding ChannelInterceptors - auth headers, performance
   * interceptors, and user agent.
   * </p>
   */
  protected Channel createChannel(String hostString, int channelCount) throws IOException {
    final InetSocketAddress host = new InetSocketAddress(getHost(hostString), options.getPort());
    final Channel channels[] = new Channel[channelCount];
    for (int i = 0; i < channelCount; i++) {
      ReconnectingChannel reconnectingChannel = createReconnectingChannel(host);
      clientCloseHandlers.add(reconnectingChannel);
      channels[i] = reconnectingChannel;
    }
    return wrapChannel(new ChannelPool(channels));
  }

  private InetAddress getHost(String hostName) throws IOException {
    String overrideIp = options.getOverrideIp();
    if (overrideIp == null) {
      return InetAddress.getByName(hostName);
    } else {
      InetAddress override = InetAddress.getByName(overrideIp);
      return InetAddress.getByAddress(hostName, override.getAddress());
    }
  }

  protected ReconnectingChannel createReconnectingChannel(final InetSocketAddress host)
      throws IOException {
    return new ReconnectingChannel(options.getTimeoutMs(), new ReconnectingChannel.Factory() {
      @Override
      public Channel createChannel() throws IOException {
        return NettyChannelBuilder
            .forAddress(host)
            .sslContext(createSslContext())
            .eventLoopGroup(elg)
            .executor(batchPool)
            .negotiationType(NegotiationType.TLS)
            .streamWindowSize(1 << 20) // 1 MB -- TODO(sduskis): make this configurable
            .build();
      }

      @Override
      public Closeable createClosable(final Channel channel) {
        return new Closeable() {
          @Override
          public void close() throws IOException {
            ChannelImpl channelImpl = (ChannelImpl) channel;
            channelImpl.shutdown();
            while (!channelImpl.isTerminated()) {
              try {
                channelImpl.awaitTerminated(CHANNEL_TERMINATE_WAIT_MS, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.interrupted();
                throw new IOException("Interrupted while sleeping for close", e);
              }
            }
          }
        };
      }
    });
  }

  @Override
  public void close() {
    elg.shutdownGracefully();
    scheduledRetries.shutdown();
    for (final Closeable clientCloseHandler : clientCloseHandlers) {
      batchPool.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          clientCloseHandler.close();
          return null;
        }
      });
    }
    awaiteTerminated(scheduledRetries);
    if (terminateBatchPool) {
      batchPool.shutdown();
      awaiteTerminated(batchPool);
    }
    // Don't wait for elg to shut down.
  }

  private static void awaiteTerminated(ExecutorService executorService) {
    while (!executorService.isTerminated()) {
      MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS);
    }
  }

  private Channel wrapChannel(Channel channel) {
    List<ClientInterceptor> interceptors = new ArrayList<>();
    if (clientAuthInterceptor != null) {
      interceptors.add(clientAuthInterceptor);
    }

    CallCompletionStatusInterceptor preRetryCallStatusInterceptor = null;
    if (!Strings.isNullOrEmpty(options.getCallStatusReportPath())) {
      preRetryCallStatusInterceptor = new CallCompletionStatusInterceptor();
      interceptors.add(preRetryCallStatusInterceptor);
    }

    interceptors.add(new UserAgentUpdaterInterceptor(options.getUserAgent()));

    if (!interceptors.isEmpty()) {
      channel = ClientInterceptors.intercept(channel, interceptors);
      interceptors.clear();
    }

    if (options.getRetryOptions().enableRetries()) {
      RetryOptions unaryCallRetryOptions = options.getRetryOptions();
      channel = new UnaryCallRetryInterceptor(
          channel,
          scheduledRetries,
          methodsToRetryMap,
          unaryCallRetryOptions.getInitialBackoffMillis(),
          unaryCallRetryOptions.getBackoffMultiplier(),
          unaryCallRetryOptions.getMaxElaspedBackoffMillis());
    }

    if (!Strings.isNullOrEmpty(options.getCallStatusReportPath())) {
      CallCompletionStatusInterceptor postRetryCallStatusInterceptor =
          new CallCompletionStatusInterceptor();

      registerCallStatusReportingShutdownHook(
          options.getCallStatusReportPath(),
          preRetryCallStatusInterceptor,
          postRetryCallStatusInterceptor);

      channel = ClientInterceptors.intercept(channel, postRetryCallStatusInterceptor);
    }

    return channel;
  }

  /**
   * Create a Map of MethodDescriptor instances to predicates that will be used to
   * specify which method calls should be retried and which should not.
   */
  @VisibleForTesting
  protected static Map<MethodDescriptor<?, ?>, Predicate<?>> createMethodRetryMap() {
    Predicate<MutateRowRequest> retryMutationsWithTimestamps = new Predicate<MutateRowRequest>() {
      @Override
      public boolean apply(@Nullable MutateRowRequest mutateRowRequest) {
        if (mutateRowRequest == null) {
          return false;
        }
        for (Mutation mut : mutateRowRequest.getMutationsList()) {
          if (mut.getSetCell().getTimestampMicros() == -1) {
            return false;
          }
        }
        return true;
      }
    };

    Predicate<CheckAndMutateRowRequest> retryCheckAndMutateWithTimestamps =
        new Predicate<CheckAndMutateRowRequest>() {
          @Override
          public boolean apply(@Nullable CheckAndMutateRowRequest checkAndMutateRowRequest) {
            if (checkAndMutateRowRequest == null) {
              return false;
            }
            for (Mutation mut : checkAndMutateRowRequest.getTrueMutationsList()) {
              if (mut.getSetCell().getTimestampMicros() == -1) {
                return false;
              }
            }
            for (Mutation mut : checkAndMutateRowRequest.getFalseMutationsList()) {
              if (mut.getSetCell().getTimestampMicros() == -1) {
                return false;
              }
            }
            return true;
          }
        };

    return ImmutableMap.<MethodDescriptor<?, ?>, Predicate<?>>builder()
        .put(BigtableServiceGrpc.CONFIG.mutateRow, retryMutationsWithTimestamps)
        .put(BigtableServiceGrpc.CONFIG.checkAndMutateRow, retryCheckAndMutateWithTimestamps)
        .build();
  }

  /**
   * Write out CallCompletionStatus entries to a PrintWriter.
   */
  protected static void writeCallStatusesTo(
      PrintWriter writer, String linePrefix, CallCompletionStatusInterceptor interceptor) {
    for (Multiset.Entry<CallCompletionStatusInterceptor.CallCompletionStatus> entry :
        interceptor.getCallCompletionStatuses().entrySet()) {
      writer.println(String.format(
          "%s,%s,%s,%s",
          linePrefix,
          entry.getElement().getMethod().getName(),
          entry.getElement().getCallStatus().getCode(),
          entry.getCount()));
    }
  }

  /**
   * Register a shutdown hook that writes out call reports on JVM shutdown.
   */
  private static void registerCallStatusReportingShutdownHook(
      final String reportPath,
      final CallCompletionStatusInterceptor preRetryCallStatusInterceptor,
      final CallCompletionStatusInterceptor postRetryCallStatusInterceptor) {

    Thread reportingThread = new Thread() {
      @Override
      public void run() {
        try (PrintWriter out =
            new PrintWriter(new BufferedWriter(new FileWriter(reportPath, true)))) {
          writeCallStatusesTo(out, PRE_RETRY_REPORT_ENTRY, preRetryCallStatusInterceptor);
          writeCallStatusesTo(out, POST_RETRY_REPORT_ENTRY, postRetryCallStatusInterceptor);
        } catch (IOException e) {
          System.err.println(String.format("Error writing retry report %s", e));
        }
      }};

    Runtime.getRuntime().addShutdownHook(reportingThread);
  }
}

