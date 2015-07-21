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

package com.google.cloud.bigtable.grpc;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
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
import com.google.common.base.Preconditions;
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
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.stub.MetadataUtils;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

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

  /** Entry in call reports that indicates an entry is from before retries */
  private static final String PRE_RETRY_REPORT_ENTRY = "PreRetry";
  /** Entry in call reports that indicates an entry is from after retries */
  private static final String POST_RETRY_REPORT_ENTRY = "PostRetry";
  /** Number of threads to use to initiate retry calls */
  public static final String RETRY_THREADPOOL_NAME = "bigtable-rpc-retry";
  public static final int RETRY_THREAD_COUNT = 4;

  private final Map<MethodDescriptor<?, ?>, Predicate<?>> methodsToRetryMap =
      createMethodRetryMap();

  static {
    // Initialize some core dependencies in parallel.  This can speed up startup by 150+ ms.
    ExecutorService connectionStartupExecutor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("BigtableSession-startup-%s")
                .setDaemon(true)
                .build());

    connectionStartupExecutor.execute(new Runnable() {
      @Override
      public void run() {
        // The first invocation of BigtableOptions.SSL_CONTEXT_FACTORY.create() is expensive.
        // Create a throw away object in order to speed up the creation of the first
        // BigtableConnection which uses SslContexts under the covers.
        @SuppressWarnings("unused")
        SslContext warmup = TransportOptions.SSL_CONTEXT_FACTORY.create();
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
    for (final String host : Arrays.asList(BigtableOptions.BIGTABLE_HOST_DEFAULT,
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

  private final static Logger LOG = new Logger(BigtableSession.class);

  public static final String GRPC_EVENTLOOP_GROUP_NAME = "bigtable-grpc-elg";

  /** Number of milliseconds to wait for a termination before trying again. */
  public static final long CHANNEL_TERMINATE_WAIT_MS = 5000;

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

  private BigtableClient client;
  private BigtableTableAdminClient tableAdminClient;
  private BigtableClusterAdminClient clusterAdminClient;

  private final BigtableOptions options;
  private final ExecutorService batchPool;
  private final EventLoopGroup elg;
  private final ScheduledExecutorService scheduledRetries;
  private final List<Closeable> clientCloseHandlers = new ArrayList<>();

  public BigtableSession(BigtableOptions options, ExecutorService batchPool) {
    this(options, batchPool, createDefaultEventLoopGroup(), createDefaultRetryExecutor());
  }

  public BigtableSession(BigtableOptions options, ExecutorService batchPool, EventLoopGroup elg,
      ScheduledExecutorService scheduledRetries) {
    this.elg = elg;
    this.batchPool = batchPool;
    this.options = options;
    LOG.info("Opening connection for project %s on data host %s, "
        + "table admin host %s, using transport %s.", options.getProjectId(),
      options.getDataHost(), options.getTableAdminHost(),
      TransportOptions.BigtableTransports.HTTP2_NETTY_TLS);
    this.scheduledRetries = scheduledRetries;
  }

  protected TransportOptions createTransportOptions(InetAddress host) {
    return new TransportOptions(
        TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
        host,
        options.getPort(),
        TransportOptions.SSL_CONTEXT_FACTORY,
        elg);
  }

  public synchronized BigtableClient getDataClient() {
    if (this.client == null) {
      Channel channel = createChannel(
        createTransportOptions(options.getDataHost()),
        options.getChannelCount());
      this.client = new BigtableGrpcClient(channel, batchPool, options.getRetryOptions());
    }

    return client;
  }

  public synchronized BigtableTableAdminClient getTableAdminClient() {
    if (this.tableAdminClient == null) {
      Channel channel = createChannel(
        createTransportOptions(options.getTableAdminHost()),
        1);
      this.tableAdminClient = new BigtableTableAdminGrpcClient(channel);
    }
    return tableAdminClient;
  }

  public synchronized BigtableClusterAdminClient getClusterAdminClient() {
    if (this.clusterAdminClient == null) {
      Channel channel = createChannel(
        createTransportOptions(options.getClusterAdminHost()),
        1);
      this.clusterAdminClient = new BigtableClusterAdminGrpcClient(channel);
    }

    return clusterAdminClient;
  }

  /**
   * <p>
   * Create a new Channel, optionally adding OAuth2 support.
   * </p>
   * <p>
   * To create a pool, createPool has to be true and channel count has to be greater than one.
   * </p>
   */
  protected Channel createChannel(final TransportOptions transportOptions, int channelCount) {

    Preconditions.checkArgument(
      transportOptions.getTransport() == TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
      "Bigtable requires the NETTY_TLS transport.");
    long timeoutMs = options.getTimeoutMs();

    Channel channels[] = new Channel[channelCount];
    for (int i = 0; i < channelCount; i++) {
      ReconnectingChannel reconnectingChannel =
          createReconnectingChannel(transportOptions, batchPool, timeoutMs);
      clientCloseHandlers.add(reconnectingChannel);
      channels[i] = reconnectingChannel;
    }
    Channel channel = new ChannelPool(channels);
    return wrapChannel(channel);
  }

  protected ReconnectingChannel createReconnectingChannel(
      final TransportOptions transportOptions, final ExecutorService executor, long timeoutMs) {
    return new ReconnectingChannel(timeoutMs, new ReconnectingChannel.Factory() {
      @Override
      public Channel createChannel() {
        return NettyChannelBuilder
            .forAddress(
              new InetSocketAddress(transportOptions.getHost(), transportOptions.getPort()))
            .sslContext(transportOptions.createSslContext())
            .eventLoopGroup(transportOptions.getEventLoopGroup()).executor(executor)
            .negotiationType(NegotiationType.TLS).streamWindowSize(1 << 20) // 1 MB
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
  public void close() throws Exception {
    elg.shutdownGracefully();
    batchPool.shutdown();
    scheduledRetries.shutdown();
    for (Closeable clientCloseHandler : clientCloseHandlers) {
      try {
        clientCloseHandler.close();
      } catch (IOException e) {
        throw new RuntimeException("Error when shutting down clients", e);
      }
    }
    awaiteTerminated(batchPool);
    awaiteTerminated(elg);
    awaiteTerminated(scheduledRetries);
  }

  private static void awaiteTerminated(ExecutorService executorService) {
    while (!executorService.isTerminated()) {
      MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS);
    }
  }

  private Channel wrapChannel(Channel channel) {
    List<ClientInterceptor> interceptors = new ArrayList<>();
    if (options.getCredential() != null) {
      interceptors.add(new ClientAuthInterceptor(options.getCredential(), batchPool));
    }

    if (options.getAuthority() != null) {
      Metadata.Headers headers = new Metadata.Headers();
      headers.setAuthority(options.getAuthority());
      interceptors.add(MetadataUtils.newAttachHeadersInterceptor(headers));
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
  protected Map<MethodDescriptor<?, ?>, Predicate<?>> createMethodRetryMap() {
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

