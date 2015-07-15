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

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Methods for constructing, configuring and wrapping Channel objects
 */
public class BigtableChannels {

  /** Entry in call reports that indicates an entry is from before retries */
  private static final String PRE_RETRY_REPORT_ENTRY = "PreRetry";
  /** Entry in call reports that indicates an entry is from after retries */
  private static final String POST_RETRY_REPORT_ENTRY = "PostRetry";
  /** Number of threads to use to initiate retry calls */
  private static final int RETRY_THREAD_COUNT = 5;

  /** Number of seconds to wait for a termination before trying again. */
  public static final int CHANNEL_TERMINATE_WAIT_MS = 5;

  private static final Map<MethodDescriptor<?, ?>, Predicate<?>> METHODS_TO_RETRY_MAP =
      createMethodRetryMap();

  /**
   * Create a new Channel, optionally adding OAuth2 support.
   */
  public static Channel createChannel(TransportOptions transportOptions,
       ChannelOptions channelOptions, ExecutorService executor) {
    return createChannel(transportOptions, channelOptions, executor, false);
  }

  /**
   * <p> Create a new Channel, optionally adding OAuth2 support.</p>
   *
   * <p> To create a pool, createPool has to be true and channel count has to be greater than one.
   * </p>
   */
  public static Channel createChannel(final TransportOptions transportOptions,
      final ChannelOptions channelOptions, final ExecutorService executor, boolean createPool) {
    Preconditions.checkArgument(
        transportOptions.getTransport() == TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
        "Bigtable requires the NETTY_TLS transport.");

    int channelCount = createPool ? channelOptions.getChannelCount() : 1;
    long timeoutMs = channelOptions.getTimeoutMs();

    Channel channels[] = new Channel[channelCount];
    for (int i = 0; i < channelCount; i++) {
      ReconnectingChannel reconnectingChannel =
          createReconnectingChannel(transportOptions, executor, timeoutMs);
      channelOptions.addClientCloseHandler(reconnectingChannel);
      channels[i] = reconnectingChannel;
    }
    Channel channel = new ChannelPool(channels);
    return wrapChannel(channelOptions, executor, channel);
  }

  private static ReconnectingChannel createReconnectingChannel(
      final TransportOptions transportOptions, final ExecutorService executor, long timeoutMs) {
    return new ReconnectingChannel(timeoutMs, new ReconnectingChannel.Factory() {
      @Override
      public Channel createChannel() {
        return NettyChannelBuilder
            .forAddress(
                new InetSocketAddress(transportOptions.getHost(), transportOptions.getPort()))
            .sslContext(transportOptions.createSslContext())
            .eventLoopGroup(transportOptions.getEventLoopGroup())
            .executor(executor)
            .negotiationType(NegotiationType.TLS)
            .streamWindowSize(1 << 20) // 1 MB
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

  private static Channel wrapChannel(ChannelOptions channelOptions,
      ExecutorService executor, Channel channel) {
    List<ClientInterceptor> interceptors = new ArrayList<>();
    if (channelOptions.getCredential() != null) {
      interceptors.add(new ClientAuthInterceptor(channelOptions.getCredential(), executor));
    }

    if (channelOptions.getAuthority() != null) {
      Metadata.Headers headers = new Metadata.Headers();
      headers.setAuthority(channelOptions.getAuthority());
      interceptors.add(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    CallCompletionStatusInterceptor preRetryCallStatusInterceptor = null;
    if (!Strings.isNullOrEmpty(channelOptions.getCallStatusReportPath())) {
      preRetryCallStatusInterceptor = new CallCompletionStatusInterceptor();
      interceptors.add(preRetryCallStatusInterceptor);
    }

    interceptors.add(new UserAgentUpdaterInterceptor(channelOptions.getUserAgent()));

    if (!interceptors.isEmpty()) {
      channel = ClientInterceptors.intercept(channel, interceptors);
      interceptors.clear();
    }

    if (channelOptions.getUnaryCallRetryOptions().enableRetries()) {
      ScheduledExecutorService scheduledRetries;
      if (channelOptions.getScheduledExecutorService() != null) {
        scheduledRetries = channelOptions.getScheduledExecutorService();
      } else {
        scheduledRetries = createScheduledRetryPool();
        channelOptions.addClientCloseHandler(createExecutorCloseHandler(scheduledRetries));
      }

      RetryOptions unaryCallRetryOptions = channelOptions.getUnaryCallRetryOptions();
      channel = new UnaryCallRetryInterceptor(
          channel,
          scheduledRetries,
          METHODS_TO_RETRY_MAP,
          unaryCallRetryOptions.getInitialBackoffMillis(),
          unaryCallRetryOptions.getBackoffMultiplier(),
          unaryCallRetryOptions.getMaxElaspedBackoffMillis());
    }

    if (!Strings.isNullOrEmpty(channelOptions.getCallStatusReportPath())) {
      CallCompletionStatusInterceptor postRetryCallStatusInterceptor =
          new CallCompletionStatusInterceptor();

      registerCallStatusReportingShutdownHook(
          channelOptions.getCallStatusReportPath(),
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
  static Map<MethodDescriptor<?, ?>, Predicate<?>> createMethodRetryMap() {
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
   * Create a new {@link ScheduledExecutorService} for executing RPC retrie operations.
   */
  private static ScheduledExecutorService createScheduledRetryPool() {
    return Executors.newScheduledThreadPool(
        RETRY_THREAD_COUNT,
        new ThreadFactoryBuilder()
          .setNameFormat("grpc-call-retry-pool-%s")
          .build());
  }

  /**
   * Create a ClientCloseHandler that attempts to shutdown the given ExecutorService via
   * MoreExecutors.shutdownAndAwaitTermination.
   */
  private static Closeable createExecutorCloseHandler(
      final ExecutorService executorService) {
    return new Closeable() {
      @Override
      public void close() throws IOException {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 30, TimeUnit.SECONDS);
      }
    };
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
