/*
 * Copyright 2020 Google LLC
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

import static com.google.cloud.bigtable.config.BigtableOptions.ChannelConfigurator;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.test_env.ConnectionMode;
import com.google.cloud.bigtable.hbase.util.IpVerificationInterceptor;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.alts.ComputeEngineChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelDuplexHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelPromise;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.netty.shaded.io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(KnownHBaseGap.class)
@RunWith(JUnit4.class)
public class DirectPathFallbackIT extends AbstractTest {
  // A threshold of completed read calls to observe to ascertain IPv6 is working.
  // This was determined experimentally to account for both gRPC-LB RPCs and Bigtable api RPCs.
  private static final int MIN_COMPLETE_READ_CALLS = 40;
  private static final int NUM_RPCS_TO_SEND = 20;

  // IP address prefixes allocated for DirectPath backends.
  private static final String DP_IPV6_PREFIX = "2001:4860:8040";
  private static final String DP_IPV4_PREFIX = "34.126";

  private AtomicBoolean blackholeDpAddr = new AtomicBoolean();
  private AtomicInteger numBlocked = new AtomicInteger();
  private AtomicInteger numDpAddrRead = new AtomicInteger();

  private ChannelFactory<NioSocketChannel> channelFactory;
  private EventLoopGroup eventLoopGroup;
  private BigtableSession instrumentedSession;

  public DirectPathFallbackIT() {
    // Create a transport channel provider that can intercept ipv6 packets.
    channelFactory = new MyChannelFactory();
    eventLoopGroup = new NioEventLoopGroup();
  }

  @Before
  public void setup() throws IOException {
    Set<ConnectionMode> validModes =
        ImmutableSet.of(
            ConnectionMode.REQUIRE_DIRECT_PATH, ConnectionMode.REQUIRE_DIRECT_PATH_IPV4);
    if (!validModes.contains(sharedTestEnv.getConnectionMode())) {
      throw new AssumptionViolatedException(
          "DirectPathFallbackIT can only return when explicitly requested");
    }

    BigtableOptions.Builder bigtableOptions =
        BigtableOptionsFactory.fromConfiguration(sharedTestEnv.getConfiguration())
            .toBuilder()
            .setDataChannelCount(1);

    final ChannelConfigurator oldConfigurator = bigtableOptions.getChannelConfigurator();
    bigtableOptions.setChannelConfigurator(
        new ChannelConfigurator() {
          @Override
          public ManagedChannelBuilder configureChannel(
              ManagedChannelBuilder builder, String host) {
            if (oldConfigurator != null) {
              builder = oldConfigurator.configureChannel(builder, host);
            }
            // TODO: remove this when admin api supports DirectPath
            if (host.contains("admin")) {
              return builder;
            }
            injectNettyChannelHandler(builder);
            // Since we are forcing fallback, disable ip verification
            builder.intercept(
                new ClientInterceptor() {
                  @Override
                  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                    return next.newCall(
                        method,
                        callOptions.withOption(
                            IpVerificationInterceptor.SKIP_IP_VERIFICATION, true));
                  }
                });
            // Fail fast when blackhole is active
            builder.keepAliveTime(1, TimeUnit.SECONDS);
            builder.keepAliveTimeout(1, TimeUnit.SECONDS);
            return builder;
          }
        });

    instrumentedSession = new BigtableSession(bigtableOptions.build());
  }

  @After
  public void teardown() throws IOException {
    if (instrumentedSession != null) {
      instrumentedSession.close();
    }
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully();
    }
  }

  @Test
  public void testFallback() throws InterruptedException, TimeoutException {
    // Precondition: wait for DirectPath to connect
    Assert.assertTrue("Failed to observe RPCs over DirectPath", exerciseDirectPath());

    // Enable the blackhole, which will prevent communication with grpclb and thus DirectPath.
    blackholeDpAddr.set(true);

    // Send a request, which should be routed over IPv4 and CFE.
    instrumentedSession
        .getDataClient()
        .readFlatRowsList(
            ReadRowsRequest.newBuilder()
                .setTableName(
                    String.format(
                        "projects/%s/instances/%s/tables/%s",
                        sharedTestEnv.getConfiguration().get(BigtableOptionsFactory.PROJECT_ID_KEY),
                        sharedTestEnv
                            .getConfiguration()
                            .get(BigtableOptionsFactory.INSTANCE_ID_KEY),
                        sharedTestEnv.getDefaultTableName().toString()))
                .setRows(RowSet.newBuilder().addRowKeys(ByteString.copyFromUtf8("nonexistent-row")))
                .setRowsLimit(1)
                .build());

    // Verify that the above check was meaningful, by verifying that the blackhole actually dropped
    // packets.
    Assert.assertTrue("Failed to detect any IPv6 traffic in blackhole", numBlocked.get() > 0);

    // Make sure that the client will start reading from IPv6 again by sending new requests and
    // checking the injected IPv6 counter has been updated.
    blackholeDpAddr.set(false);

    Assert.assertTrue("Failed to upgrade back to DirectPath", exerciseDirectPath());
  }

  private boolean exerciseDirectPath() throws InterruptedException, TimeoutException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    numDpAddrRead.set(0);

    boolean seenEnough = false;

    ReadRowsRequest request =
        ReadRowsRequest.newBuilder()
            .setTableName(
                String.format(
                    "projects/%s/instances/%s/tables/%s",
                    sharedTestEnv.getConfiguration().get(BigtableOptionsFactory.PROJECT_ID_KEY),
                    sharedTestEnv.getConfiguration().get(BigtableOptionsFactory.INSTANCE_ID_KEY),
                    sharedTestEnv.getDefaultTableName().toString()))
            .setRows(RowSet.newBuilder().addRowKeys(ByteString.copyFromUtf8("nonexistent-row")))
            .setRowsLimit(1)
            .build();

    while (!seenEnough && stopwatch.elapsed(TimeUnit.MINUTES) < 2) {
      for (int i = 0; i < NUM_RPCS_TO_SEND; i++) {
        // Verify that we can send a request
        instrumentedSession.getDataClient().readFlatRowsList(request);
      }
      Thread.sleep(100);
      seenEnough = numDpAddrRead.get() >= MIN_COMPLETE_READ_CALLS;
    }
    return seenEnough;
  }

  /**
   * This is a giant hack to enable testing DirectPath CFE fallback.
   *
   * <p>It unwraps the {@link ComputeEngineChannelBuilder} to inject a NettyChannelHandler to signal
   * IPv6 packet loss.
   */
  private void injectNettyChannelHandler(ManagedChannelBuilder<?> channelBuilder) {
    try {
      // Extract the delegate NettyChannelBuilder using reflection
      Field delegateField = ComputeEngineChannelBuilder.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);

      ComputeEngineChannelBuilder gceChannelBuilder =
          ((ComputeEngineChannelBuilder) channelBuilder);
      Object delegateChannelBuilder = delegateField.get(gceChannelBuilder);

      NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) delegateChannelBuilder;
      nettyChannelBuilder.channelFactory(channelFactory);
      nettyChannelBuilder.eventLoopGroup(eventLoopGroup);

      channelBuilder.intercept(
          new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
              return next.newCall(
                  method,
                  callOptions.withOption(IpVerificationInterceptor.SKIP_IP_VERIFICATION, true));
            }
          });
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to inject the netty ChannelHandler", e);
    }
  }

  /** @see MyChannelHandler */
  private class MyChannelFactory implements ChannelFactory<NioSocketChannel> {
    @Override
    public NioSocketChannel newChannel() {
      NioSocketChannel channel = new NioSocketChannel();
      channel.pipeline().addLast(new MyChannelHandler());

      return channel;
    }
  }

  /**
   * A netty {@link io.grpc.netty.shaded.io.netty.channel.ChannelHandler} that can be instructed to
   * make IPv6 packets disappear
   */
  private class MyChannelHandler extends ChannelDuplexHandler {
    private boolean isDpAddr;

    @Override
    public void connect(
        ChannelHandlerContext ctx,
        SocketAddress remoteAddress,
        SocketAddress localAddress,
        ChannelPromise promise)
        throws Exception {

      if (remoteAddress instanceof InetSocketAddress) {
        InetAddress inetAddress = ((InetSocketAddress) remoteAddress).getAddress();
        String addr = inetAddress.getHostAddress();
        isDpAddr = addr.startsWith(DP_IPV6_PREFIX) || addr.startsWith(DP_IPV4_PREFIX);
      }

      if (!(isDpAddr && blackholeDpAddr.get())) {
        super.connect(ctx, remoteAddress, localAddress, promise);
      } else {
        // Fail the connection fast
        promise.setFailure(new IOException("fake error"));
      }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      boolean dropCall = isDpAddr && blackholeDpAddr.get();

      if (dropCall) {
        // Don't notify the next handler and increment counter
        numBlocked.incrementAndGet();
        ReferenceCountUtil.release(msg);
      } else {
        super.channelRead(ctx, msg);
      }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      boolean dropCall = isDpAddr && blackholeDpAddr.get();

      if (dropCall) {
        // Don't notify the next handler and increment counter
        numBlocked.incrementAndGet();
      } else {
        if (isDpAddr) {
          numDpAddrRead.incrementAndGet();
        }
        super.channelReadComplete(ctx);
      }
    }
  }
}
