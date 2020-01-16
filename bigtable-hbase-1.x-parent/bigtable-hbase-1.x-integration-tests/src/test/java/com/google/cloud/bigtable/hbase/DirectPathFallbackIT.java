package com.google.cloud.bigtable.hbase;

import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowSet;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions.ChannelConfigurator;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.repackaged.io.grpc.ManagedChannelBuilder;
import com.google.bigtable.repackaged.io.grpc.alts.ComputeEngineChannelBuilder;
import com.google.bigtable.repackaged.io.grpc.netty.NettyChannelBuilder;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.ChannelDuplexHandler;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.ChannelPromise;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
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

  private AtomicBoolean blackholeIPv6 = new AtomicBoolean();
  private AtomicInteger numBlocked = new AtomicInteger();
  private AtomicInteger numIPv6Read = new AtomicInteger();

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
    if (!BigtableOptions.isDirectPathEnabled()) {
      throw new AssumptionViolatedException(
          "DirectPath integration tests can only run against DirectPathEnv");
    }

    BigtableOptions.Builder bigtableOptions =
        BigtableOptionsFactory.fromConfiguration(sharedTestEnv.getConfiguration())
            .toBuilder()
            .setDataChannelCount(1);

    bigtableOptions.setChannelConfigurator(
        new ChannelConfigurator() {
          @Override
          public ManagedChannelBuilder configureChannel(
              ManagedChannelBuilder builder, String host) {
            // TODO: remove this when admin api supports DirectPath
            if (host.contains("admin")) {
              return builder;
            }
            injectNettyChannelHandler(builder);
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
  public void testFallback() throws InterruptedException {
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
    // Verify that we can send a request
    instrumentedSession.getDataClient().readFlatRowsList(request);

    // Enable the blackhole, which will prevent communication via IPv6 and thus DirectPath.
    blackholeIPv6.set(true);

    // Send a request, which should be routed over IPv4 and CFE.
    instrumentedSession.getDataClient().readFlatRowsList(request);

    // Verify that the above check was meaningful, by verifying that the blackhole actually dropped
    // packets.
    Assert.assertTrue("Failed to detect any IPv6 traffic in blackhole", numBlocked.get() > 0);

    // Make sure that the client will start reading from IPv6 again by sending new requests and
    // checking the injected IPv6 counter has been updated.
    blackholeIPv6.set(false);
    numIPv6Read.set(0);

    while (numIPv6Read.get() < MIN_COMPLETE_READ_CALLS) {
      for (int i = 0; i < NUM_RPCS_TO_SEND; i++) {
        instrumentedSession.getDataClient().readFlatRowsList(request);
      }

      Thread.sleep(100);
    }

    Assert.assertTrue(
        "Failed to upgrade back to DirectPath", numIPv6Read.get() > MIN_COMPLETE_READ_CALLS);
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
   * A netty {@link
   * com.google.bigtable.repackaged.io.grpc.netty.shaded.io.netty.channel.ChannelHandler} that can
   * be instructed to make IPv6 packets disappear
   */
  private class MyChannelHandler extends ChannelDuplexHandler {
    private boolean isIPv6;

    @Override
    public void connect(
        ChannelHandlerContext ctx,
        SocketAddress remoteAddress,
        SocketAddress localAddress,
        ChannelPromise promise)
        throws Exception {

      this.isIPv6 =
          (remoteAddress instanceof InetSocketAddress)
              && ((InetSocketAddress) remoteAddress).getAddress() instanceof Inet6Address;

      if (!(isIPv6 && blackholeIPv6.get())) {
        super.connect(ctx, remoteAddress, localAddress, promise);
      } else {
        // Fail the connection fast
        promise.setFailure(new IOException("fake error"));
      }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      boolean dropCall = isIPv6 && blackholeIPv6.get();

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
      boolean dropCall = isIPv6 && blackholeIPv6.get();

      if (dropCall) {
        // Don't notify the next handler and increment counter
        numBlocked.incrementAndGet();
      } else {
        if (isIPv6) {
          numIPv6Read.incrementAndGet();
        }
        super.channelReadComplete(ctx);
      }
    }
  }
}
