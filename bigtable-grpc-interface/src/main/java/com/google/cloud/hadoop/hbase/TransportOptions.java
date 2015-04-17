package com.google.cloud.hadoop.hbase;


import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Options for constructing the transport to Anviltop.
 */
public class TransportOptions {

  // TODO: I suspect other transports will require other options. Even the
  /**
   * Available transport implementations
   */
  public static enum BigtableTransports {
    HTTP2_NETTY_TLS,
  }

  /**
   * Temporary class that mimics the old signature of available transport implementations.
   * 
   * TODO(sduskis): Remove this once github is updated.
   */
  @Deprecated
  public static class AnviltopTransports {
    public static final BigtableTransports HTTP2_NETTY_TLS = BigtableTransports.HTTP2_NETTY_TLS;
  }

  /**
   * Creates a SslContext.
   */
  public interface SslContextFactory {
    SslContext create();
  }

  private final BigtableTransports transport;
  private final InetAddress endpointAddress;
  private final int port;
  private final SslContextFactory sslContextFactory;
  private final EventLoopGroup eventLoopGroup;

  /**
   * Construct a new TransportOptions object.
   * @param transport The transport implementation to use
   * @param host The host to connect to
   * @param port The port to connect to
   */
  public TransportOptions(BigtableTransports transport, String host, int port)
      throws IOException {
    this(transport, InetAddress.getByName(host), port);
  }

  /**
   * Construct a new TransportOptions object.
   */
  public TransportOptions(BigtableTransports transport, InetAddress endpointAddress, int port) {
    this(transport, endpointAddress, port, new SslContextFactory() {
      @Override
      public SslContext create() {
        return null;
      }
    }, null);
  }

  /**
   * Construct a new TransportOptions object.
   */
  @Deprecated
  public TransportOptions(
      BigtableTransports transport,
      InetAddress endpointAddress,
      int port,
      final SslContext sslContext,
      EventLoopGroup eventLoopGroup) {
    this.transport = transport;
    this.endpointAddress = endpointAddress;
    this.port = port;
    this.sslContextFactory = new SslContextFactory() {
      @Override
      public SslContext create() {
        return sslContext;
      }
    };
    this.eventLoopGroup = eventLoopGroup;
  }

  public TransportOptions(
      BigtableTransports transport,
      InetAddress endpointAddress,
      int port,
      SslContextFactory sslContextFactory,
      EventLoopGroup eventLoopGroup) {
    this.transport = transport;
    this.endpointAddress = endpointAddress;
    this.port = port;
    this.sslContextFactory = sslContextFactory;
    this.eventLoopGroup = eventLoopGroup;
  }
  /**
   * The transport implementation to use
   */
  public BigtableTransports getTransport() {
    return transport;
  }

  /**
   * The host to connect to
   */
  public InetAddress getHost() {
    return endpointAddress;
  }

  /**
   * The port to connect to.
   */
  public int getPort() {
    return port;
  }

  /**
   * A new SslContext to use for TLS connections.  Each Channel needs its own SslContext.
   */
  public SslContext createSslContext() {
    return sslContextFactory.create();
  }

  /**
   * An EventLoopGroup to use for managing connections / RPCs.
   */
  public EventLoopGroup getEventLoopGroup() { return eventLoopGroup; }
}
