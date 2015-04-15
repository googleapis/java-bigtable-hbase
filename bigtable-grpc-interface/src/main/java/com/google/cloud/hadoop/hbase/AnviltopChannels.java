package com.google.cloud.hadoop.hbase;

import java.util.concurrent.ExecutorService;

/**
 * Methods for constructing, configuring and wrapping Channel objects
 *
 * @deprecated Use {@link BigtableChannels#createChannel(TransportOptions, ChannelOptions,
 *             ExecutorService)} instead.
 *
 * TODO(sduskis): Remove this once github is updated.
 */

@Deprecated
public class AnviltopChannels {
  public static CloseableChannel createChannel(TransportOptions transportOptions,
      ChannelOptions channelOptions, ExecutorService executor) {
    return BigtableChannels.createChannel(transportOptions, channelOptions, executor);
  }
}
