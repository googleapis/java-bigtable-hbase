package com.google.cloud.hadoop.hbase;

import io.grpc.Call;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 */
public class ChannelPool implements CloseableChannel {

  protected static final Logger log = Logger.getLogger(ChannelPool.class.getName());

  private final CloseableChannel[] channels;
  private final AtomicInteger requestCount = new AtomicInteger();

  public ChannelPool(CloseableChannel[] channels) {
    this.channels = channels;
  }

  @Override
  public <RequestT, ResponseT> Call<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor) {
    int currentRequestNum = requestCount.getAndIncrement();
    int index = Math.abs(currentRequestNum % channels.length);
    return channels[index].newCall(methodDescriptor);
  }

  @Override
  public void close() throws IOException {
    for (CloseableChannel closeableChannel : channels) {
      closeableChannel.close();
    }
  }
}
