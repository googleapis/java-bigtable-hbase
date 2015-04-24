package com.google.cloud.bigtable.grpc;

import io.grpc.Channel;

import java.io.IOException;

/**
 * A {@link Channel} that allows clients to close it, freeing any resources.
 */
public interface CloseableChannel extends Channel {

  /**
   * Release resources used by this channel.
   * @throws IOException
   */
  void close() throws IOException;
}
