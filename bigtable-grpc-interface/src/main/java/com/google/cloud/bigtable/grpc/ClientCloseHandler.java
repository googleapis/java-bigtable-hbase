package com.google.cloud.bigtable.grpc;

import java.io.IOException;

/**
 * Callback used for adding extra actions that are required to be performed to shutdown
 * the client cleanly.
 */
public interface ClientCloseHandler {

  /**
   * Perform any cleanup actions needed to close the client cleanly.
   * @throws IOException
   */
  void close() throws IOException;
}
