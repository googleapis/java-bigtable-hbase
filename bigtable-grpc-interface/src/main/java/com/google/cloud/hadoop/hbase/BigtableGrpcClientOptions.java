package com.google.cloud.hadoop.hbase;

import java.util.concurrent.TimeUnit;

/**
 * Options controlling behavior of the BigtableGrpcClient.
 */
public class BigtableGrpcClientOptions {

  /**
   * Builder for BigtableGrpcClientOptions.
   */
  public static class Builder {
    private int streamingBufferSize = BigtableGrpcClient.SCANNER_BUFFER_SIZE;
    // We can timeout when reading large cells with a low value here. With a 10MB
    // cell limit, 60 seconds allows our connection to drop to ~170kbyte/s. A 10 second
    // timeout requires 1Mbyte/s
    private int readPartialRowTimeoutMillis =
        (int) TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);
    private RetryOptions.Builder streamingRetryOptionsBuilder =
        new RetryOptions.Builder();

    /**
     * Set the maximum number of messages to buffer when scanning.
     */
    public Builder setStreamingBufferSize(int streamingBufferSize) {
      this.streamingBufferSize = streamingBufferSize;
      return this;
    }

    public Builder setReadPartialRowTimeoutMillis(int timeout) {
      this.readPartialRowTimeoutMillis = timeout;
      return this;
    }

    /**
     * A builder for streaming readRows RetryOptions.
     */
    public RetryOptions.Builder getStreamingRetryOptionsBuilder() {
      return streamingRetryOptionsBuilder;
    }

    /**
     * Construct a new BigtableGrpcClientOptions object.
     */
    public BigtableGrpcClientOptions build() {
      return new BigtableGrpcClientOptions(
          streamingBufferSize,
          readPartialRowTimeoutMillis,
          streamingRetryOptionsBuilder.build());
    }
  }

  private final int streamingBufferSize;
  private final int readPartialRowTimeoutMillis;
  private final RetryOptions streamingRetryOptions;

  public BigtableGrpcClientOptions(
      int streamingBufferSize,
      int readPartialRowTimeoutMillis,
      RetryOptions streamingRetryOptions) {
    this.streamingBufferSize = streamingBufferSize;
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    this.streamingRetryOptions = streamingRetryOptions;
  }

  /**
   * The maximum number of messages to buffer when scanning.
   */
  public int getStreamingBufferSize() {
    return streamingBufferSize;
  }

  /**
   * A timeout for reading individual ReadRowsResponse messages from a stream.
   */
  public int getReadPartialRowTimeoutMillis() {
    return readPartialRowTimeoutMillis;
  }

  /**
   * Streaming readRows RetryOptions.
   */
  public RetryOptions getStreamingRetryOptions() {
    return streamingRetryOptions;
  }
}
