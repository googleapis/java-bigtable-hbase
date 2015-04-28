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
