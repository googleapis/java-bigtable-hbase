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
package com.google.cloud.bigtable.config;

import java.util.concurrent.TimeUnit;

import com.google.cloud.bigtable.grpc.BigtableGrpcClient;

/**
 * Options for retrying requests, including back off configuration.
 */
public class RetryOptions {

  public static int DEFAULT_STREAMING_BUFFER_SIZE = BigtableGrpcClient.SCANNER_BUFFER_SIZE;
  // We can timeout when reading large cells with a low value here. With a 10MB
  // cell limit, 60 seconds allows our connection to drop to ~170kbyte/s. A 10 second
  // timeout requires 1Mbyte/s
  public static int DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS =
      (int) TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

  /**
   * Initial amount of time to wait before retrying failed operations (default value: 5ms).
   */
  public static final int DEFAULT_INITIAL_BACKOFF_MILLIS = 5;
  /**
   * Multiplier to apply to wait times after failed retries (default value: 2).
   */
  public static final double DEFAULT_BACKOFF_MULTIPLIER = 2;
  /**
   * Maximum amount of time to retry before failing the operation (default value: 60 seconds).
   */
  public static final int DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS = 60 * 1000;


  /**
   * A Builder for ChannelOptions objects.
   */
  public static class Builder {
    private boolean enableRetries = false;
    private boolean retryOnDeadlineExceeded = true;
    private int initialBackoffMillis = DEFAULT_INITIAL_BACKOFF_MILLIS;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
    private int maxElaspedBackoffMillis = DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
    private int streamingBufferSize = DEFAULT_STREAMING_BUFFER_SIZE;
    private int readPartialRowTimeoutMillis = DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS;

    /**
     * Enable or disable retries.
     */
    public Builder setEnableRetries(boolean enabled) {
      this.enableRetries = enabled;
      return this;
    }

    /**
     * Enable or disable retry on deadline exceeded.
     */
    public Builder setRetryOnDeadlineExceeded(boolean enabled) {
      this.retryOnDeadlineExceeded = enabled;
      return this;
    }

    /**
     * The amount of time in miliiseconds we will wait for our first error retry.
     */
    public Builder setInitialBackoffMillis(int initialBackoffMillis) {
      this.initialBackoffMillis = initialBackoffMillis;
      return this;
    }

    /**
     * Multiplier we will apply to backoff times between retries.
     */
    public Builder setBackoffMultiplier(double multiplier) {
      this.backoffMultiplier = multiplier;
      return this;
    }

    /**
     * Maximum amount of time we will retry an operation that is failing.
     */
    public Builder setMaxElapsedBackoffMillis(int maxElaspedBackoffMillis) {
      this.maxElaspedBackoffMillis = maxElaspedBackoffMillis;
      return this;
    }

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
     * Construct a new RetryOptions object.
     */
    public RetryOptions build() {
      return new RetryOptions(
          enableRetries,
          retryOnDeadlineExceeded,
          initialBackoffMillis,
          backoffMultiplier,
          maxElaspedBackoffMillis,
          streamingBufferSize,
          readPartialRowTimeoutMillis);
    }
  }

  private final boolean retriesEnabled;
  // Retry when the response status is deadline exceeded and {@code retriesEnabled} is set to
  // {@code true}.
  private final boolean retryOnDeadlineExceeded;
  private final int initialBackoffMillis;
  private final int maxElaspedBackoffMillis;
  private final double backoffMultiplier;
  private final int streamingBufferSize;
  private final int readPartialRowTimeoutMillis;

  private RetryOptions(
      boolean retriesEnabled,
      boolean retryOnDeadlineExceeded,
      int initialBackoffMillis,
      double backoffMultiplier,
      int maxElaspedBackoffMillis,
      int streamingBufferSize,
      int readPartialRowTimeoutMillis) {
    this.retriesEnabled = retriesEnabled;
    this.retryOnDeadlineExceeded = retryOnDeadlineExceeded;
    this.initialBackoffMillis = initialBackoffMillis;
    this.maxElaspedBackoffMillis = maxElaspedBackoffMillis;
    this.backoffMultiplier = backoffMultiplier;
    this.streamingBufferSize = streamingBufferSize;
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
  }

  /**
   * The amount of time in miliiseconds we will wait for our first error retry.
   */
  public int getInitialBackoffMillis() {
    return initialBackoffMillis;
  }

  /**
   * Maximum amount of time we will retry an operation that is failing.
   */
  public int getMaxElaspedBackoffMillis() {
    return maxElaspedBackoffMillis;
  }

  /**
   * Multiplier we will apply to backoff times between retries.
   */
  public double getBackoffMultiplier() {
    return backoffMultiplier;
  }

  /**
   * Enable or disable retries.
   */
  public boolean enableRetries() {
    return retriesEnabled;
  }

  /**
   * Whether to retry on deadline exceeded.
   */
  public boolean retryOnDeadlineExceeded() {
    return retryOnDeadlineExceeded;
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

}
