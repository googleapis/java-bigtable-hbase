/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.api.core.InternalApi;
import com.google.api.core.InternalExtensionOnly;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Options for retrying requests, including back off configuration. */
@InternalExtensionOnly
public class RetryOptions implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  /** @deprecated This field will be removed in the future */
  @Deprecated public static int DEFAULT_STREAMING_BUFFER_SIZE = 60;

  /**
   * Flag indicating whether or not grpc retries should be enabled. The default is to enable retries
   * on failed idempotent operations.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final boolean DEFAULT_ENABLE_GRPC_RETRIES = true;

  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static final Set<Status.Code> DEFAULT_ENABLE_GRPC_RETRIES_SET =
      ImmutableSet.of(
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED,
          Status.Code.UNAUTHENTICATED);

  /**
   * We can timeout when reading large cells with a low value here. With a 10MB cell limit, 60
   * seconds allows our connection to drop to ~170kbyte/s. A 10 second timeout requires 1Mbyte/s
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(5);

  /**
   * Initial amount of time to wait before retrying failed operations (default value: 10ms).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_INITIAL_BACKOFF_MILLIS = 10;
  /**
   * Multiplier to apply to wait times after failed retries (default value: 2.0).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
  /**
   * Maximum amount of time to retry before failing the operation (default value: 600 seconds).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS = (int) TimeUnit.MINUTES.toMillis(10);
  /**
   * Maximum number of times to retry after a scan timeout (default value: 10 retries).
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public static final int DEFAULT_MAX_SCAN_TIMEOUT_RETRIES = 10;

  public static RetryOptions getDefaultOptions() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A Builder for ChannelOptions objects. */
  public static class Builder {
    private RetryOptions options;

    /** @deprecated Please use RetryOptions.builder() */
    @Deprecated
    public Builder() {
      options = new RetryOptions();
      options.retriesEnabled = DEFAULT_ENABLE_GRPC_RETRIES;
      options.allowRetriesWithoutTimestamp = false;
      options.initialBackoffMillis = DEFAULT_INITIAL_BACKOFF_MILLIS;
      options.maxElapsedBackoffMillis = DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
      options.backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
      options.streamingBufferSize = DEFAULT_STREAMING_BUFFER_SIZE;
      options.readPartialRowTimeoutMillis = DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS;
      options.maxScanTimeoutRetries = DEFAULT_MAX_SCAN_TIMEOUT_RETRIES;
      options.statusToRetryOn = new HashSet<>(DEFAULT_ENABLE_GRPC_RETRIES_SET);
    }

    /** @deprecated Please use RetryOptions.toBuilder() */
    public Builder(RetryOptions options) {
      this.options = options.clone();
    }

    /** Enable or disable retries. */
    public Builder setEnableRetries(boolean enabled) {
      options.retriesEnabled = enabled;
      return this;
    }

    /** Enable or disable retry on deadline exceeded. */
    public Builder setRetryOnDeadlineExceeded(boolean enabled) {
      if (enabled) {
        options.statusToRetryOn.add(Status.Code.DEADLINE_EXCEEDED);
      } else {
        options.statusToRetryOn.remove(Status.Code.DEADLINE_EXCEEDED);
      }
      return this;
    }

    /** The amount of time in milliseconds we will wait for our first error retry. */
    public Builder setInitialBackoffMillis(int initialBackoffMillis) {
      options.initialBackoffMillis = initialBackoffMillis;
      return this;
    }

    /** Multiplier we will apply to backoff times between retries. */
    public Builder setBackoffMultiplier(double multiplier) {
      options.backoffMultiplier = multiplier;
      return this;
    }

    /** Maximum amount of time we will retry an operation that is failing. */
    public Builder setMaxElapsedBackoffMillis(int maxElapsedBackoffMillis) {
      options.maxElapsedBackoffMillis = maxElapsedBackoffMillis;
      return this;
    }

    /** Set the maximum number of messages to buffer when scanning. */
    public Builder setStreamingBufferSize(int streamingBufferSize) {
      options.streamingBufferSize = streamingBufferSize;
      return this;
    }

    /**
     * Set the timeout in milliseconds for reading individual ReadRowsResponse messages from a
     * stream.
     */
    public Builder setReadPartialRowTimeoutMillis(int timeout) {
      options.readPartialRowTimeoutMillis = timeout;
      return this;
    }

    /** Set the maximum number of times to retry after a scan timeout. */
    public Builder setMaxScanTimeoutRetries(int maxScanTimeoutRetries) {
      options.maxScanTimeoutRetries = maxScanTimeoutRetries;
      return this;
    }

    public Builder addStatusToRetryOn(Status.Code code) {
      options.statusToRetryOn.add(code);
      return this;
    }

    /**
     * Perform retries even if a Put (or other Mutations) don't have a time stamp set and set the
     * timestamp to the server time.
     */
    public Builder setAllowRetriesWithoutTimestamp(boolean allowRetriesWithoutTimestamp) {
      options.allowRetriesWithoutTimestamp = allowRetriesWithoutTimestamp;
      return this;
    }

    /** Construct a new RetryOptions object. */
    public RetryOptions build() {
      return options;
    }
  }

  private boolean retriesEnabled;
  private boolean allowRetriesWithoutTimestamp;
  private int initialBackoffMillis;
  private int maxElapsedBackoffMillis;
  private double backoffMultiplier;
  @Deprecated private int streamingBufferSize;
  private int readPartialRowTimeoutMillis;
  private int maxScanTimeoutRetries;
  private Set<Status.Code> statusToRetryOn;
  /**
   * Constructor for RetryOptions.
   *
   * @param retriesEnabled a boolean.
   * @param allowRetriesWithoutTimestamp a boolean.
   * @param initialBackoffMillis a int.
   * @param backoffMultiplier a double.
   * @param maxElapsedBackoffMillis a int.
   * @param streamingBufferSize a int.
   * @param readPartialRowTimeoutMillis a int.
   * @param maxScanTimeoutRetries a int.
   * @param statusToRetryOn a Set.
   * @deprecated Please use RetryOptions.builder()
   */
  @Deprecated
  public RetryOptions(
      boolean retriesEnabled,
      boolean allowRetriesWithoutTimestamp,
      int initialBackoffMillis,
      double backoffMultiplier,
      int maxElapsedBackoffMillis,
      int streamingBufferSize,
      int readPartialRowTimeoutMillis,
      int maxScanTimeoutRetries,
      Set<Status.Code> statusToRetryOn) {
    this.retriesEnabled = retriesEnabled;
    this.allowRetriesWithoutTimestamp = allowRetriesWithoutTimestamp;
    this.initialBackoffMillis = initialBackoffMillis;
    this.maxElapsedBackoffMillis = maxElapsedBackoffMillis;
    this.backoffMultiplier = backoffMultiplier;
    this.streamingBufferSize = streamingBufferSize;
    this.readPartialRowTimeoutMillis = readPartialRowTimeoutMillis;
    this.maxScanTimeoutRetries = maxScanTimeoutRetries;
    this.statusToRetryOn = ImmutableSet.copyOf(statusToRetryOn);
  }

  private RetryOptions() {}
  /**
   * The amount of time in milliseconds we will wait for our first error retry.
   *
   * @return a int.
   */
  public int getInitialBackoffMillis() {
    return initialBackoffMillis;
  }

  /**
   * Maximum amount of time we will retry an operation that is failing.
   *
   * @return a int.
   */
  public int getMaxElapsedBackoffMillis() {
    return maxElapsedBackoffMillis;
  }

  /**
   * Multiplier we will apply to backoff times between retries.
   *
   * @return a double.
   */
  public double getBackoffMultiplier() {
    return backoffMultiplier;
  }

  /**
   * Enable or disable retries.
   *
   * @return a boolean.
   */
  public boolean enableRetries() {
    return retriesEnabled;
  }

  /**
   * Should retries be allowed even if a timestamp isn't set?
   *
   * @return a boolean.
   */
  public boolean allowRetriesWithoutTimestamp() {
    return allowRetriesWithoutTimestamp;
  }

  /**
   * Whether to retry on deadline exceeded.
   *
   * @return a boolean.
   */
  public boolean retryOnDeadlineExceeded() {
    return statusToRetryOn.contains(Status.Code.DEADLINE_EXCEEDED);
  }

  /** @deprecated This getter will be removed in the future */
  @Deprecated
  public int getStreamingBufferSize() {
    return streamingBufferSize;
  }

  /**
   * A timeout for reading individual ReadRowsResponse messages from a stream.
   *
   * @return a int.
   */
  public int getReadPartialRowTimeoutMillis() {
    return readPartialRowTimeoutMillis;
  }

  /**
   * The maximum number of times to retry after a scan timeout.
   *
   * @return a int.
   */
  public int getMaxScanTimeoutRetries() {
    return maxScanTimeoutRetries;
  }

  /**
   * Determines if the RPC should be retried based on the input {@link io.grpc.Status.Code}.
   *
   * @param code a {@link io.grpc.Status.Code} object.
   * @return a boolean.
   */
  public boolean isRetryable(Status.Code code) {
    return statusToRetryOn.contains(code);
  }

  /**
   * Returns {@link Set} of {@link Status.Code} that allow RPC retries.
   *
   * @return a {@link Set}.
   */
  public Set<Status.Code> getRetryableStatusCodes() {
    return ImmutableSet.copyOf(statusToRetryOn);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != RetryOptions.class) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    RetryOptions other = (RetryOptions) obj;

    return retriesEnabled == other.retriesEnabled
        && allowRetriesWithoutTimestamp == other.allowRetriesWithoutTimestamp
        && Objects.equals(statusToRetryOn, other.statusToRetryOn)
        && initialBackoffMillis == other.initialBackoffMillis
        && maxElapsedBackoffMillis == other.maxElapsedBackoffMillis
        && backoffMultiplier == other.backoffMultiplier
        && streamingBufferSize == other.streamingBufferSize
        && readPartialRowTimeoutMillis == other.readPartialRowTimeoutMillis
        && maxScanTimeoutRetries == other.maxScanTimeoutRetries;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("retriesEnabled", retriesEnabled)
        .add("allowRetriesWithoutTimestamp", allowRetriesWithoutTimestamp)
        .add("statusToRetryOn", statusToRetryOn)
        .add("initialBackoffMillis", initialBackoffMillis)
        .add("maxElapsedBackoffMillis", maxElapsedBackoffMillis)
        .add("backoffMultiplier", backoffMultiplier)
        .add("streamingBufferSize", streamingBufferSize)
        .add("readPartialRowTimeoutMillis", readPartialRowTimeoutMillis)
        .add("maxScanTimeoutRetries", maxScanTimeoutRetries)
        .toString();
  }

  /**
   * toBuilder.
   *
   * @return a {@link com.google.cloud.bigtable.config.RetryOptions.Builder} object.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  protected RetryOptions clone() {
    try {
      return (RetryOptions) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Could not clone RetryOptions.");
    }
  }
}
