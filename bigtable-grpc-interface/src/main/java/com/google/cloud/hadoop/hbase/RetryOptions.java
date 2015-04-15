package com.google.cloud.hadoop.hbase;

/**
 * Options for retrying requests, including back off configuration.
 */
public class RetryOptions {

  /**
   * A Builder for ChannelOptions objects.
   */
  public static class Builder {

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

    private boolean enableRetries = false;
    private int initialBackoffMillis = DEFAULT_INITIAL_BACKOFF_MILLIS;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
    private int maxElaspedBackoffMillis = DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;

    /**
     * Enable or disable retries.
     */
    public Builder setEnableRetries(boolean enabled) {
      this.enableRetries = enabled;
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
     * Construct a new RetryOptions object.
     */
    public RetryOptions build() {
      return new RetryOptions(
          enableRetries, initialBackoffMillis, backoffMultiplier, maxElaspedBackoffMillis);
    }
  }

  private final boolean retriesEnabled;
  private final int initialBackoffMillis;
  private final int maxElaspedBackoffMillis;
  private final double backoffMultiplier;

  public RetryOptions(
      boolean retriesEnabled,
      int initialBackoffMillis,
      double backoffMultiplier,
      int maxElaspedBackoffMillis) {
    this.retriesEnabled = retriesEnabled;
    this.initialBackoffMillis = initialBackoffMillis;
    this.maxElaspedBackoffMillis = maxElaspedBackoffMillis;
    this.backoffMultiplier = backoffMultiplier;
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
}
