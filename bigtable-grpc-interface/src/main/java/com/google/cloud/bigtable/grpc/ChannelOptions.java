package com.google.cloud.bigtable.grpc;

import com.google.auth.Credentials;
import com.google.common.base.Preconditions;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Options for constructing an Bigtable RPC channel.
 */
public class ChannelOptions {

  /**
   * A Builder for ChannelOptions objects.
   */
  public static class Builder {
    private Credentials credential;
    private String authority;
    private String callStatusReportPath;
    private String callTimingReportPath;
    private ScheduledExecutorService scheduledExecutorService = null;
    private RetryOptions.Builder retryOptionsBuilder = new RetryOptions.Builder();
    private long timeoutMs = -1;
    private int channelCount = 1;

    /**
     * Credential to use to connect to Bigtable.
     */
    public Builder setCredential(Credentials credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Authority to provide in outgoing headers.
     */
    public Builder setAuthority(String authority) {
      this.authority = authority;
      return this;
    }

    /**
     * A client-local file to which a report of call statuses will be appended.
     */
    public Builder setCallStatusReportPath(String callStatusReportPath) {
      this.callStatusReportPath = callStatusReportPath;
      return this;
    }

    /**
     * A client-local file to which a report of call timings will be appended.
     */
    public Builder setCallTimingReportPath(String callTimingReportPath) {
      this.callTimingReportPath = callTimingReportPath;
      return this;
    }

    /**
     * Enable retries on the channel.
     * Deprecated, use the RetryOptions.Builder returned from getUnaryCallRetryOptionsBuilder().
     */
    @Deprecated
    public Builder setEnableRetries(boolean enableRetries) {
      retryOptionsBuilder.setEnableRetries(enableRetries);
      return this;
    }

    /**
     * The amount of time in miliiseconds we will wait for our first error retry.
     * Deprecated, use the RetryOptions.Builder returned from getUnaryCallRetryOptionsBuilder().
     */
    @Deprecated
    public Builder setInitialBackoffMillis(int initialBackoffMillis) {
      retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);
      return this;
    }

    /**
     * Multiplier we will apply to backoff times between retries.
     * Deprecated, use the RetryOptions.Builder returned from getUnaryCallRetryOptionsBuilder().
     */
    @Deprecated
    public Builder setBackoffMultiplier(double multiplier) {
      retryOptionsBuilder.setBackoffMultiplier(multiplier);
      return this;
    }

    /**
     * Maximum amount of time we will retry an operation that is failing.
     * Deprecated, use the RetryOptions.Builder returned from getUnaryCallRetryOptionsBuilder().
     */
    @Deprecated
    public Builder maxElapsedBackoffMillis(int maxElaspedBackoffMillis) {
      retryOptionsBuilder.setMaxElapsedBackoffMillis(maxElaspedBackoffMillis);
      return this;
    }

    /**
     * Get a builder for retry options.
     */
    public RetryOptions.Builder getUnaryCallRetryOptionsBuilder() {
      return retryOptionsBuilder;
    }

    /**
     * The ScheduledExecutorService to use to perform rpc retries.
     */
    public Builder setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
      this.scheduledExecutorService = scheduledExecutorService;
      return this;
    }

    /**
     * The number of channels to create.
     */
    public Builder setChannelCount(int channelCount) {
      Preconditions.checkState(channelCount > 0);
      this.channelCount  = channelCount;
      return this;
    }

    /**
     * The timeout after which a Channel should be discarded.
     */
    public Builder setTimeoutMs(long timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public ChannelOptions build() {
      return new ChannelOptions(
          credential,
          authority,
          callTimingReportPath,
          callStatusReportPath,
          retryOptionsBuilder.build(),
          scheduledExecutorService,
          timeoutMs,
          channelCount);
    }
  }

  private final Credentials credential;
  private final String authority;
  private final String callTimingReportPath;
  private final String callStatusReportPath;
  private final RetryOptions unaryCallRetryOptions;
  private final ScheduledExecutorService scheduledExecutorService;
  private final long timeoutMs;
  private final int channelCount;


  /**
   * Construct a ChannelOptions object
   * @param credential A credential to use, may be null.
   * @param authority The authority to be passed in the HTTP/2 headers, or {@code null}
   * to use the default.
   * @param callTimingReportPath A client-local file to which a report of call timings
   * will be appended
   * @param callStatusReportPath A client-local file to which a report of call statuses
   * will be appended
   * @param unaryCallRetryOptions Options for how to handle retriable failed UnaryCalls.
   * @param scheduledExecutorService ScheduledExecutorService on which to retry RPCs.
   */
  public ChannelOptions(Credentials credential,
      String authority,
      String callTimingReportPath,
      String callStatusReportPath,
      RetryOptions unaryCallRetryOptions,
      ScheduledExecutorService scheduledExecutorService,
      long timeoutMs,
      int channelCount) {
    this.credential = credential;
    this.authority = authority;
    this.callTimingReportPath = callTimingReportPath;
    this.callStatusReportPath = callStatusReportPath;
    this.scheduledExecutorService = scheduledExecutorService;
    this.unaryCallRetryOptions = unaryCallRetryOptions;
    this.timeoutMs = timeoutMs;
    this.channelCount = channelCount;
  }

  /**
   * Get the credential this object was constructed with. May be null.
   * @return Null to indicate no credentials, otherwise, the Credentials object.
   */
  public Credentials getCredential() {
    return credential;
  }

  /**
   * Gets the authority to be passed in the HTTP/2 headers when creating new streams
   * for the channel.
   */
  public String getAuthority() {
    return authority;
  }

  /**
   * Get the client-local file to which a report of call timings will be appended.
   */
  public String getCallTimingReportPath() {
    return callTimingReportPath;
  }

  /**
   * Get the client-local file to which a report of call statuses will be appended.
   */
  public String getCallStatusReportPath() {
    return callStatusReportPath;
  }

  /**
   * Options controlling retries for retriable uanry calls.
   */
  public RetryOptions getUnaryCallRetryOptions() { return unaryCallRetryOptions; }

  /**
   * The ScheduledExecutorService to use for RPC retries.
   */
  public ScheduledExecutorService getScheduledExecutorService() {
    return this.scheduledExecutorService;
  }

  /**
   * The timeout for a channel.
   */
  public long getTimeoutMs() {
    return timeoutMs;
  }

  /**
   * The number of channels to create.
   */
  public int getChannelCount() {
    return channelCount;
  }
}
