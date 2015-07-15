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

import com.google.api.client.util.Strings;
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
    private String userAgent;
    private String callStatusReportPath;
    private String callTimingReportPath;
    private ScheduledExecutorService scheduledExecutorService = null;
    private RetryOptions retryOptions = null;
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
     * A user-agent to be appended to outgoing User-Agent header.
     */
    public Builder setUserAgent(String userAgent) {
      this.userAgent = userAgent;
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
      this.channelCount  = channelCount;
      return this;
    }

    /**
     * The retry options.
     */
    public Builder setRetryOptions(RetryOptions retryOptions) {
      this.retryOptions = retryOptions;
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
          userAgent,
          callTimingReportPath,
          callStatusReportPath,
          retryOptions,
          scheduledExecutorService,
          timeoutMs,
          channelCount);
    }
  }

  private final Credentials credential;
  private final String authority;
  private final String userAgent;
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
   * @param userAgent The user-agent to be appended to HTTP/2 User-Agent header, or {@code null}
   * to use the default. Example: "bigtable-hbase/1.0.0".
   * @param callTimingReportPath A client-local file to which a report of call timings
   * will be appended
   * @param callStatusReportPath A client-local file to which a report of call statuses
   * will be appended
   * @param unaryCallRetryOptions Options for how to handle retriable failed UnaryCalls.
   * @param scheduledExecutorService ScheduledExecutorService on which to retry RPCs.
   */
  public ChannelOptions(Credentials credential,
      String authority,
      String userAgent,
      String callTimingReportPath,
      String callStatusReportPath,
      RetryOptions unaryCallRetryOptions,
      ScheduledExecutorService scheduledExecutorService,
      long timeoutMs,
      int channelCount) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(userAgent),
        "UserAgent must not be empty or null");
    Preconditions.checkArgument(channelCount > 0, "Channel count has to be at least 1.");
    Preconditions.checkArgument(timeoutMs >= -1,
      "ChannelTimeoutMs has to be positive, or -1 for none.");

    this.credential = credential;
    this.authority = authority;
    this.userAgent = userAgent;
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
   * Gets the user-agent to be appended to User-Agent header when creating new streams
   * for the channel.
   */
  public String getUserAgent() {
    return userAgent;
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
