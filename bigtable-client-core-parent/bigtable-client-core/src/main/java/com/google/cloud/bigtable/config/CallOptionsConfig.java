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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.io.Serializable;

/**
 * Experimental options to turn on timeout options. {@link io.grpc.CallOptions} supports other
 * settings as well, which this configuration object could help set.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class CallOptionsConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Constant <code>USE_TIMEOUT_DEFAULT=false</code> */
  public static final boolean USE_TIMEOUT_DEFAULT = false;

  /**
   * The default duration to wait before timing out RPCs. 1 minute is probably too long for most
   * RPCs, but the intent is to have a conservative timeout by default and aim for user overrides.
   */
  public static final int SHORT_TIMEOUT_MS_DEFAULT = 60_000;

  /**
   * The default duration to wait before timing out RPCs. 10 minute is probably too long for most
   * RPCs, but the intent is to have a conservative timeout by default and aim for user overrides.
   * There could very well be 10 minute scans, so keep the value conservative for most cases and
   * allow user overrides as needed.
   */
  public static final int LONG_TIMEOUT_MS_DEFAULT = 600_000;

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean useTimeout = USE_TIMEOUT_DEFAULT;
    private int shortRpcTimeoutMs = SHORT_TIMEOUT_MS_DEFAULT;
    private int longRpcTimeoutMs = LONG_TIMEOUT_MS_DEFAULT;
    private int mutateRpcTimeoutMs = LONG_TIMEOUT_MS_DEFAULT;
    private int readRowsRpcTimeoutMs = LONG_TIMEOUT_MS_DEFAULT;

    @Deprecated
    public Builder() {}

    private Builder(CallOptionsConfig original) {
      this.useTimeout = original.useTimeout;
      this.shortRpcTimeoutMs = original.shortRpcTimeoutMs;
      this.longRpcTimeoutMs = original.longRpcTimeoutMs;
      this.mutateRpcTimeoutMs = original.mutateRpcTimeoutMs;
      this.readRowsRpcTimeoutMs = original.readStreamRpcTimeoutMs;
    }

    /**
     * If true, turn on timeouts for unary RPCS like mutations, and single row readRows.
     *
     * @param useTimeout flag to enable use of timeout.
     * @return a {@link Builder} object, for chaining
     */
    public Builder setUseTimeout(boolean useTimeout) {
      this.useTimeout = useTimeout;
      return this;
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for short RPCs.
     *
     * @param timeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    @Deprecated
    public Builder setTimeoutMs(int timeoutMs) {
      return setShortRpcTimeoutMs(timeoutMs);
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for short RPCs.
     *
     * @param shortRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    public Builder setShortRpcTimeoutMs(int shortRpcTimeoutMs) {
      Preconditions.checkArgument(
          shortRpcTimeoutMs > 0, "Short Timeout ms has to be greater than 0.");
      this.shortRpcTimeoutMs = shortRpcTimeoutMs;
      return this;
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for long RPCs.
     *
     * @param longRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     * @deprecated Please use {@link #setMutateRpcTimeoutMs(int)} or {@link
     *     #setReadRowsRpcTimeoutMs(int)}.
     */
    @Deprecated
    public Builder setLongRpcTimeoutMs(int longRpcTimeoutMs) {
      Preconditions.checkArgument(longRpcTimeoutMs > 0, "Long Timeout ms has to be greater than 0");
      this.longRpcTimeoutMs = longRpcTimeoutMs;
      return this;
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for mutation RPCs.
     *
     * @param mutateRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    public Builder setMutateRpcTimeoutMs(int mutateRpcTimeoutMs) {
      Preconditions.checkArgument(
          mutateRpcTimeoutMs > 0, "Long Timeout ms has to be greater than 0");
      this.mutateRpcTimeoutMs = mutateRpcTimeoutMs;
      return this;
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for readRows
     * streaming RPCs.
     *
     * @param readStreamRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    public Builder setReadRowsRpcTimeoutMs(int readStreamRpcTimeoutMs) {
      Preconditions.checkArgument(
          readStreamRpcTimeoutMs > 0, "Read Stream Timeout ms has to be greater than 0");
      this.readRowsRpcTimeoutMs = readStreamRpcTimeoutMs;
      return this;
    }

    public CallOptionsConfig build() {
      return new CallOptionsConfig(this);
    }
  }

  private final boolean useTimeout;
  private final int shortRpcTimeoutMs;
  private final int longRpcTimeoutMs;
  private final int mutateRpcTimeoutMs;
  private final int readStreamRpcTimeoutMs;

  /**
   * Constructor for CallOptionsConfig.
   *
   * @param useTimeout a boolean.
   * @param unaryRpcTimeoutMs an int.
   * @param longRpcTimeoutMs an int.
   * @deprecated Please use {@link #builder()}
   */
  @Deprecated
  public CallOptionsConfig(boolean useTimeout, int unaryRpcTimeoutMs, int longRpcTimeoutMs) {
    this.useTimeout = useTimeout;
    this.shortRpcTimeoutMs = unaryRpcTimeoutMs;
    this.longRpcTimeoutMs = longRpcTimeoutMs;
    this.mutateRpcTimeoutMs = longRpcTimeoutMs;
    this.readStreamRpcTimeoutMs = longRpcTimeoutMs;
  }

  private CallOptionsConfig(Builder builder) {
    this.useTimeout = builder.useTimeout;
    this.shortRpcTimeoutMs = builder.shortRpcTimeoutMs;
    this.longRpcTimeoutMs = builder.longRpcTimeoutMs;
    int mutateTimeout = builder.mutateRpcTimeoutMs;
    int readRowsTimeout = builder.readRowsRpcTimeoutMs;

    if (mutateTimeout == LONG_TIMEOUT_MS_DEFAULT && longRpcTimeoutMs != LONG_TIMEOUT_MS_DEFAULT) {
      mutateTimeout = longRpcTimeoutMs;
    }
    if (readRowsTimeout == LONG_TIMEOUT_MS_DEFAULT && longRpcTimeoutMs != LONG_TIMEOUT_MS_DEFAULT) {
      readRowsTimeout = longRpcTimeoutMs;
    }
    this.mutateRpcTimeoutMs = mutateTimeout;
    this.readStreamRpcTimeoutMs = readRowsTimeout;
  }

  /**
   * isUseTimeout.
   *
   * @return a boolean.
   */
  public boolean isUseTimeout() {
    return useTimeout;
  }

  /**
   * Getter for the field <code>shortRpcTimeoutMs</code>. Use {@link #getShortRpcTimeoutMs()}
   * instead.
   *
   * @return an int.
   */
  @Deprecated
  public int getTimeoutMs() {
    return getShortRpcTimeoutMs();
  }

  /**
   * Getter for the field <code>shortRpcTimeoutMs</code>.
   *
   * @return an int.
   */
  public int getShortRpcTimeoutMs() {
    return shortRpcTimeoutMs;
  }

  /**
   * Getter for the field <code>longRpcTimeoutMs</code>.
   *
   * @return an int.
   * @deprecated Please use {@link #getMutateRpcTimeoutMs()} or {@link
   *     #getReadStreamRpcTimeoutMs()}.
   */
  @Deprecated
  public int getLongRpcTimeoutMs() {
    return longRpcTimeoutMs;
  }

  /**
   * Getter for the field <code>mutateRpcTimeoutMs</code>.
   *
   * @return an int.
   */
  public int getMutateRpcTimeoutMs() {
    return mutateRpcTimeoutMs;
  }

  /**
   * Getter for the field <code>readStreamRpcTimeoutMs</code>.
   *
   * @return an int.
   */
  public int getReadStreamRpcTimeoutMs() {
    return readStreamRpcTimeoutMs;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != CallOptionsConfig.class) {
      return false;
    }
    CallOptionsConfig other = (CallOptionsConfig) obj;
    return useTimeout == other.useTimeout
        && shortRpcTimeoutMs == other.shortRpcTimeoutMs
        && longRpcTimeoutMs == other.longRpcTimeoutMs
        && mutateRpcTimeoutMs == other.mutateRpcTimeoutMs
        && readStreamRpcTimeoutMs == other.readStreamRpcTimeoutMs;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("useTimeout", useTimeout)
        .add("shortRpcTimeoutMs", shortRpcTimeoutMs)
        .add("longRpcTimeoutMs", longRpcTimeoutMs)
        .add("mutateRpcTimeoutMs", mutateRpcTimeoutMs)
        .add("readStreamRpcTimeoutMs", readStreamRpcTimeoutMs)
        .toString();
  }

  /**
   * toBuilder.
   *
   * @return a {@link com.google.cloud.bigtable.config.CallOptionsConfig.Builder} object.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }
}
