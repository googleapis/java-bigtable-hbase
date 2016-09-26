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

import java.io.Serializable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Experimental options to turn on timeout options. {@link io.grpc.CallOptions} supports other settings as
 * well, which this configuration object could help set.
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
  public static final int TIMEOUT_MS_DEFAULT = 60_000;

  /**
   * The default duration to wait before timing out RPCs. 1 minute is probably too long for most
   * RPCs, but the intent is to have a conservative timeout by default and aim for user overrides.
   */
  public static final int SCAN_TIMEOUT_MS_DEFAULT = 60_000;

  public static class Builder {
    private boolean useTimeout = USE_TIMEOUT_DEFAULT;
    private int shortRpcTimeoutMs = TIMEOUT_MS_DEFAULT;
    private int longRpcTimeoutMs = SCAN_TIMEOUT_MS_DEFAULT;

    public Builder() {
    }

    private Builder(CallOptionsConfig original) {
      this.useTimeout = original.useTimeout;
      this.shortRpcTimeoutMs = original.shortRpcTimeoutMs;
      this.longRpcTimeoutMs = original.longRpcTimeoutMs;
    }

    /**
     * If true, turn on timeouts for unary RPCS like mutations, and single row readRows.
     * @param useTimeout
     * @return
     */
    public Builder setUseTimeout(boolean useTimeout) {
      this.useTimeout = useTimeout;
      return this;
    }

    /**
     * If true, turn on timeouts for unary RPCS like mutations, and single row readRows. 
     * @param useTimeout
     * @return this for chaining
     */
    @Deprecated
    public Builder setTimeoutMs(int timeoutMs) {
      return setShortRpcTimeoutMs(timeoutMs);
    }

   /**
    * If true, turn on timeouts for unary RPCS like mutations, and single row readRows. 
    * @param shortRpcTimeoutMs
    * @return this for chaining
    */
    public Builder setShortRpcTimeoutMs(int shortRpcTimeoutMs) {
      Preconditions.checkArgument(shortRpcTimeoutMs > 0, "Short Timeout ms has to be greater than 0");
      this.shortRpcTimeoutMs = shortRpcTimeoutMs;
      return this;
    }

    /**
     * If true, turn on timeouts for unary RPCS, specifically multi-row readRows. 
     * @param longRpcTimeoutMs
     * @return 
     * @return this for chaining
     */
    public Builder setLongRpcTimeoutMs(int longRpcTimeoutMs) {
      Preconditions.checkArgument(longRpcTimeoutMs > 0, "Long Timeout ms has to be greater than 0");
      this.longRpcTimeoutMs = longRpcTimeoutMs;
      return this;
    }

    public CallOptionsConfig build() {
      return new CallOptionsConfig(useTimeout, shortRpcTimeoutMs, longRpcTimeoutMs);
    }
  }

  private final boolean useTimeout;
  private final int shortRpcTimeoutMs;
  private final int longRpcTimeoutMs;

  /**
   * <p>Constructor for CallOptionsConfig.</p>
   *
   * @param useTimeout a boolean.
   * @param unaryRpcTimeoutMs a int.
   * @param longRpcTimeoutMs a int.
   */
  public CallOptionsConfig(boolean useTimeout, int unaryRpcTimeoutMs, int longRpcTimeoutMs) {
    this.useTimeout = useTimeout;
    this.shortRpcTimeoutMs = unaryRpcTimeoutMs;
    this.longRpcTimeoutMs = longRpcTimeoutMs;
  }

  /**
   * <p>isUseTimeout.</p>
   *
   * @return a boolean.
   */
  public boolean isUseTimeout() {
    return useTimeout;
  }

  /**
   * <p>Getter for the field <code>shortRpcTimeoutMs</code>.</p>
   *
   * @return a int.
   */
  @Deprecated
  public int getTimeoutMs() {
    return shortRpcTimeoutMs;
  }

  /**
   * <p>Getter for the field <code>shortRpcTimeoutMs</code>.</p>
   *
   * @return a int.
   */
  public int getShortRpcTimeoutMs() {
    return shortRpcTimeoutMs;
  }

  /**
   * <p>Getter for the field <code>longRpcTimeoutMs</code>.</p>
   *
   * @return a int.
   */
  public int getLongRpcTimeoutMs() {
    return longRpcTimeoutMs;
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
        && longRpcTimeoutMs == other.longRpcTimeoutMs;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("useTimeout", useTimeout)
        .add("timeoutMs", shortRpcTimeoutMs)
        .toString();
  }

  /**
   * <p>toBuilder.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.CallOptionsConfig.Builder} object.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }
}
