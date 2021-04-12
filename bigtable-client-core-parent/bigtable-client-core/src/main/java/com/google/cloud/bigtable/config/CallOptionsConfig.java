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

import com.google.api.core.BetaApi;
import com.google.api.core.InternalExtensionOnly;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.grpc.CallOptions;
import java.io.Serializable;
import java.util.Objects;

/**
 * Experimental options to turn on timeout options. {@link CallOptions} supports other settings as
 * well, which this configuration object could help set.
 */
@InternalExtensionOnly
public class CallOptionsConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Constant <code>USE_TIMEOUT_DEFAULT=false</code> */
  public static final boolean USE_TIMEOUT_DEFAULT = false;

  /** The default duration to wait before timing out RPCs (default value: 20 seconds). */
  public static final int SHORT_TIMEOUT_MS_DEFAULT = 20_000;

  /**
   * The default duration to wait before timing out idempotent RPCs operation. 10 minute is probably
   * too long for most RPCs, but the intent is to have a conservative timeout by default and aim for
   * user overrides. There could very well be 10 minute scans, so keep the value conservative for
   * most cases and allow user overrides as needed.
   */
  public static final int LONG_TIMEOUT_MS_DEFAULT = 600_000;

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean useTimeout = USE_TIMEOUT_DEFAULT;
    private int shortRpcTimeoutMs = SHORT_TIMEOUT_MS_DEFAULT;
    private Optional<Integer> shortRpcAttemptTimeoutMs = Optional.absent();
    private int longRpcTimeoutMs = LONG_TIMEOUT_MS_DEFAULT;
    private int mutateRpcTimeoutMs = LONG_TIMEOUT_MS_DEFAULT;
    private Optional<Integer> mutateRpcAttemptTimeoutMs = Optional.absent();
    private int readStreamRpcTimeoutMs = LONG_TIMEOUT_MS_DEFAULT;
    private Optional<Integer> readRowsRpcAttemptTimeoutMs = Optional.absent();

    /** @deprecated Please use {@link CallOptionsConfig#builder()} */
    @Deprecated
    public Builder() {}

    private Builder(CallOptionsConfig original) {
      this.useTimeout = original.useTimeout;
      this.shortRpcTimeoutMs = original.shortRpcTimeoutMs;
      this.shortRpcAttemptTimeoutMs = original.shortRpcAttemptTimeoutMs;
      this.longRpcTimeoutMs = original.longRpcTimeoutMs;
      this.mutateRpcTimeoutMs = original.mutateRpcTimeoutMs;
      this.mutateRpcAttemptTimeoutMs = original.mutateRpcAttemptTimeoutMs;
      this.readStreamRpcTimeoutMs = original.readStreamRpcTimeoutMs;
      this.readRowsRpcAttemptTimeoutMs = original.readStreamRpcAttemptTimeoutMs;
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
     * @deprecated Use {@link #setShortRpcAttemptTimeoutMs(int)} instead
     */
    @Deprecated
    public Builder setTimeoutMs(int timeoutMs) {
      return setShortRpcTimeoutMs(timeoutMs);
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for a short RPC
     * operation.
     *
     * <p>By short RPC, we mean single-row operations like CheckAndMutateRow, MutateRow, and
     * ReadRows on a single key.
     *
     * <p>The timeout spans the full end-to-end operation from the user perspective, i.e. covering
     * multiple retry attempts.
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
     * The amount of milliseconds to wait before issuing a client side timeout for a short RPC
     * attempt.
     *
     * <p>See {@link #setShortRpcTimeoutMs(int)} for the definition of short RPC.
     *
     * <p>This timeout covers a single RPC call attempt to the server, whereas {@link
     * #setShortRpcTimeoutMs(int)} covers the total time across all attempts.
     *
     * @param shortRpcAttemptTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    @BetaApi("The API for setting attempt timeouts is not yet stable and may change in the future")
    public Builder setShortRpcAttemptTimeoutMs(int shortRpcAttemptTimeoutMs) {
      Preconditions.checkArgument(
          shortRpcAttemptTimeoutMs > 0, "Short Timeout ms has to be greater than 0.");
      this.shortRpcAttemptTimeoutMs = Optional.of(shortRpcAttemptTimeoutMs);
      return this;
    }

    /**
     * The amount of milliseconds to wait before issuing a client side timeout for long running
     * RPCs.
     *
     * @param longRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     * @deprecated Please use {@link #setMutateRpcTimeoutMs(int)} or {@link
     *     #setReadRowsRpcTimeoutMs(int)}.
     */
    @Deprecated
    public Builder setLongRpcTimeoutMs(int longRpcTimeoutMs) {
      Preconditions.checkArgument(
          longRpcTimeoutMs > 0, "Long running RPC Timeout ms has to be greater than 0");
      this.longRpcTimeoutMs = longRpcTimeoutMs;
      return this;
    }

    /**
     * The amount of time in milliseconds to wait before issuing a client side timeout for bulk row
     * mutation RPCs (i.e. multiple rows). Single-row mutations are governed by {@link
     * #shortRpcTimeoutMs};
     *
     * <p>The timeout spans the full end-to-end operation from the user perspective, i.e. covering
     * multiple retry attempts.
     *
     * @param mutateRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    public Builder setMutateRpcTimeoutMs(int mutateRpcTimeoutMs) {
      Preconditions.checkArgument(
          mutateRpcTimeoutMs > 0, "Mutate Rows RPC Timeout ms has to be greater than 0");
      this.mutateRpcTimeoutMs = mutateRpcTimeoutMs;
      return this;
    }

    /**
     * The amount of time in milliseconds to wait before issuing a client side timeout for bulk row
     * mutation RPCs (i.e. multiple rows). Single-row mutations are governed by {@link
     * #shortRpcAttemptTimeoutMs};
     *
     * <p>This timeout covers a single RPC call attempt to the server, whereas {@link
     * #setShortRpcTimeoutMs(int)} covers the total time across all attempts.
     *
     * @param mutateRpcAttemptTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    @BetaApi("The API for setting attempt timeouts is not yet stable and may change in the future")
    public Builder setMutateRpcAttemptTimeoutMs(int mutateRpcAttemptTimeoutMs) {
      Preconditions.checkArgument(
          mutateRpcAttemptTimeoutMs > 0, "Mutate Rows RPC Timeout ms has to be greater than 0");
      this.mutateRpcAttemptTimeoutMs = Optional.of(mutateRpcAttemptTimeoutMs);
      return this;
    }

    /**
     * The amount of time in millisecond to wait before issuing a client side timeout for readRows
     * streaming RPCs.
     *
     * <p>The timeout spans the full end-to-end operation from the user perspective, i.e. covering
     * multiple retry attempts.
     *
     * @param readStreamRpcTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    public Builder setReadRowsRpcTimeoutMs(int readStreamRpcTimeoutMs) {
      Preconditions.checkArgument(
          readStreamRpcTimeoutMs > 0, "Read Stream RPC Timeout ms has to be greater than 0");
      this.readStreamRpcTimeoutMs = readStreamRpcTimeoutMs;
      return this;
    }

    /**
     * The amount of time in milliseconds to wait before issuing a client side timeout for readRows
     * streaming RPCs.
     *
     * <p>This timeout covers a single RPC call attempt to the server, whereas {@link
     * #setShortRpcTimeoutMs(int)} covers the total time across all attempts.
     *
     * @param readRowsRpcAttemptTimeoutMs timeout value in milliseconds.
     * @return a {@link Builder} object, for chaining
     */
    @BetaApi("The API for setting attempt timeouts is not yet stable and may change in the future")
    public Builder setReadRowsRpcAttemptTimeoutMs(int readRowsRpcAttemptTimeoutMs) {
      Preconditions.checkArgument(
          readRowsRpcAttemptTimeoutMs > 0, "Mutate Rows RPC Timeout ms has to be greater than 0");
      this.readRowsRpcAttemptTimeoutMs = Optional.of(readRowsRpcAttemptTimeoutMs);
      return this;
    }

    public CallOptionsConfig build() {
      return new CallOptionsConfig(this);
    }
  }

  private final boolean useTimeout;
  private final int shortRpcTimeoutMs;
  private final Optional<Integer> shortRpcAttemptTimeoutMs;
  private final int longRpcTimeoutMs;
  private final int mutateRpcTimeoutMs;
  private final Optional<Integer> mutateRpcAttemptTimeoutMs;
  private final int readStreamRpcTimeoutMs;
  private final Optional<Integer> readStreamRpcAttemptTimeoutMs;

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
    this.shortRpcAttemptTimeoutMs = Optional.absent();
    this.longRpcTimeoutMs = longRpcTimeoutMs;
    this.mutateRpcTimeoutMs = longRpcTimeoutMs;
    this.mutateRpcAttemptTimeoutMs = Optional.absent();
    this.readStreamRpcTimeoutMs = longRpcTimeoutMs;
    this.readStreamRpcAttemptTimeoutMs = Optional.absent();
  }

  private CallOptionsConfig(Builder builder) {
    this.useTimeout = builder.useTimeout;
    this.shortRpcTimeoutMs = builder.shortRpcTimeoutMs;
    this.shortRpcAttemptTimeoutMs = builder.shortRpcAttemptTimeoutMs;
    this.longRpcTimeoutMs = builder.longRpcTimeoutMs;
    int mutateTimeout = builder.mutateRpcTimeoutMs;
    int readRowsTimeout = builder.readStreamRpcTimeoutMs;

    if (mutateTimeout == LONG_TIMEOUT_MS_DEFAULT && longRpcTimeoutMs != LONG_TIMEOUT_MS_DEFAULT) {
      mutateTimeout = longRpcTimeoutMs;
    }
    if (readRowsTimeout == LONG_TIMEOUT_MS_DEFAULT && longRpcTimeoutMs != LONG_TIMEOUT_MS_DEFAULT) {
      readRowsTimeout = longRpcTimeoutMs;
    }
    this.mutateRpcTimeoutMs = mutateTimeout;
    this.mutateRpcAttemptTimeoutMs = builder.mutateRpcAttemptTimeoutMs;
    this.readStreamRpcTimeoutMs = readRowsTimeout;
    this.readStreamRpcAttemptTimeoutMs = builder.readRowsRpcAttemptTimeoutMs;
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
   * Getter for the field <code>shortRpcAttemptTimeoutMs</code>.
   *
   * <p>If the Optional value is not present, the attempt timeout will leverage {@link
   * #shortRpcTimeoutMs}.
   *
   * @return an Optional int, populated if the property was present.
   */
  @BetaApi("The API for getting attempt timeouts is not yet stable and may change in the future")
  public Optional<Integer> getShortRpcAttemptTimeoutMs() {
    return shortRpcAttemptTimeoutMs;
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
   * Getter for the field <code>mutateRpcAttemptTimeoutMs</code>.
   *
   * <p>If the Optional value is not present, the attempt timeout will leverage {@link
   * #mutateRpcTimeoutMs}.
   *
   * @return an Optional int, populated if the property was present.
   */
  @BetaApi("The API for getting attempt timeouts is not yet stable and may change in the future")
  public Optional<Integer> getMutateRpcAttemptTimeoutMs() {
    return mutateRpcAttemptTimeoutMs;
  }

  /**
   * Getter for the field <code>readStreamRpcTimeoutMs</code>.
   *
   * @return an int.
   */
  public int getReadStreamRpcTimeoutMs() {
    return readStreamRpcTimeoutMs;
  }

  /**
   * Getter for the field <code>readStreamRpcAttemptTimeoutMs</code>.
   *
   * <p>If the Optional value is not present, the attempt timeout will leverage {@link
   * #readStreamRpcTimeoutMs}.
   *
   * @return an Optional int, populated if the property was present.
   */
  @BetaApi("The API for getting attempt timeouts is not yet stable and may change in the future")
  public Optional<Integer> getReadStreamRpcAttemptTimeoutMs() {
    return readStreamRpcAttemptTimeoutMs;
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
        && Objects.equals(shortRpcAttemptTimeoutMs, other.shortRpcAttemptTimeoutMs)
        && longRpcTimeoutMs == other.longRpcTimeoutMs
        && mutateRpcTimeoutMs == other.mutateRpcTimeoutMs
        && Objects.equals(mutateRpcAttemptTimeoutMs, other.mutateRpcAttemptTimeoutMs)
        && readStreamRpcTimeoutMs == other.readStreamRpcTimeoutMs
        && Objects.equals(readStreamRpcAttemptTimeoutMs, other.readStreamRpcAttemptTimeoutMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        useTimeout,
        shortRpcTimeoutMs,
        shortRpcAttemptTimeoutMs,
        longRpcTimeoutMs,
        mutateRpcTimeoutMs,
        mutateRpcAttemptTimeoutMs,
        readStreamRpcTimeoutMs,
        readStreamRpcAttemptTimeoutMs);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("useTimeout", useTimeout)
        .add("shortRpcTimeoutMs", shortRpcTimeoutMs)
        .add("shortRpcAttemptTimeoutMs", shortRpcAttemptTimeoutMs)
        .add("longRpcTimeoutMs", longRpcTimeoutMs)
        .add("mutateRpcTimeoutMs", mutateRpcTimeoutMs)
        .add("mutateRpcAttemptTimeoutMs", mutateRpcAttemptTimeoutMs)
        .add("readStreamRpcTimeoutMs", readStreamRpcTimeoutMs)
        .add("readStreamRpcAttemptTimeoutMs", readStreamRpcAttemptTimeoutMs)
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
