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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * An immutable class providing access to bulk related configuration options for Bigtable.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BulkOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Constant <code>BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT=2</code> */
  public static final int BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT = 2;

  /**
   * This describes the maximum size a bulk mutation RPC should be before sending it to the server
   * and starting the next bulk call. Defaults to 1 MB.
   */
  public static final long BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT = 1 << 20;

  /**
   * This describes the maximum number of individual mutation requests to bundle in a single bulk
   * mutation RPC before sending it to the server and starting the next bulk call.
   * The server has a maximum of 100,000.  Since RPCs can be retried, we should limit the number of
   * keys to 25 by default so we don't keep retrying larger batches.  25 is also better from the 
   * server's perspective.
   */
  public static final int BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT = 25;

  /**
   * Whether or not to enable a mechanism that reduces the likelihood that a {@link BulkMutation}
   * intensive application will overload a cluster.
   */
  public static final boolean BIGTABLE_BULK_ENABLE_THROTTLE_REBALANCE_DEFAULT = false;

  /**
   * The target RPC response time for a MutateRows request. This value is meaningful if bulk
   * mutation throttling is enabled. 40 ms. is a generally ok latency for MutateRows rpcs, but it
   * could go higher (for example 200 ms) for less latency sensitive applications that need more
   * throughput, or lower (10 ms) for latency sensitive applications.
   */
  public static final int BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT = 40;

  /**
   * The maximum amount of time a row will be buffered for. By default 0: indefinitely.
   */
  public static long BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT = 0;

  /** Default rpc count per channel. */
  public static final int BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT = 50;

  /**
    * This is the maximum accumulated size of uncompleted requests that we allow before throttling.
    * Default to 10% of available memory with a max of 1GB.
   */
  public static final long BIGTABLE_MAX_MEMORY_DEFAULT =
      (long) Math.min(1 << 30, (Runtime.getRuntime().maxMemory() * 0.1d));

  /**
   * A mutable builder for BigtableConnectionOptions.
   */
  public static class Builder {

    private int asyncMutatorCount = BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT;
    private boolean useBulkApi = false;
    private int bulkMaxRowKeyCount = BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT;
    private long bulkMaxRequestSize = BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT;
    private long autoflushMs = BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT;
    private int maxInflightRpcs = -1;
    private long maxMemory = BIGTABLE_MAX_MEMORY_DEFAULT;
    private boolean enableBulkMutationThrottling = BIGTABLE_BULK_ENABLE_THROTTLE_REBALANCE_DEFAULT;
    private int bulkMutationRpcTargetMs = BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT;

    public Builder() {
    }

    private Builder(BulkOptions original) {
      this.asyncMutatorCount = original.asyncMutatorCount;
      this.useBulkApi = original.useBulkApi;
      this.bulkMaxRowKeyCount = original.bulkMaxRowKeyCount;
      this.bulkMaxRequestSize = original.bulkMaxRequestSize;
      this.autoflushMs = original.autoflushMs;
      this.maxInflightRpcs = original.maxInflightRpcs;
      this.maxMemory = original.maxMemory;
      this.enableBulkMutationThrottling = original.enableBulkMutationThrottling;
      this.bulkMutationRpcTargetMs = original.bulkMutationRpcTargetMs;
    }

    public Builder setAsyncMutatorWorkerCount(int asyncMutatorCount) {
      Preconditions.checkArgument(
          asyncMutatorCount >= 0, "asyncMutatorCount must be greater or equal to 0.");
      this.asyncMutatorCount = asyncMutatorCount;
      return this;
    }

    public Builder setUseBulkApi(boolean useBulkApi) {
      this.useBulkApi = useBulkApi;
      return this;
    }

    public Builder setBulkMaxRowKeyCount(int bulkMaxRowKeyCount) {
      Preconditions.checkArgument(
        bulkMaxRowKeyCount >= 0, "bulkMaxRowKeyCount must be greater or equal to 0.");
      this.bulkMaxRowKeyCount = bulkMaxRowKeyCount;
      return this;
    }

    public Builder setBulkMaxRequestSize(long bulkMaxRequestSize) {
      Preconditions.checkArgument(
        bulkMaxRequestSize >= 0, "bulkMaxRequestSize must be greater or equal to 0.");
      this.bulkMaxRequestSize = bulkMaxRequestSize;
      return this;
    }

    public Builder setAutoflushMs(long autoflushMs) {
      Preconditions.checkArgument(
          autoflushMs >= 0, "autoflushMs must be greater or equal to 0.");
      this.autoflushMs = autoflushMs;
      return this;
    }

    public Builder setMaxInflightRpcs(int maxInflightRpcs) {
      Preconditions.checkArgument(maxInflightRpcs > 0, "maxInflightRpcs must be greater than 0.");
      this.maxInflightRpcs = maxInflightRpcs;
      return this;
    }

    public Builder setMaxMemory(long maxMemory) {
      Preconditions.checkArgument(maxMemory > 0, "maxMemory must be greater than 0.");
      this.maxMemory = maxMemory;
      return this;
    }

    public Builder enableBulkMutationThrottling() {
      this.enableBulkMutationThrottling = true;
      return this;
    }

    public Builder setBulkMutationRpcTargetMs(int bulkMutationRpcTargetMs) {
      this.bulkMutationRpcTargetMs = bulkMutationRpcTargetMs;
      return this;
    }

    public BulkOptions build() {
      return new BulkOptions(
          asyncMutatorCount,
          useBulkApi,
          bulkMaxRowKeyCount,
          bulkMaxRequestSize,
          autoflushMs,
          maxInflightRpcs,
          maxMemory,
          enableBulkMutationThrottling,
          bulkMutationRpcTargetMs);
    }
  }

  private final int asyncMutatorCount;
  private final boolean useBulkApi;
  private final int bulkMaxRowKeyCount;
  private final long bulkMaxRequestSize;
  private final long autoflushMs;

  private final int maxInflightRpcs;
  private final long maxMemory;

  private final boolean enableBulkMutationThrottling;
  private final int bulkMutationRpcTargetMs;

  @VisibleForTesting
  BulkOptions() {
      asyncMutatorCount = 1;
      useBulkApi = false;
      bulkMaxRowKeyCount = -1;
      bulkMaxRequestSize = -1;
      autoflushMs = -1l;
      maxInflightRpcs = -1;
      maxMemory = -1l;
      enableBulkMutationThrottling = false;
      bulkMutationRpcTargetMs = -1;
  }

  private BulkOptions(
      int asyncMutatorCount,
      boolean useBulkApi,
      int bulkMaxKeyCount,
      long bulkMaxRequestSize,
      long autoflushMs,
      int maxInflightRpcs,
      long maxMemory,
      boolean enableBulkMutationThrottling,
      int bulkMutationRpcTargetMs) {
    this.asyncMutatorCount = asyncMutatorCount;
    this.useBulkApi = useBulkApi;
    this.bulkMaxRowKeyCount = bulkMaxKeyCount;
    this.bulkMaxRequestSize = bulkMaxRequestSize;
    this.autoflushMs = autoflushMs;
    this.maxInflightRpcs = maxInflightRpcs;
    this.maxMemory = maxMemory;
    this.enableBulkMutationThrottling = enableBulkMutationThrottling;
    this.bulkMutationRpcTargetMs = bulkMutationRpcTargetMs;
  }

  /**
   * <p>Getter for the field <code>asyncMutatorCount</code>.</p>
   *
   * @return a int.
   */
  public int getAsyncMutatorCount() {
    return asyncMutatorCount;
  }

  /**
   * <p>useBulkApi.</p>
   *
   * @return a boolean.
   */
  public boolean useBulkApi() {
    return useBulkApi;
  }

  /**
   * <p>Getter for the field <code>bulkMaxRowKeyCount</code>.</p>
   *
   * @return a int.
   */
  public int getBulkMaxRowKeyCount() {
    return bulkMaxRowKeyCount;
  }

  /**
   * <p>Getter for the field <code>bulkMaxRequestSize</code>.</p>
   *
   * @return a long.
   */
  public long getBulkMaxRequestSize() {
    return bulkMaxRequestSize;
  }

  /**
   * <p>Getter for the field <code>autoflushMs</code>.</p>
   * @return a long
   */
  public long getAutoflushMs() {
    return autoflushMs;
  }

  /**
   * <p>Getter for the field <code>maxInflightRpcs</code>.</p>
   *
   * @return a int.
   */
  public int getMaxInflightRpcs() {
    return maxInflightRpcs;
  }

  /**
   * <p>Getter for the field <code>maxMemory</code>.</p>
   *
   * @return a long.
   */
  public long getMaxMemory() {
    return maxMemory;
  }

  /**
   * Is an experimental feature of throttling bulk mutation RPCs turned on?
   *
   * @return a boolean
   */
  public boolean isEnableBulkMutationThrottling() {
    return enableBulkMutationThrottling;
  }

  /**
   * if {@link #isEnableBulkMutationThrottling()}, then bulk mutation RPC latency will be compared
   * against this value. If the RPC latency is higher, then some throttling will be applied.
   * @return the number of milliseconds that is an appropriate amount of time for a bulk mutation
   *         RPC.
   */
  public int getBulkMutationRpcTargetMs() {
    return bulkMutationRpcTargetMs;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != BulkOptions.class) {
      return false;
    }
    BulkOptions other = (BulkOptions) obj;
    return (asyncMutatorCount == other.asyncMutatorCount)
        && (useBulkApi == other.useBulkApi)
        && (bulkMaxRowKeyCount == other.bulkMaxRowKeyCount)
        && (bulkMaxRequestSize == other.bulkMaxRequestSize)
        && (autoflushMs == other.autoflushMs)
        && (maxInflightRpcs == other.maxInflightRpcs)
        && (maxMemory == other.maxMemory)
        && (enableBulkMutationThrottling == other.enableBulkMutationThrottling)
        && (bulkMutationRpcTargetMs == other.bulkMutationRpcTargetMs);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("asyncMutatorCount", asyncMutatorCount)
        .add("useBulkApi", useBulkApi)
        .add("bulkMaxKeyCount", bulkMaxRowKeyCount)
        .add("bulkMaxRequestSize", bulkMaxRequestSize)
        .add("autoflushMs", autoflushMs)
        .add("maxInflightRpcs", maxInflightRpcs)
        .add("maxMemory", maxMemory)
        .add("enableBulkMutationThrottling", enableBulkMutationThrottling)
        .add("bulkMutationRpcTargetMs", bulkMutationRpcTargetMs)
        .toString();
  }

  /**
   * <p>toBuilder.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.BulkOptions.Builder} object.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }
}
