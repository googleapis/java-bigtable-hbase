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
 */
public class BulkOptions implements Serializable {

  private static final long serialVersionUID = 1L;

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
   * keys to 100 by default so we don't keep retrying larger batches.
   */
  public static final int BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT = 100;


  // Default rpc count per channel.
  public static final int BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT = 50;

  // This is the maximum accumulated size of uncompleted requests that we allow before throttling.
  // Default to 10% of available memory with a max of 1GB.
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
    private int maxInflightRpcs = -1;
    private long maxMemory = BIGTABLE_MAX_MEMORY_DEFAULT;

    public Builder() {
    }

    private Builder(BulkOptions original) {
      this.asyncMutatorCount = original.asyncMutatorCount;
      this.useBulkApi = original.useBulkApi;
      this.bulkMaxRowKeyCount = original.bulkMaxRowKeyCount;
      this.bulkMaxRequestSize = original.bulkMaxRequestSize;
      this.maxInflightRpcs = original.maxInflightRpcs;
      this.maxMemory = original.maxMemory;
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

    public BulkOptions build() {
      return new BulkOptions(
          asyncMutatorCount,
          useBulkApi,
          bulkMaxRowKeyCount,
          bulkMaxRequestSize,
          maxInflightRpcs,
          maxMemory);
    }
  }

  private final int asyncMutatorCount;
  private final boolean useBulkApi;
  private final int bulkMaxRowKeyCount;
  private final long bulkMaxRequestSize;

  private final int maxInflightRpcs;
  private final long maxMemory;

  @VisibleForTesting
  BulkOptions() {
      asyncMutatorCount = 1;
      useBulkApi = false;
      bulkMaxRowKeyCount = -1;
      bulkMaxRequestSize = -1;
      maxInflightRpcs = -1;
      maxMemory = -1l;
  }

  private BulkOptions(
      int asyncMutatorCount,
      boolean useBulkApi,
      int bulkMaxKeyCount,
      long bulkMaxRequestSize,
      int maxInflightRpcs,
      long maxMemory) {
    this.asyncMutatorCount = asyncMutatorCount;
    this.useBulkApi = useBulkApi;
    this.bulkMaxRowKeyCount = bulkMaxKeyCount;
    this.bulkMaxRequestSize = bulkMaxRequestSize;
    this.maxInflightRpcs = maxInflightRpcs;
    this.maxMemory = maxMemory;
  }

  public int getAsyncMutatorCount() {
    return asyncMutatorCount;
  }

  public boolean useBulkApi() {
    return useBulkApi;
  }

  public int getBulkMaxRowKeyCount() {
    return bulkMaxRowKeyCount;
  }

  public long getBulkMaxRequestSize() {
    return bulkMaxRequestSize;
  }

  public int getMaxInflightRpcs() {
    return maxInflightRpcs;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

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
        && (maxInflightRpcs == other.maxInflightRpcs)
        && (maxMemory == other.maxMemory);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("asyncMutatorCount", asyncMutatorCount)
        .add("useBulkApi", useBulkApi)
        .add("bulkMaxKeyCount", bulkMaxRowKeyCount)
        .add("bulkMaxRequestSize", bulkMaxRequestSize)
        .add("maxInflightRpcs", maxInflightRpcs)
        .add("maxMemory", maxMemory)
        .toString();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }
}