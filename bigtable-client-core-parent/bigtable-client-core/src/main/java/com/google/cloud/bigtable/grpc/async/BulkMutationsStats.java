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
package com.google.cloud.bigtable.grpc.async;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

/**
 * This class tracks timing and counts of mutations performed by {@link BulkMutation} and throttling
 * performed by {@link ResourceLimiter}.
 */
public class BulkMutationsStats {

  private static BulkMutationsStats instance = new BulkMutationsStats();

  public static BulkMutationsStats getInstance() {
    return instance;
  }

  /**
   * This class is a point in time summary of mean latency and mutaiton throughput.
   */
  public static class StatsSnapshot {
    private final Long mutationRpcLatencyInMillis;
    private final Long mutationRatePerSecond;
    private final Long throttlingPerRpcInMicros;

    public StatsSnapshot(@Nullable Long mutationRpcLatencyInMillis,
        @Nullable Long mutationRatePerSecond, @Nullable Long throttlingPerRpcInMicros) {
      this.mutationRpcLatencyInMillis = mutationRpcLatencyInMillis;
      this.mutationRatePerSecond = mutationRatePerSecond;
      this.throttlingPerRpcInMicros = throttlingPerRpcInMicros;
    }

    /**
     * @return latency of RPCs in milliseconds.  Return null if no RPCs have been made.
     */
    @Nullable
    public Long getMutationRpcLatencyInMillis() {
      return mutationRpcLatencyInMillis;
    }

    /**
     * @return mutations per second.  Return null if no RPCs have been made.
     */
    @Nullable
    public Long getMutationRatePerSecond() {
      return mutationRatePerSecond;
    }

    /**
     * @return throttling per RPC in microseconds.  Return null if no RPCs have been made.
     */
    @Nullable
    public Long getThrottlingPerRpcInMicros() {
      return throttlingPerRpcInMicros;
    }
  }

  private final MetricRegistry registry = new MetricRegistry();

  private Timer mutationTimer;
  private Meter mutationMeter;
  private Timer throttlingTimer;

  public synchronized StatsSnapshot getSnapshot() {
    return new StatsSnapshot(getRpcLatencyMs(), getMutationRate(), getThrottlingMicros());
  }

  @VisibleForTesting
  void reset() {
    mutationTimer = null;
    mutationMeter = null;
    throttlingTimer = null;
  }

  private Long getRpcLatencyMs() {
    return mutationTimer == null ? null
        : TimeUnit.NANOSECONDS.toMillis((long) mutationTimer.getSnapshot().getMean());
  }

  private Long getMutationRate() {
    return mutationMeter == null ? null : (long) mutationMeter.getMeanRate();
  }

  private Long getThrottlingMicros() {
    return throttlingTimer == null ? null
        : TimeUnit.NANOSECONDS.toMicros((long) throttlingTimer.getSnapshot().getMean());
  }

  /**
   * This method updates rpc time statistics statistics.
   * @param rpcDurationInNanos
   */
  synchronized void markMutationsRpcCompletion(long rpcDurationInNanos) {
    getMutationTimer().update(rpcDurationInNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * This method updates mutations per second statistics.
   * @param mutationCount
   * @param rpcTimeInNanos
   */
  synchronized void markMutationsSuccess(long mutationCount) {
    getMutationMeter().mark(mutationCount);
  }

  /**
   * This method updates throttling statistics.
   * @param throttlingTimeinNanos
   */
  synchronized void markThrottling(long throttlingDurationInNanos) {
    getThrottlingTimer().update(throttlingDurationInNanos, TimeUnit.NANOSECONDS);
  }

  private Timer getMutationTimer() {
    if (mutationTimer == null) {
      mutationTimer = registry.timer("MutationStats.mutation.timer");
    }
    return mutationTimer;
  }

  private Meter getMutationMeter() {
    if (mutationMeter == null) {
      mutationMeter = registry.meter("MutationStats.mutations.meter");
    }
    return mutationMeter;
  }

  private Timer getThrottlingTimer() {
    if (throttlingTimer == null) {
      throttlingTimer = registry.timer("MutationStats.throttle.timer");
    }
    return throttlingTimer;
  }
}
