/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.common.annotations.VisibleForTesting;

/**
 * This class tracks timing and counts of mutations performed by {@link BulkMutation} and throttling
 * performed by {@link ResourceLimiter}.
 */
public class ResourceLimiterStats {

  private static Map<String, ResourceLimiterStats> stats = new HashMap<>();

  public static synchronized ResourceLimiterStats getInstance(BigtableInstanceName instanceName) {
    String key = instanceName.getInstanceName();
    ResourceLimiterStats instance = stats.get(key);
    if (instance == null) {
      instance = new ResourceLimiterStats();
      stats.put(key, instance);
    }
    return instance;
  }

  @VisibleForTesting
  static void reset(){
    stats.clear();
  }

  private final MetricRegistry registry = new MetricRegistry();

  private final Timer rpcTimer = registry.timer("MutationStats.mutation.timer");
  private final Timer throttlingTimer = registry.timer("MutationStats.throttling.timer");
  private final AtomicLong cumulativeThrottlingTimeNanos = new AtomicLong();

  /**
   * This method updates rpc time statistics statistics.
   * @param rpcDurationInNanos
   */
  void markRpcComplete(long rpcDurationInNanos) {
    rpcTimer.update(rpcDurationInNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * This method updates throttling statistics.
   * @param throttlingDurationInNanos
   */
  void markThrottling(long throttlingDurationInNanos) {
    throttlingTimer.update(throttlingDurationInNanos, TimeUnit.NANOSECONDS);
    cumulativeThrottlingTimeNanos.addAndGet(throttlingDurationInNanos);
  }

  public Timer getMutationTimer() {
    return rpcTimer;
  }

  public Timer getThrottlingTimer() {
    return throttlingTimer;
  }

  public long getCumulativeThrottlingTimeNanos() {
    return cumulativeThrottlingTimeNanos.get();
  }
}
