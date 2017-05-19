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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * This class tracks timing and counts of mutations performed by {@link BulkMutation} and throttling
 * performed by {@link ResourceLimiter}.
 */
public class ResourceLimiterStats {

  private final MetricRegistry registry = new MetricRegistry();

  private Timer mutationTimer;
  private Meter mutationMeter;
  private Timer throttlingTimer;

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
