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

import java.util.concurrent.TimeUnit;

import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;

public class BulkMutationThrottling {

  private static boolean isThrottling = false;

  public static synchronized void throttle(final ResourceLimiter resourceLimiter,
      final int bulkMutationRpcTargetMs) {
    if (isThrottling) {
      // Throttling was already turned on.  No need to do it again.
      return;
    }
    final long highTargetNanos =
        TimeUnit.MILLISECONDS.toNanos((long) (bulkMutationRpcTargetMs * 1.2));
    final long lowTargetNanos =
        TimeUnit.MILLISECONDS.toNanos((long) (bulkMutationRpcTargetMs * 0.8));
    // Change at 5% increments
    final int throttlingChangeStep = resourceLimiter.getMaxInFlightRpcs() / 20;
    int current = resourceLimiter.getCurrentInFlightRpcs();
    resourceLimiter.setCurrentInFlightRpcs(current / 2);
    Runnable r = new Runnable() {
      @Override
      public void run() {
        BulkMutationsStats stats = BulkMutationsStats.getInstance();
        long meanLatencyNanos = (long) stats.getMutationTimer().getSnapshot().getMean();
        if (meanLatencyNanos >= highTargetNanos) {
          int current = resourceLimiter.getCurrentInFlightRpcs();

          // decrease at 10% rather than 5%
          int newValue = current - (throttlingChangeStep * 2);
          resourceLimiter.setCurrentInFlightRpcs(Math.max(newValue, throttlingChangeStep));
        } else if (meanLatencyNanos <= lowTargetNanos && (stats.getThrottlingTimer().getSnapshot()
            .getMean() > TimeUnit.MILLISECONDS.toNanos(1))) {
          int current = resourceLimiter.getCurrentInFlightRpcs();
          int newValue = current + throttlingChangeStep;
          resourceLimiter
              .setCurrentInFlightRpcs(Math.min(newValue, resourceLimiter.getMaxInFlightRpcs()));
        }
      }
    };
    BigtableSessionSharedThreadPools.getInstance().getRetryExecutor().scheduleAtFixedRate(r, 20, 20,
      TimeUnit.SECONDS);
    isThrottling = true;
  }

}
