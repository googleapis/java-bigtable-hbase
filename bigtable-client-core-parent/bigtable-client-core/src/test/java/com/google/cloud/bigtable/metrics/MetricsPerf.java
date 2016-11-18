/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Performance test for our metrics implementations.
 *
 */
public class MetricsPerf {

  private static final int TEST_COUNT = 10_000_000;
  private static MetricRegistry registry;

  public static void main(String[] args) {
    registry = MetricRegistry.NULL_METRICS_REGISTRY;
    System.out.println("=======> Testing NULL registry");
    timeMetrics();
    timeMetrics();

    registry = new DropwizardMetricRegistry();
    System.out.println("=======> Testing DropWizard registry");
    timeMetrics();
    timeMetrics();
  }

  private static void timeMetrics() {
    timeCounters();
    timeMeters();
    timeTimers();
  }

  private static void timeCounters() {
    final Counter counter = registry.counter("testCounter");
    long start = System.nanoTime();
    for (int i = 0; i < TEST_COUNT; i++) {
      counter.inc();
      counter.dec();
    }
    print("Counters", start);
  }

  private static void timeMeters() {
    final Meter meter = registry.meter("testMeter");
    long start = System.nanoTime();
    for (int i = 0; i < TEST_COUNT; i++) {
      meter.mark();
    }
    print("Meters", start);
  }


  private static void timeTimers() {
    final Timer timer = registry.timer("testTimer");
    long start = System.nanoTime();
    for (int i = 0; i < TEST_COUNT; i++) {
      timer.time().close();
    }
    print("Timers", start);
  }

  private static void print(String type, long start) {
    long diffNanos = System.nanoTime() - start;
    long nanosPer = diffNanos / TEST_COUNT;
    System.out.println("=== " + TEST_COUNT + " " + type + " in "
        + TimeUnit.NANOSECONDS.toMillis(diffNanos) + " ms.  That's " + nanosPer + " nano/metric");
  }

}
