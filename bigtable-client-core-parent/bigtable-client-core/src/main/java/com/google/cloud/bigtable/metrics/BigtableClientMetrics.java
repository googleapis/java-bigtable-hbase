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
package com.google.cloud.bigtable.metrics;

/**
 * Singleton Container for a {@link MetricRegistry}. The default behavior is to return
 * implementations that do nothing. Exporting of metrics can be turned on by either adding TRACE
 * level logging for this class, which will write out all metrics to a log file, or via a call to
 * {@link BigtableClientMetrics#setMetricRegistry(MetricRegistry)}, configuration of reporters as per
 * the instructions on <a href="http://metrics.dropwizard.io/3.1.0/getting-started/">the Dropwizards
 * Metrics Getting Started docs</a>.
 * @author sduskis
 */
public final class BigtableClientMetrics {

  private static MetricRegistry registry = MetricRegistry.NULL_METRICS_REGISTRY;

  public static void setMetricRegistry(MetricRegistry registry) {
    BigtableClientMetrics.registry = registry;
  }
  
  public static MetricRegistry getRegistry() {
    return registry;
  }
  
  // Simplistic initialization via slf4j
  static {
    DropwizardMetricRegistry.initializeForLogging();
  }

  private BigtableClientMetrics(){
  }
}

