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

import com.codahale.metrics.Reporter;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton Container for a {@link MetricRegistry}. The default behavior is to return
 * implementations that do nothing. Exporting of metrics can be turned on by either adding TRACE
 * level logging for this class, which will write out all metrics to a log file. Alternatively, call
 * {@link BigtableClientMetrics#setMetricRegistry(MetricRegistry)}.
 *
 * <p>We provide a {@link DropwizardMetricRegistry} which can be configured with a variety of {@link
 * Reporter}s as per the instructions on <a
 * href="http://metrics.dropwizard.io/3.1.0/getting-started/">the Dropwizards Metrics Getting
 * Started docs</a>.
 *
 * <p>{@link BigtableClientMetrics#setMetricRegistry(MetricRegistry)} must be called before any
 * Cloud Bigtable connections are created.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public final class BigtableClientMetrics {

  private static MetricRegistry registry = MetricRegistry.NULL_METRICS_REGISTRY;

  /**
   * Sets a {@link MetricRegistry} to be used in all Bigtable connection created after the call.
   * NOTE: this will not update any existing connections.
   * @param registry
   */
  public static void setMetricRegistry(MetricRegistry registry) {
    BigtableClientMetrics.registry = registry;
  }

  public static MetricRegistry getMetricRegistry() {
    return registry;
  }
  
  // Simplistic initialization via slf4j
  static {
    Logger logger = LoggerFactory.getLogger(BigtableClientMetrics.class);
    if (logger.isTraceEnabled()) {
      setMetricRegistry(DropwizardMetricRegistry.createSlf4jReporter(logger, 1, TimeUnit.MINUTES));
    }
  }

  private BigtableClientMetrics(){
  }
}
