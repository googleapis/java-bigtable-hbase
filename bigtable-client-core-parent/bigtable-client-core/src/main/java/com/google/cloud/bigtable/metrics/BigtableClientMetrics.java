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
  private static MetricLevel levelToLog = MetricLevel.Info;

  public enum MetricLevel {
    Info(1), Debug(2), Trace(3);

    private final int level;

    MetricLevel(int level) {
      this.level = level;
    }

    public int getLevel() {
      return level;
    }
  }

  /**
   * Sets a {@link MetricRegistry} to be used in all Bigtable connection created after the call.
   * NOTE: this will not update any existing connections.
   * @param registry
   */
  public static void setMetricRegistry(MetricRegistry registry) {
    BigtableClientMetrics.registry = registry;
  }

  public static MetricRegistry getMetricRegistry(MetricLevel level) {
    if (levelToLog.getLevel() >= level.getLevel()) {
      return registry;
    } else {
      return MetricRegistry.NULL_METRICS_REGISTRY;
    }
  }

  /**
   * Creates a named {@link Counter}. This is a shortcut for
   * {@link BigtableClientMetrics#getMetricRegistry(MetricLevel)}.
   * {@link MetricRegistry#counter(String)}.
   *
   * @return a {@link Counter}
   */
  public static Counter counter(MetricLevel level, String name) {
    return getMetricRegistry(level).counter(name);
  }

  /** Creates a named {@link Timer}. This is a shortcut for
   * {@link BigtableClientMetrics#getMetricRegistry(MetricLevel)}.
   * {@link MetricRegistry#timer(String)}.
   *
   * @return a {@link Timer}
   */
  public static Timer timer(MetricLevel level, String name) {
    return getMetricRegistry(level).timer(name);
  }

  /** Creates a named {@link Meter}.  This is a shortcut for
   * {@link BigtableClientMetrics#getMetricRegistry(MetricLevel)}.
   * {@link MetricRegistry#meter(String)}.
   *
   * @return a {@link Meter}
   */
  public static Meter meter(MetricLevel level, String name) {
    return getMetricRegistry(level).meter(name);
  }

  /**
   * Set a level at which to log.  By default, the value is {@link MetricLevel#Info}.
   *
   * @param levelToLog
   */
  public static void setLevelToLog(MetricLevel levelToLog) {
    BigtableClientMetrics.levelToLog = levelToLog;
  }

  // Simplistic initialization via slf4j
  static {
    Logger logger = LoggerFactory.getLogger(BigtableClientMetrics.class);
    if (logger.isDebugEnabled()) {
      setMetricRegistry(DropwizardMetricRegistry.createSlf4jReporter(logger, 1, TimeUnit.MINUTES));
    }
  }

  private BigtableClientMetrics(){
  }
}
