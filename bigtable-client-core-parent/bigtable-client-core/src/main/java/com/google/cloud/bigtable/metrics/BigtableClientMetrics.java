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

import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Slf4jReporter;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton Container for a {@link MetricRegistry}. The default behavior is to return
 * implementations that do nothing. Exporting of metrics can be turned on by either adding TRACE
 * level logging for this class, which will write out all metrics to a log file, or via a call to
 * {@link BigtableClientMetrics#enable} followed by a programmatic configuration of reporters as per
 * the instructions on <a href="http://metrics.dropwizard.io/3.1.0/getting-started/">the Dropwizards
 * Metrics Getting Started docs</a>.
 * @author sduskis
 */
public final class BigtableClientMetrics {

  private static MetricRegistry CLIENT_STATS;

  // Simplistic initialization via slf4j
  static {
    // This adds a simple mechanism of enabling statistics via slf4j configuration.
    // More complex configuration is available programmatically.
    Logger logger = LoggerFactory.getLogger(BigtableClientMetrics.class);
    if (logger.isTraceEnabled()) {
      enable();
      MetricFilter nonZeroMatcher = new MetricFilter() {
        @Override
        public boolean matches(String name, Metric metric) {
          if (metric instanceof Counting) {
            Counting counter = (Counting) metric;
            return counter.getCount() > 0;
          }
          return true;
        }
      };
      final Slf4jReporter reporter =
          Slf4jReporter.forRegistry(getClientStats())
              .outputTo(logger)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .filter(nonZeroMatcher)
              .build();
      reporter.start(1, TimeUnit.MINUTES);
    }
  }

  // Null implementations of Counter and Timer

  private static final Counter NULL_COUNTER = new Counter() {
    @Override
    public void inc() {
    }
  };

  private static final Timer NULL_TIMER = new Timer() {
    private Context NULL_CONTEXT = new Context() {
      @Override
      public void close() {
      }
    };

    @Override
    public Context time() {
      return NULL_CONTEXT;
    }
  };

  /**
   * Turn on client statistics gathering. This does not set up any {@link Reporter}s. See
   * <a href="http://metrics.dropwizard.io/3.1.0/getting-started/">the Dropwizards Metrics Getting
   * Started docs</a> for reporter options.
   */
  public synchronized static void enable() {
    if (CLIENT_STATS == null) {
      CLIENT_STATS = new MetricRegistry();
    }
  }

  /**
   * Gets the client statistics.  May be null if {@link BigtableClientMetrics#enable()} was not called.
   *
   * @return a MetricRegistry that contains the client statistics
   */
  public static MetricRegistry getClientStats() {
    return CLIENT_STATS;
  }

  /**
   * Creates a named {@link Counter}.
   *
   * @param name
   * @return a Dropwizard Metrics {@link com.codahale.metrics.Counter} or {@link BigtableClientMetrics#NULL_COUNTER}.
   */
  public static Counter createCounter(String name) {
    if (getClientStats() != null) {
      final com.codahale.metrics.Counter counter = getClientStats().counter(name);
      return new Counter() {
        @Override
        public void inc() {
          counter.inc();
        }
      };
    } else {
      return NULL_COUNTER;
    }
  }

  /**
   * Creates a named {Timer Counter}.
   *
   * @param name
   * @return a Dropwizard Metrics {@link com.codahale.metrics.Timer} or {@link BigtableClientMetrics#NULL_Timer}.
   */
  public static Timer createTimer(String name) {
    if (getClientStats() != null) {
      final com.codahale.metrics.Timer timer = getClientStats().timer(name);
      return new Timer() {

        @Override
        public Context time() {
          final com.codahale.metrics.Timer.Context context = timer.time();
          return new Context() {
            @Override
            public void close() {
              context.close();
            }
          };
        }
      };
    } else {
      return NULL_TIMER;
    }
  }

  private BigtableClientMetrics(){
  }
}

