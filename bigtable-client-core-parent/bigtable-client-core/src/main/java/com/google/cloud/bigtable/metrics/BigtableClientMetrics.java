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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton Container for a MetricRegistry.
 *
 * @author sduskis
 *
 */
public final class BigtableClientMetrics {

  private static MetricRegistry CLIENT_STATS;

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

  static {
    Logger logger = LoggerFactory.getLogger(BigtableClientMetrics.class);
    if (logger.isTraceEnabled()) {
      CLIENT_STATS = new MetricRegistry();
      final Slf4jReporter reporter =
          Slf4jReporter.forRegistry(CLIENT_STATS)
              .outputTo(logger)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
      reporter.start(1, TimeUnit.MINUTES);
    }
  }

  public static void enable() {
    if (CLIENT_STATS == null) {
      CLIENT_STATS = new MetricRegistry();
    }
  }

  public static Counter createCounter(String name) {
    if (CLIENT_STATS != null) {
      final com.codahale.metrics.Counter counter = CLIENT_STATS.counter(name);
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

  public static Timer createTimer(String name) {
    if (CLIENT_STATS != null) {
      final com.codahale.metrics.Timer timer = CLIENT_STATS.timer(name);
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

