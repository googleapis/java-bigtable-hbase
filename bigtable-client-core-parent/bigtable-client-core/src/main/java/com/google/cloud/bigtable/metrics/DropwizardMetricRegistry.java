package com.google.cloud.bigtable.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricRegistry} that wraps a Dropwizard Metrics {@link
 * com.codahale.metrics.MetricRegistry}.
 */
public class DropwizardMetricRegistry implements MetricRegistry {

  private com.codahale.metrics.MetricRegistry registry;  

  public static void initializeForLogging() {
    // This adds a simple mechanism of enabling statistics via slf4j configuration.
    // More complex configuration is available programmatically.
    Logger logger = LoggerFactory.getLogger(BigtableClientMetrics.class);
    if (logger.isTraceEnabled()) {
      DropwizardMetricRegistry registry = new DropwizardMetricRegistry();
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
          Slf4jReporter.forRegistry(registry.getRegistry())
              .outputTo(logger)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .filter(nonZeroMatcher)
              .build();
      reporter.start(1, TimeUnit.MINUTES);
    }
  }
  
  /**
   * Creates a named {@link Counter} that wraps a Dropwizard Metrics {@link
   * com.codahale.metrics.Counter}.
   *
   * @param name
   * @return a {@link Counter} that wraps a Dropwizard Metrics {@link com.codahale.metrics.Counter}
   */
  @Override
  public Counter createCounter(String name) {
    final com.codahale.metrics.Counter counter = registry.counter(name);
    return new Counter() {
      @Override
      public void inc() {
        counter.inc();
      }

      @Override
      public void dec() {
        counter.dec();
      }
    };
  }

  /**
   * Creates a named {@link Timer} that wraps a Dropwizard Metrics {@link
   * com.codahale.metrics.Timer}.
   *
   * @param name
   * @return a {@link Timer} that wraps a Dropwizard Metrics {@link com.codahale.metrics.Timer}
   */
  @Override
  public Timer createTimer(String name) {
    final com.codahale.metrics.Timer timer = registry.timer(name);
    return new Timer() {

      @Override
      public Timer.Context time() {
        final com.codahale.metrics.Timer.Context timerContext = timer.time();
        return new Context() {
          @Override
          public void close() {
            timerContext.close();
          }
        };
      }
    };
  }

  /**
   * Creates a named {@link Meter} that wraps a Dropwizard Metrics {@link
   * com.codahale.metrics.Meter}.
   *
   * @param name
   * @return a {@link Meter} that wraps a Dropwizard Metrics {@link com.codahale.metrics.Meter}
   */
  @Override
  public Meter createMeter(String name) {
    final com.codahale.metrics.Meter meter = registry.meter(name);
    return new Meter() {
      @Override
      public void mark() {
        meter.mark();
      }

      @Override
      public void mark(long size) {
        meter.mark(size);
      }
    };
  }
  
  /** @return the Dropwizard {@link com.codahale.metrics.MetricRegistry} */
  public com.codahale.metrics.MetricRegistry getRegistry() {
    return registry;
  }
}
