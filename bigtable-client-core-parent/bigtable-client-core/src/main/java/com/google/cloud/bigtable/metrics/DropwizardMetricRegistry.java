package com.google.cloud.bigtable.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/**
 * A {@link MetricRegistry} that wraps a Dropwizard Metrics {@link
 * com.codahale.metrics.MetricRegistry}.
 */
public class DropwizardMetricRegistry implements MetricRegistry {

  private final com.codahale.metrics.MetricRegistry registry =
      new com.codahale.metrics.MetricRegistry();

  /**
   * Creates a {@link DropwizardMetricRegistry} with an {@link Slf4jReporter}.  Only non-zero metrics
   * will be logged to the {@link Slf4jReporter}.
   * 
   * @param registry The registry on which to add the reporter.
   * @param logger The {@link Logger} to report to
   * @param period the amount of time between polls
   * @param unit   the unit for {@code period}
   *
   * @return the {@link DropwizardMetricRegistry}
   */
  public static void createSlf4jReporter(DropwizardMetricRegistry registry, Logger logger,
      long period, TimeUnit unit) {
    MetricFilter nonZeroMatcher =
        new MetricFilter() {
          @Override
          public boolean matches(String name, Metric metric) {
            if (metric instanceof Counting) {
              Counting counter = (Counting) metric;
              return counter.getCount() > 0;
            }
            return true;
          }
        };
    Slf4jReporter.forRegistry(registry.getRegistry())
        .outputTo(logger)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(nonZeroMatcher)
        .build()
        .start(period, unit);
  }

  /**
   * Creates a named {@link Counter} that wraps a Dropwizard Metrics {@link
   * com.codahale.metrics.Counter}.
   *
   * @param name
   * @return a {@link Counter} that wraps a Dropwizard Metrics {@link com.codahale.metrics.Counter}
   */
  @Override
  public Counter counter(String name) {
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
  public Timer timer(String name) {
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
  public Meter meter(String name) {
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
