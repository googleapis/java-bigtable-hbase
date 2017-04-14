package com.google.cloud.bigtable.util;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around {@link ExponentialBackOff} to allow it to be used w/o a sleeper.
 * Its marriage of a rate limiter and an ExponentialBackOff.
 * This class is not threadsafe.
 */
public class AsyncBackOff {
  private static final Logger LOG = new Logger(AsyncBackOff.class);

  private final Stopwatch stopwatch;
  private final BackOff backOff;
  private long currentWait = 0;

  public AsyncBackOff() {
    this(5, 5000);

  }
  public AsyncBackOff(int initialIntervalMillis, int maxIntervalMillis) {
    stopwatch = Stopwatch.createUnstarted();
    backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(initialIntervalMillis)
        .setMaxIntervalMillis(maxIntervalMillis)
        .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
        .build();
  }

  @VisibleForTesting
  AsyncBackOff(Ticker ticker, BackOff backOff) {
    this.stopwatch = Stopwatch.createUnstarted(ticker);
    this.backOff = backOff;
  }

  /**
   * Try to acquire access. If we are not trying to back off, will return true.
   * If access was acquired, the next call will be subject to a back off timer.
   */
  public boolean tryAcquire() {
    if (stopwatch.isRunning()) {
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) < currentWait) {
        return false;
      }
      stopwatch.reset();
      currentWait = 0;
    }

    try {
      currentWait = backOff.nextBackOffMillis();
    } catch (IOException e) {
      // should never happen
      LOG.error("Failed to get next backoff");
      return false;
    }
    stopwatch.start();
    return true;
  }

  /**
   * Reset the back off.
   */
  public void reset() {
    if (stopwatch.isRunning()) {
      stopwatch.reset();
    }
    currentWait = 0;
    try {
      backOff.reset();
    } catch (IOException e) {
      // should never happen
      LOG.error("Failed to reset the backoff");
    }
  }
}
