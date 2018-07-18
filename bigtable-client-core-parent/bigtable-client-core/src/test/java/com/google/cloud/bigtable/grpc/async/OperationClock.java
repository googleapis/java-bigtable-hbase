/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;
import com.google.cloud.bigtable.config.RetryOptions;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * An implementation of {@link NanoClock} that is geared towards {@link AbstractRetryingOperation}.
 * It can configure a mock {@link ScheduledExecutorService} to set internal time, and it can
 * validate that the amount of sleep time matches expectations.
 */
public class OperationClock implements NanoClock {

  private long timeNs;
  private long totalSleepTime = 0;

  public OperationClock() {
    this.timeNs = System.nanoTime();
  }

  @Override
  public synchronized long nanoTime() {
    return timeNs + totalSleepTime;
  }

  /**
   * Sets up a mock {@link ScheduledExecutorService} to use update the clock based on the amount
   * of time the scheduler was set to.
   */
  public void initializeMockSchedule(ScheduledExecutorService mockExecutor,
     final ScheduledFuture future) {

    Answer<ScheduledFuture> runAutomatically = new Answer<ScheduledFuture>() {
      @Override public ScheduledFuture answer(InvocationOnMock invocation)  {
        long duration = invocation.getArgumentAt(1, Long.class);
        TimeUnit timeUnit = invocation.getArgumentAt(2, TimeUnit.class);
        synchronized (OperationClock.this) {
          totalSleepTime += timeUnit.toNanos(duration);
        }
        invocation.getArgumentAt(0, Runnable.class).run();
        return future;
      }
    };

    when(mockExecutor.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
        .then(runAutomatically);
  }

  /**
   * Creates a {@link ExponentialBackOff} with this as its {@link NanoClock}.
   */
  public ExponentialBackOff createBackoff(RetryOptions retryOptions) {
    return new ExponentialBackOff.Builder()
        .setNanoClock(this)
        .setInitialIntervalMillis(retryOptions.getInitialBackoffMillis())
        .setMaxElapsedTimeMillis(retryOptions.getMaxElapsedBackoffMillis())
        .setMultiplier(retryOptions.getBackoffMultiplier()).build();
  }

  /**
   * Checks to make sure that the expected sleep time matches the actual time slept.
   */
  public synchronized void assertTimeWithinExpectations(long expectedSleepNs) {
    long sleptSeconds = TimeUnit.NANOSECONDS.toSeconds(totalSleepTime);
    Assert.assertTrue(String.format("Slept only %d seconds", sleptSeconds),
        totalSleepTime >= expectedSleepNs);
    Assert.assertTrue(String.format("Slept more than expected (%d seconds)", sleptSeconds),
        totalSleepTime < expectedSleepNs * 2);
  }
}
