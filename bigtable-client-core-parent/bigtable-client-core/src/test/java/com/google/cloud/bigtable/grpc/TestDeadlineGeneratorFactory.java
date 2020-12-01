/*
 * Copyright 2018 Google LLC
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
package com.google.cloud.bigtable.grpc;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.grpc.async.OperationClock;
import com.google.common.base.Optional;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.DeadlineUtil;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for {@link DeadlineGeneratorFactory}. */
@RunWith(JUnit4.class)
public class TestDeadlineGeneratorFactory {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final int LARGE_TIMEOUT = 500_000;

  /**
   * Utility method to return a mock RequestCallOptions that answers all calls to {@link
   * DeadlineGenerator#getRpcAttemptDeadline()} with the given callOptions object.
   */
  public static DeadlineGenerator mockCallOptionsFactory(final CallOptions callOptions) {
    return new DeadlineGenerator() {
      @Override
      public Optional<Long> getOperationTimeoutMs() {
        Deadline deadline = callOptions.getDeadline();
        return deadline != null
            ? Optional.of(deadline.timeRemaining(TimeUnit.MILLISECONDS))
            : Optional.<Long>absent();
      }

      @Override
      public Optional<Deadline> getRpcAttemptDeadline() {
        return Optional.fromNullable(callOptions.getDeadline());
      }
    };
  }

  @Mock ScheduledExecutorService mockExecutor;

  @Test
  public void testDefault() {
    DeadlineGeneratorFactory factory = DeadlineGeneratorFactory.DEFAULT;
    Assert.assertFalse(
        factory.getRequestDeadlineGenerator(null).getRpcAttemptDeadline().isPresent());
  }

  @Test
  public void testDefaultWithContext() {
    final Deadline deadline =
        DeadlineUtil.deadlineWithFixedTime(1, TimeUnit.SECONDS, new OperationClock());
    Context.CancellableContext context = Context.current().withDeadline(deadline, mockExecutor);
    context.run(
        new Runnable() {
          @Override
          public void run() {
            DeadlineGeneratorFactory factory = DeadlineGeneratorFactory.DEFAULT;
            assertEqualsDeadlines(
                deadline.timeRemaining(TimeUnit.MILLISECONDS), getDeadlineMs(factory, null));
          }
        });
  }

  @Test
  public void testConfiguredDefaultConfig() {
    CallOptionsConfig config = CallOptionsConfig.builder().build();
    DeadlineGeneratorFactory factory = new ConfiguredDeadlineGeneratorFactory(config);

    assertThat(
        factory
            .getRequestDeadlineGenerator(null)
            .getRpcAttemptDeadline()
            .get()
            .timeRemaining(TimeUnit.MINUTES),
        both(greaterThanOrEqualTo(5L)).and(lessThanOrEqualTo(7L)));
  }

  @Test
  public void testConfiguredConfigEnabled() {
    CallOptionsConfig config =
        CallOptionsConfig.builder()
            .setUseTimeout(true)
            .setReadRowsRpcTimeoutMs(LARGE_TIMEOUT)
            .build();
    DeadlineGeneratorFactory factory = new ConfiguredDeadlineGeneratorFactory(config);
    assertEqualsDeadlines(
        config.getShortRpcTimeoutMs(),
        getDeadlineMs(factory, SampleRowKeysRequest.getDefaultInstance()));
    assertEqualsDeadlines(
        config.getMutateRpcTimeoutMs(),
        getDeadlineMs(factory, MutateRowsRequest.getDefaultInstance()));
    assertEqualsDeadlines(
        LARGE_TIMEOUT, getDeadlineMs(factory, ReadRowsRequest.getDefaultInstance()));
  }

  @Test
  public void testConfiguredWithContext() {
    Deadline deadline =
        DeadlineUtil.deadlineWithFixedTime(1, TimeUnit.SECONDS, new OperationClock());
    Context.CancellableContext context = Context.current().withDeadline(deadline, mockExecutor);
    context.run(
        new Runnable() {
          @Override
          public void run() {
            CallOptionsConfig config =
                CallOptionsConfig.builder()
                    .setUseTimeout(true)
                    .setShortRpcTimeoutMs((int) TimeUnit.SECONDS.toMillis(100))
                    .setMutateRpcTimeoutMs((int) TimeUnit.SECONDS.toMillis(1000))
                    .setReadRowsRpcTimeoutMs((int) TimeUnit.SECONDS.toMillis(1000))
                    .build();
            DeadlineGeneratorFactory factory = new ConfiguredDeadlineGeneratorFactory(config);
            // The deadline in the context in 1 second, and the deadline in the config is 100+
            // seconds
            assertEqualsDeadlines(
                TimeUnit.SECONDS.toMillis(1),
                getDeadlineMs(factory, MutateRowRequest.getDefaultInstance()));
          }
        });
  }

  @Test
  public void testShortAndLongRpcDeadline() {
    int short_time = (int) TimeUnit.SECONDS.toMillis(100);
    int read_time = (int) TimeUnit.SECONDS.toMillis(1000);
    int mutate_time = (int) TimeUnit.SECONDS.toMillis(1500);
    CallOptionsConfig config =
        CallOptionsConfig.builder()
            .setUseTimeout(true)
            .setShortRpcTimeoutMs(short_time)
            .setMutateRpcTimeoutMs(mutate_time)
            .setReadRowsRpcTimeoutMs(read_time)
            .build();
    DeadlineGeneratorFactory factory = new ConfiguredDeadlineGeneratorFactory(config);
    // The deadline in the context in 1 second, and the deadline in the config is 100+
    // seconds
    assertEqualsDeadlines(
        short_time, getDeadlineMs(factory, SampleRowKeysRequest.getDefaultInstance()));
    assertEqualsDeadlines(
        mutate_time, getDeadlineMs(factory, MutateRowsRequest.getDefaultInstance()));
    assertEqualsDeadlines(read_time, getDeadlineMs(factory, ReadRowsRequest.getDefaultInstance()));
  }

  @Test
  public void testCallOptionsWithMutateRowsRequest() {
    CallOptionsConfig config =
        CallOptionsConfig.builder()
            .setUseTimeout(true)
            .setMutateRpcTimeoutMs(LARGE_TIMEOUT)
            .build();
    DeadlineGeneratorFactory factory = new ConfiguredDeadlineGeneratorFactory(config);
    assertEqualsDeadlines(
        LARGE_TIMEOUT, getDeadlineMs(factory, MutateRowsRequest.getDefaultInstance()));
  }

  @Test
  public void testWhenNoTimeout() {
    CallOptionsConfig config = CallOptionsConfig.builder().setUseTimeout(false).build();
    DeadlineGeneratorFactory factory = new ConfiguredDeadlineGeneratorFactory(config);
    Assert.assertFalse(
        factory
            .getRequestDeadlineGenerator(ReadRowsRequest.getDefaultInstance())
            .getRpcAttemptDeadline()
            .isPresent());
  }

  /**
   * Deadline / Timestamp math could lead to some minor variations from expected values. This method
   * allows for a minor delta.
   *
   * @param expected
   * @param actual
   */
  private static void assertEqualsDeadlines(long expected, long actual) {
    Assert.assertEquals((double) expected, (double) actual, 10);
  }

  private int getDeadlineMs(DeadlineGeneratorFactory factory, Object request) {
    return (int)
        factory
            .getRequestDeadlineGenerator(request)
            .getRpcAttemptDeadline()
            .get()
            .timeRemaining(TimeUnit.MILLISECONDS);
  }
}
