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
package com.google.cloud.bigtable.grpc;

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.grpc.async.OperationClock;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.DeadlineUtil;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link CallOptionsFactory}. */
@RunWith(JUnit4.class)
public class TestCallOptionsFactory {

  private static final int LARGE_TIMEOUT = 500_000;

  @Mock ScheduledExecutorService mockExecutor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testDefault() {
    CallOptionsFactory factory = new CallOptionsFactory.Default();
    Assert.assertSame(CallOptions.DEFAULT, factory.create(null, null));
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
            CallOptionsFactory factory = new CallOptionsFactory.Default();
            assertEqualsDeadlines(
                deadline.timeRemaining(TimeUnit.MILLISECONDS), getDeadlineMs(factory, null));
          }
        });
  }

  @Test
  public void testConfiguredDefaultConfig() {
    CallOptionsConfig config = CallOptionsConfig.builder().build();
    CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
    Assert.assertSame(CallOptions.DEFAULT, factory.create(null, null));
  }

  @Test
  public void testConfiguredConfigEnabled() {
    CallOptionsConfig config =
        CallOptionsConfig.builder()
            .setUseTimeout(true)
            .setReadRowsRpcTimeoutMs(LARGE_TIMEOUT)
            .build();
    CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
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
            CallOptionsFactory factory =
                new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
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
    CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
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
    CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
    assertEqualsDeadlines(
        LARGE_TIMEOUT, getDeadlineMs(factory, MutateRowsRequest.getDefaultInstance()));
  }

  @Test
  public void testWhenNoTimeout() {
    CallOptionsConfig config = CallOptionsConfig.builder().setUseTimeout(false).build();
    CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
    Assert.assertEquals(
        CallOptions.DEFAULT, factory.create(null, ReadRowsRequest.getDefaultInstance()));
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

  private int getDeadlineMs(CallOptionsFactory factory, Object request) {
    return (int) factory.create(null, request).getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
  }
}
