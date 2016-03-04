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
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Test for {@link RetryingRpcFunction}
 */
@RunWith(JUnit4.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class RetryingRpcFunctionTest {

  private RetryingRpcFunction underTest;

  @Mock
  private BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> readAsync;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private NanoClock nanoClock;
  @Mock
  private ListenableFuture mockFuture;

  private RetryOptions retryOptions;

  AtomicLong totalSleep;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);

    underTest = new RetryingRpcFunction<>(retryOptions, ReadRowsRequest.getDefaultInstance(),
        readAsync, MoreExecutors.newDirectExecutorService());

    totalSleep = new AtomicLong();

    underTest.sleeper = new Sleeper() {
      @Override
      public void sleep(long ms) throws InterruptedException {
        totalSleep.addAndGet(ms * 1000000);
      }
    };

    final long start = System.nanoTime();

    // We want the nanoClock to mimic the behavior of sleeping, but without the time penalty.
    // This will allow the RetryingRpcFutureFallback's ExponentialBackOff to work properly.
    // The ExponentialBackOff sends a BackOff.STOP only when the clock time reaches
    // start + maxElapsedTimeMillis.  Since we don't want to wait maxElapsedTimeMillis (60 seconds)
    // for the test to complete, we mock the clock.
    when(nanoClock.nanoTime()).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return start + totalSleep.get();
      }
    });

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
    when(readAsync.call(any(ReadRowsRequest.class))).thenReturn(mockFuture);
  }

  @Test
  public void testOK() throws Exception {
    final ReadRowsResponse result = ReadRowsResponse.getDefaultInstance();
    when(mockFuture.get()).thenReturn(result);
    Assert.assertEquals(result, underTest.callRpcWithRetry().get(1, TimeUnit.SECONDS));
    verify(nanoClock, times(0)).nanoTime();
  }

  @Test
  public void testRecoveredFailure() throws Exception {
    final ReadRowsResponse result = ReadRowsResponse.getDefaultInstance();
    final Status errorStatus = Status.UNAVAILABLE;
    final AtomicInteger counter = new AtomicInteger(0);
    when(mockFuture.get()).thenAnswer(new Answer<ReadRowsResponse>() {
      @Override
      public ReadRowsResponse answer(InvocationOnMock invocation) throws Throwable {
        if (counter.incrementAndGet() < 5){
          throw errorStatus.asRuntimeException();
        }
        return ReadRowsResponse.getDefaultInstance();
      }
    });
    Assert.assertEquals(result, underTest.callRpcWithRetry().get(1, TimeUnit.SECONDS));
    Assert.assertEquals(5, counter.get());
  }

  @Test
  public void testCompleteFailure() throws Exception {
    Status expectedStatus = Status.UNAVAILABLE;
    when(mockFuture.get()).thenThrow(expectedStatus.asRuntimeException());
    try {
      underTest.callRpcWithRetry().get(1, TimeUnit.SECONDS);
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertEquals(BigtableRetriesExhaustedException.class, e.getCause().getClass());
      BigtableRetriesExhaustedException retriesExhaustedException =
          (BigtableRetriesExhaustedException) e.getCause();
      StatusRuntimeException sre = (StatusRuntimeException) retriesExhaustedException.getCause();
      Assert.assertEquals(expectedStatus.getCode(), sre.getStatus().getCode());
      long maxSleep = TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElaspedBackoffMillis());
      Assert.assertTrue(
        String.format("Slept only %d seconds", TimeUnit.NANOSECONDS.toSeconds(totalSleep.get())),
        totalSleep.get() >= maxSleep);
    }
  }
}
