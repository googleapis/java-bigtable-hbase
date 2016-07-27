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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ClientCall.Listener;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Test for {@link AbstractRetryingRpcListener}
 */
@RunWith(JUnit4.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class RetryingUnaryRpcListenerTest {

  private RetryingUnaryRpcListener underTest;

  @Mock
  private BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> readAsync;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private NanoClock nanoClock;

  private RetryOptions retryOptions;

  AtomicLong totalSleep;

  @Mock
  private ScheduledExecutorService executorService;

  public final static MetricRegistry metrics = new MetricRegistry();
  public final static Timer timer = metrics.timer(RetryingUnaryRpcListenerTest.class.getName());

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);

    when(readAsync.createTimerContext()).thenReturn(timer.time());
    underTest = new RetryingUnaryRpcListener<>(retryOptions, ReadRowsRequest.getDefaultInstance(),
        readAsync, CallOptions.DEFAULT, executorService, new Metadata());

    totalSleep = new AtomicLong();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        long value = invocation.getArgumentAt(1, Long.class);
        TimeUnit timeUnit = invocation.getArgumentAt(2, TimeUnit.class);
        totalSleep.addAndGet(timeUnit.toNanos(value));
        new Thread(invocation.getArgumentAt(0, Runnable.class)).start();
        return null;
      }
    }).when(executorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

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
    when(readAsync.isRetryable(any(ReadRowsRequest.class))).thenReturn(true);
  }


  @Test
  public void testOK() throws Exception {
    long originalCount = timer.getCount();
    final ReadRowsResponse result = ReadRowsResponse.getDefaultInstance();
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Listener listener = invocation.getArgumentAt(1, ClientCall.Listener.class);
        listener.onMessage(result);
        listener.onClose(Status.OK, null);
        return null;
      }
    };
    doAnswer(answer).when(readAsync).call(any(ReadRowsRequest.class),
      any(ClientCall.Listener.class), any(CallOptions.class), any(Metadata.class));
    underTest.start();
    Assert.assertEquals(result, underTest.getCompletionFuture().get(1, TimeUnit.SECONDS));
    verify(nanoClock, times(0)).nanoTime();
    Assert.assertEquals(1, timer.getCount() - originalCount);
  }

  @Test
  public void testRecoveredFailure() throws Exception {
    long originalCount = timer.getCount();
    final ReadRowsResponse result = ReadRowsResponse.getDefaultInstance();
    final Status errorStatus = Status.UNAVAILABLE;
    final AtomicInteger counter = new AtomicInteger(0);
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Listener listener = invocation.getArgumentAt(1, ClientCall.Listener.class);
        if (counter.incrementAndGet() < 5) {
          listener.onClose(errorStatus, null);
        } else {
          listener.onMessage(result);
          listener.onClose(Status.OK, null);
        }
        return null;
      }
    };
    doAnswer(answer).when(readAsync).call(any(ReadRowsRequest.class),
      any(ClientCall.Listener.class), any(CallOptions.class), any(Metadata.class));
    underTest.start();

    Assert.assertEquals(result, underTest.getCompletionFuture().get(1, TimeUnit.HOURS));
    Assert.assertEquals(5, counter.get());
    Assert.assertEquals(1, timer.getCount() - originalCount);
  }

  @Test
  public void testCompleteFailure() throws Exception {
    final Status errorStatus = Status.UNAVAILABLE;
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(1, ClientCall.Listener.class).onClose(errorStatus, null);
        return null;
      }
    };
    doAnswer(answer).when(readAsync).call(any(ReadRowsRequest.class),
      any(ClientCall.Listener.class), any(CallOptions.class), any(Metadata.class));
    try {
      underTest.start();
      underTest.getCompletionFuture().get(1, TimeUnit.MINUTES);
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertEquals(BigtableRetriesExhaustedException.class, e.getCause().getClass());
      BigtableRetriesExhaustedException retriesExhaustedException =
          (BigtableRetriesExhaustedException) e.getCause();
      StatusRuntimeException sre = (StatusRuntimeException) retriesExhaustedException.getCause();
      Assert.assertEquals(errorStatus.getCode(), sre.getStatus().getCode());
      long maxSleep = TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElaspedBackoffMillis());
      Assert.assertTrue(
        String.format("Slept only %d seconds", TimeUnit.NANOSECONDS.toSeconds(totalSleep.get())),
        totalSleep.get() >= maxSleep);
    }
  }
}