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
import static org.mockito.Matchers.anyInt;
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

import com.google.api.client.util.ExponentialBackOff;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ClientCall.Listener;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Test for {@link RetryingUnaryOperation} and {@link AbstractRetryingOperation}
 * functionality.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestRetryingUnaryOperation {

  private static final BigtableAsyncRpc.RpcMetrics metrics =
      BigtableAsyncRpc.RpcMetrics.createRpcMetrics(BigtableGrpc.getReadRowsMethod());

  @Mock
  private BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> readAsync;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private NanoClock nanoClock;

  @Mock
  private RetryOptions mockRetryOptions;

  private RetryOptions retryOptions;

  AtomicLong totalSleep;

  @Mock
  private ScheduledExecutorService executorService;


  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);

    when(readAsync.getRpcMetrics()).thenReturn(metrics);
    when(readAsync.getMethodDescriptor()).thenReturn(BigtableGrpc.getReadRowsMethod());

    totalSleep = new AtomicLong();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        long value = invocation.getArgumentAt(1, Long.class);
        TimeUnit timeUnit = invocation.getArgumentAt(2, TimeUnit.class);
        totalSleep.addAndGet(timeUnit.toNanos(value));
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(executorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

    final long startNewCall = System.nanoTime();

    // We want the nanoClock to mimic the behavior of sleeping, but without the time penalty.
    // This will allow the RetryingRpcFutureFallback's ExponentialBackOff to work properly.
    // The ExponentialBackOff sends a BackOff.STOP only when the clock time reaches
    // startNewCall + maxElapsedTimeMillis.  Since we don't want to wait maxElapsedTimeMillis (60 seconds)
    // for the test to complete, we mock the clock.
    when(nanoClock.nanoTime()).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return startNewCall + totalSleep.get();
      }
    });
    when(readAsync.isRetryable(any(ReadRowsRequest.class))).thenReturn(true);
  }

  @Test
  public void testOK() throws Exception {
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
    doAnswer(answer)
        .when(readAsync)
        .start(
            any(ReadRowsRequest.class),
            any(ClientCall.Listener.class),
            any(Metadata.class),
            any(ClientCall.class));
    AbstractRetryingOperation underTest = createOperation(CallOptions.DEFAULT);
    ListenableFuture future = underTest.getAsyncResult();
    Assert.assertEquals(result, future.get(1, TimeUnit.SECONDS));
    verify(nanoClock, times(0)).nanoTime();
  }

  private RetryingUnaryOperation createOperation(CallOptions options) {
    return new RetryingUnaryOperation<>(retryOptions, ReadRowsRequest.getDefaultInstance(),
            readAsync, options, executorService, new Metadata());
  }

  @Test
  public void testRecoveredFailure() throws Exception {
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
    doAnswer(answer).when(readAsync).start(any(ReadRowsRequest.class),
      any(ClientCall.Listener.class), any(Metadata.class), any(ClientCall.class));
    AbstractRetryingOperation underTest = createOperation(CallOptions.DEFAULT);
    ListenableFuture future = underTest.getAsyncResult();

    Assert.assertEquals(result, future.get(1, TimeUnit.SECONDS));
    Assert.assertEquals(5, counter.get());
  }

  @Test
  public void testCompleteFailure_withoutDeadline() throws Exception {
    final Status errorStatus = Status.UNAVAILABLE;
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(1, ClientCall.Listener.class).onClose(errorStatus, null);
        return null;
      }
    };
    doAnswer(answer).when(readAsync).start(any(ReadRowsRequest.class),
            any(ClientCall.Listener.class), any(Metadata.class), any(ClientCall.class));

    try {
      AbstractRetryingOperation underTest = createOperation(CallOptions.DEFAULT.withDeadlineAfter(2,
              TimeUnit.SECONDS));
      underTest.getAsyncResult().get(1, TimeUnit.SECONDS);
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertEquals(BigtableRetriesExhaustedException.class, e.getCause().getClass());
      BigtableRetriesExhaustedException retriesExhaustedException =
              (BigtableRetriesExhaustedException) e.getCause();
      StatusRuntimeException sre = (StatusRuntimeException) retriesExhaustedException.getCause();
      Assert.assertEquals(errorStatus.getCode(), sre.getStatus().getCode());
      long maxSleep = TimeUnit.MILLISECONDS.toNanos(retryOptions.getMaxElapsedBackoffMillis());
      Assert.assertTrue(
              String.format("Slept only %d seconds", TimeUnit.NANOSECONDS.toSeconds(totalSleep.get())),
              totalSleep.get() >= maxSleep);
    }
  }

  @Test
  public void testCompleteFailure_withDeadline() throws Exception {
    final Status errorStatus = Status.UNAVAILABLE;
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(1, ClientCall.Listener.class).onClose(errorStatus, null);
        return null;
      }
    };
    doAnswer(answer).when(readAsync).start(any(ReadRowsRequest.class),
      any(ClientCall.Listener.class), any(Metadata.class), any(ClientCall.class));

    try {
      AbstractRetryingOperation underTest = createOperation(CallOptions.DEFAULT.withDeadlineAfter(2,
              TimeUnit.SECONDS));
      underTest.getAsyncResult().get(1, TimeUnit.SECONDS);
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertEquals(BigtableRetriesExhaustedException.class, e.getCause().getClass());
      BigtableRetriesExhaustedException retriesExhaustedException =
          (BigtableRetriesExhaustedException) e.getCause();
      StatusRuntimeException sre = (StatusRuntimeException) retriesExhaustedException.getCause();
      Assert.assertEquals(errorStatus.getCode(), sre.getStatus().getCode());
      long maxSleep = TimeUnit.SECONDS.toNanos(2);
      Assert.assertTrue(
        String.format("Slept only %d seconds", TimeUnit.NANOSECONDS.toSeconds(totalSleep.get())),
        totalSleep.get() >= maxSleep);
    }
  }

  @Test
  public void testDefaultBackoff() {
    when(mockRetryOptions.createBackoff()).thenReturn(new ExponentialBackOff.Builder().build());

    RetryingUnaryOperation underTest = new RetryingUnaryOperation<>(mockRetryOptions, ReadRowsRequest.getDefaultInstance(),
            readAsync, CallOptions.DEFAULT, executorService, new Metadata());

    underTest.getNextBackoff();
    verify(mockRetryOptions, times(1)).createBackoff();
  }

  @Test
  public void testDeadlineBackoff() {
    when(mockRetryOptions.createBackoff(anyInt())).thenReturn(new ExponentialBackOff.Builder().build());
    CallOptions callOptions = CallOptions.DEFAULT.withDeadlineAfter(1000, TimeUnit.MILLISECONDS);
    RetryingUnaryOperation underTest = new RetryingUnaryOperation<>(mockRetryOptions, ReadRowsRequest.getDefaultInstance(),
            readAsync, callOptions, executorService, new Metadata());

    underTest.getNextBackoff();

    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
    verify(mockRetryOptions, times(1)).createBackoff(captor.capture());

    // The deadline remaining and the should be within 100 ms.
    Assert.assertEquals((double) callOptions.getDeadline().timeRemaining(TimeUnit.MILLISECONDS),
            (double) captor.getValue(), 100.0);
  }
}