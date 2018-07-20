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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.api.core.ApiClock;
import com.google.cloud.bigtable.config.RetryOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ClientCall.Listener;
import io.grpc.Status;

/**
 * Test for {@link RetryingUnaryOperation} and {@link AbstractRetryingOperation}
 * functionality.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestRetryingUnaryOperation {

  private static final RetryOptions RETRY_OPTIONS = new RetryOptions.Builder().build();

  private static final BigtableAsyncRpc.RpcMetrics metrics =
      BigtableAsyncRpc.RpcMetrics.createRpcMetrics(BigtableGrpc.getReadRowsMethod());

  @Mock
  private BigtableAsyncRpc<ReadRowsRequest, ReadRowsResponse> readAsync;

  private OperationClock clock;

  @Mock
  private ScheduledExecutorService executorService;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(readAsync.getRpcMetrics()).thenReturn(metrics);
    when(readAsync.getMethodDescriptor()).thenReturn(BigtableGrpc.getReadRowsMethod());
    when(readAsync.isRetryable(any(ReadRowsRequest.class))).thenReturn(true);

    clock = new OperationClock();
    clock.initializeMockSchedule(executorService, null);
  }

  @Test
  public void testOK() throws Exception {
    final ReadRowsResponse result = ReadRowsResponse.getDefaultInstance();
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
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
    ListenableFuture future = createOperation(CallOptions.DEFAULT).getAsyncResult();
    Assert.assertEquals(result, future.get(1, TimeUnit.SECONDS));
    verify(readAsync, times(1)).start(
        any(ReadRowsRequest.class),
        any(ClientCall.Listener.class),
        any(Metadata.class),
        any(ClientCall.class));
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
    ListenableFuture future = createOperation(CallOptions.DEFAULT).getAsyncResult();

    Assert.assertEquals(result, future.get(1, TimeUnit.SECONDS));
    Assert.assertEquals(5, counter.get());
  }

  @Test
  public void testCompleteFailure() throws Exception {
    final Status errorStatus = Status.UNAVAILABLE;
    Answer<Void> answer = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        invocation.getArgumentAt(1, Listener.class).onClose(errorStatus, null);
        return null;
      }
    };
    doAnswer(answer)
        .when(readAsync)
        .start(
            any(ReadRowsRequest.class),
            any(Listener.class),
            any(Metadata.class),
            any(ClientCall.class));
    try {
      createOperation(CallOptions.DEFAULT).getAsyncResult().get(1, TimeUnit.SECONDS);
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertEquals(BigtableRetriesExhaustedException.class, e.getCause().getClass());
      Assert.assertEquals(errorStatus.getCode(), Status.fromThrowable(e).getCode());
    }

    clock.assertTimeWithinExpectations(
        TimeUnit.MILLISECONDS.toNanos(RETRY_OPTIONS.getMaxElapsedBackoffMillis()));
  }

  private RetryingUnaryOperation createOperation(CallOptions options) {
    return new RetryingUnaryOperation<>(RETRY_OPTIONS, ReadRowsRequest.getDefaultInstance(),
        readAsync, options, executorService, new Metadata(), clock);
  }

}