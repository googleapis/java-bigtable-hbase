/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.DeadlineGenerator;
import com.google.cloud.bigtable.metrics.RpcMetrics;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.rpc.Status;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status.Code;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

/** Tests for {@link RetryingMutateRowsOperation}. */
@RunWith(JUnit4.class)
public class TestRetryingMutateRowsOperation {
  // TODO: remove silent and tighten mocks
  @Rule public MockitoRule rule = MockitoJUnit.rule().silent();

  private static final RetryOptions RETRY_OPTIONS = RetryOptions.getDefaultOptions();

  private static Status OK = statusOf(io.grpc.Status.Code.OK);
  private static Status DEADLINE_EXCEEDED = statusOf(io.grpc.Status.Code.DEADLINE_EXCEEDED);
  private static final RpcMetrics metrics =
      BigtableAsyncUtilities.Default.createRpcMetrics(BigtableGrpc.getMutateRowsMethod());

  private static MutateRowsResponse createResponse(Status... statuses) {
    MutateRowsResponse.Builder builder = MutateRowsResponse.newBuilder();
    for (int i = 0; i < statuses.length; i++) {
      builder.addEntries(toEntry(i, statuses[i]));
    }
    return builder.build();
  }

  private static MutateRowsResponse createResponse(MutateRowsResponse.Entry... entries) {
    return MutateRowsResponse.newBuilder().addAllEntries(Arrays.asList(entries)).build();
  }

  private static com.google.bigtable.v2.MutateRowsResponse.Entry toEntry(int i, Status status) {
    return MutateRowsResponse.Entry.newBuilder().setIndex(i).setStatus(status).build();
  }

  private static MutateRowsRequest createRequest(int entryCount) {
    MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
    for (int i = 0; i < entryCount; i++) {
      Mutation mutation =
          Mutation.newBuilder()
              .setSetCell(SetCell.newBuilder().setFamilyName("Family" + i).build())
              .build();
      builder.addEntries(Entry.newBuilder().addMutations(mutation));
    }
    return builder.build();
  }

  private static MutateRowsRequest createRequest(MutateRowsRequest.Entry... entries) {
    return MutateRowsRequest.newBuilder().addAllEntries(Arrays.asList(entries)).build();
  }

  private static Status statusOf(Code code) {
    return Status.newBuilder().setCode(code.value()).build();
  }

  private static void send(RetryingMutateRowsOperation underTest, Status... statuses) {
    send(underTest, createResponse(statuses));
  }

  private static void send(RetryingMutateRowsOperation underTest, MutateRowsResponse sendResponse) {
    underTest.onMessage(sendResponse);
    underTest.onClose(io.grpc.Status.OK, new Metadata());
  }

  private static void checkResponse(ListenableFuture<?> future, MutateRowsResponse response)
      throws Exception {
    Assert.assertEquals(Arrays.asList(response), future.get(3, TimeUnit.MILLISECONDS));
  }

  @Mock private BigtableAsyncRpc<MutateRowsRequest, MutateRowsResponse> mutateRows;

  @Mock private ScheduledExecutorService executorService;

  private OperationClock clock;

  @Before
  public void setup() {
    when(mutateRows.getRpcMetrics()).thenReturn(metrics);
    when(mutateRows.isRetryable(any(MutateRowsRequest.class))).thenReturn(true);
    when(mutateRows.getMethodDescriptor()).thenReturn(BigtableGrpc.getMutateRowsMethod());
    clock = new OperationClock();
    clock.initializeMockSchedule(executorService, null);
  }

  @Test
  public void testSingleSuccess() throws Exception {
    RetryingMutateRowsOperation underTest = createOperation(createRequest(1));
    ListenableFuture<?> future = underTest.getAsyncResult();
    send(underTest, OK);
    checkExecutor(0);
    checkResponse(future, createResponse(OK));
  }

  @Test
  public void testRetry() throws Exception {
    MutateRowsRequest request = createRequest(2);
    RetryingMutateRowsOperation underTest = createOperation(request);
    ListenableFuture<?> future = underTest.getAsyncResult();
    MutateRowsRequest retryRequest = createRequest(request.getEntries(1));

    send(underTest, OK, DEADLINE_EXCEEDED);
    checkExecutor(1);
    Assert.assertEquals(retryRequest, underTest.getRetryRequest());

    for (int i = 1; i < 6; i++) {
      send(underTest, DEADLINE_EXCEEDED);
      checkExecutor(i + 1);
      Assert.assertEquals(retryRequest, underTest.getRetryRequest());
    }

    send(underTest, OK);
    checkResponse(future, createResponse(OK, OK));
  }

  @Test
  public void testRetryExhausted() throws Exception {
    MutateRowsRequest request = createRequest(2);
    RetryingMutateRowsOperation underTest = createOperation(request);
    ListenableFuture<?> future = underTest.getAsyncResult();
    MutateRowsRequest retryRequest = createRequest(request.getEntries(1));

    send(underTest, OK, DEADLINE_EXCEEDED);
    checkExecutor(1);
    Assert.assertEquals(retryRequest, underTest.getRetryRequest());

    // Fast forward until retry limit has been reached
    clock.setTime(
        clock.millisTime() + RETRY_OPTIONS.getMaxElapsedBackoffMillis() + 1, TimeUnit.MILLISECONDS);

    send(underTest, DEADLINE_EXCEEDED);
    checkExecutor(1);
    Assert.assertEquals(retryRequest, underTest.getRetryRequest());

    checkResponse(future, createResponse(OK, DEADLINE_EXCEEDED));
  }

  @Test
  public void testCompleteFailure() throws InterruptedException, TimeoutException {
    MutateRowsRequest request = createRequest(2);
    final RetryingMutateRowsOperation underTest = createOperation(request);

    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) {
                ClientCall.Listener listener = invocation.getArgument(1);
                listener.onClose(io.grpc.Status.DEADLINE_EXCEEDED, new Metadata());
                return null;
              }
            })
        .when(mutateRows)
        .start(
            any(MutateRowsRequest.class),
            (ClientCall.Listener) any(),
            (Metadata) any(),
            (ClientCall) any());

    try {
      underTest.getAsyncResult().get(1, TimeUnit.MINUTES);
      Assert.fail("Expecting a DEADLINE_EXCEEDED exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          io.grpc.Status.DEADLINE_EXCEEDED.getCode(), io.grpc.Status.fromThrowable(e).getCode());
    }

    // Check that the amount of sleep required is correct
    clock.assertTimeWithinExpectations(
        TimeUnit.MILLISECONDS.toNanos(RETRY_OPTIONS.getMaxElapsedBackoffMillis()));
  }

  @Test
  public void testResponseOutOfOrder() throws Exception {
    MutateRowsRequest request = createRequest(2);
    RetryingMutateRowsOperation underTest = createOperation(request);
    ListenableFuture<?> future = underTest.getAsyncResult();
    send(underTest, createResponse(toEntry(1, DEADLINE_EXCEEDED), toEntry(0, OK)));
    checkExecutor(1);
    Assert.assertEquals(createRequest(request.getEntries(1)), underTest.getRetryRequest());
    send(underTest, OK);
    checkResponse(future, createResponse(OK, OK));
  }

  @Test
  public void testPartialResponse() {
    RetryingMutateRowsOperation underTest = createOperation(createRequest(2));
    ListenableFuture<?> future = underTest.getAsyncResult();
    send(underTest, OK);
    try {
      future.get(3, TimeUnit.MILLISECONDS);
      Assert.fail("Expected exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(io.grpc.Status.Code.INTERNAL, io.grpc.Status.fromThrowable(e).getCode());
    } catch (Exception e) {
      Assert.fail("Expected ExecutionException.");
    }
  }

  private RetryingMutateRowsOperation createOperation(MutateRowsRequest request) {
    return new RetryingMutateRowsOperation(
        RETRY_OPTIONS,
        request,
        mutateRows,
        DeadlineGenerator.DEFAULT,
        executorService,
        new Metadata(),
        clock);
  }

  private void checkExecutor(int count) {
    verify(executorService, times(count))
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }
}
