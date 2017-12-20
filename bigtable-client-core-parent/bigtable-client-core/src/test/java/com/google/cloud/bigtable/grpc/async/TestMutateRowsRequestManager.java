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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

import com.google.api.client.util.NanoClock;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsRequest.Entry;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.async.MutateRowsRequestManager.ProcessingStatus;
import com.google.rpc.Status;

import io.grpc.Status.Code;

/**
 * Tests for {@link MutateRowsRequestManager}
 */
@RunWith(JUnit4.class)
public class TestMutateRowsRequestManager {

  private static Status OK = statusOf(io.grpc.Status.Code.OK);
  private static Status DEADLINE_EXCEEDED = statusOf(io.grpc.Status.Code.DEADLINE_EXCEEDED);
  private static Status NOT_FOUND = statusOf(io.grpc.Status.Code.NOT_FOUND);

  private static MutateRowsRequest createRequest(int entryCount) {
    MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
    for (int i = 0; i < entryCount; i++) {
      Mutation mutation = Mutation.newBuilder()
          .setSetCell(SetCell.newBuilder().setFamilyName("Family" + i).build()).build();
      builder.addEntries(Entry.newBuilder().addMutations(mutation));
    }
    return builder.build();
  }

  private static MutateRowsRequest createRequest(MutateRowsRequest original, int ... indices) {
    MutateRowsRequest.Builder builder = MutateRowsRequest.newBuilder();
    for (int i : indices) {
      builder.addEntries(original.getEntries(i));
    }
    return builder.build();
  }

  private static MutateRowsResponse createResponse(Status... statuses) {
    MutateRowsResponse.Builder builder = MutateRowsResponse.newBuilder();
    for (int i = 0; i < statuses.length; i++) {
      builder.addEntries(toEntry(i, statuses[i]));
    }
    return builder.build();
  }

  private static com.google.bigtable.v2.MutateRowsResponse.Entry toEntry(int i, Status status) {
    return MutateRowsResponse.Entry.newBuilder().setIndex(i).setStatus(status).build();
  }

  private static Status statusOf(Code code) {
    return Status.newBuilder().setCode(code.value()).build();
  }

  private static ProcessingStatus send(MutateRowsRequestManager underTest,
      MutateRowsResponse response) {
    underTest.onMessage(response);
    return underTest.onOK();
  }

  private AtomicLong time = new AtomicLong();
  private NanoClock nanoClock = new NanoClock() {
    @Override
    public long nanoTime() {
      return time.get();
    }
  };

  private RetryOptions retryOptions;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);
  }

  @Test
  /**
   * An empty request should return an empty response
   */
  public void testEmptySuccess() {
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, createRequest(0));
    send(underTest, createResponse());
    Assert.assertEquals(createResponse(), underTest.buildResponse());
  }

  @Test
  /**
   * A single successful entry should work.
   */
  public void testSingleSuccess() {
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, createRequest(1));
    send(underTest, createResponse(OK));
    Assert.assertEquals(createResponse(OK), underTest.buildResponse());
  }

  @Test
  /**
   * Two individual calls with one retry should work.
   */
  public void testTwoTrySuccessOneFailure() {
    MutateRowsRequest originalRequest = createRequest(3);
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, originalRequest);
    send(underTest, createResponse(OK, DEADLINE_EXCEEDED, OK));
    Assert.assertEquals(createRequest(originalRequest, 1), underTest.getRetryRequest());
    Assert.assertEquals(createResponse(OK, DEADLINE_EXCEEDED, OK), underTest.buildResponse());
    send(underTest, createResponse(OK));
    Assert.assertEquals(createResponse(OK, OK, OK), underTest.buildResponse());
  }

  @Test
  /**
   * Two individual calls in a more complicated case with one retry should work.
   */
  public void testMultiSuccess() {
    MutateRowsRequest originalRequest = createRequest(10);
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, originalRequest);

    // 5 mutations succeed, 5 mutations are retryable.
    MutateRowsResponse firstResponse = createResponse(OK, DEADLINE_EXCEEDED, OK, DEADLINE_EXCEEDED,
      OK, DEADLINE_EXCEEDED, OK, DEADLINE_EXCEEDED, OK, DEADLINE_EXCEEDED);
    send(underTest, firstResponse);
    Assert.assertEquals(createRequest(originalRequest, 1, 3, 5, 7, 9), underTest.getRetryRequest());
    Assert.assertEquals(firstResponse, underTest.buildResponse());

    // 3 mutations succeed, 2 mutations are retryable.
    send(underTest, createResponse(OK, DEADLINE_EXCEEDED, OK, OK, DEADLINE_EXCEEDED));
    Assert.assertEquals(createRequest(originalRequest, 3, 9), underTest.getRetryRequest());
    MutateRowsResponse secondResponse = createResponse(OK, OK, OK, DEADLINE_EXCEEDED,
      OK, OK, OK, OK, OK, DEADLINE_EXCEEDED);
    Assert.assertEquals(secondResponse, underTest.buildResponse());

    // The final 2 mutations are OK
    send(underTest, createResponse(OK, OK));
    Assert.assertEquals(createResponse(OK, OK, OK, OK, OK, OK, OK, OK, OK, OK),
      underTest.buildResponse());
  }

  @Test
  /**
   * Multiple attempts at retries should work as expected. 10 mutations are added, and 1 gets an OK
   * status for 9 rounds until 1 mutation is left.  Each success shows up in a random location.
   */
  public void testMultiAttempt() {
    MutateRowsRequest originalRequest = createRequest(10);
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, originalRequest);

    // At the beginning, all mutations are outstanding
    List<Integer> remaining = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      remaining.add(i);
    }

    for (int i = 0; i < 9; i++) {
      int remainingMutationCount = remaining.size();
      // Randomly choose a successful mutation
      int successIndex = (int) (Math.random() * remainingMutationCount);

      // Create a Status array filled with DEADLINE_EXCEEDED except for the chose one.
      Status statuses[] = new Status[remainingMutationCount];
      Arrays.fill(statuses, DEADLINE_EXCEEDED);
      statuses[successIndex] = OK;

      // The successful status can now be removed.
      remaining.remove(successIndex);

      // Make sure that the request is retryable, and that the retry request looks reasonable.
      Assert.assertEquals(ProcessingStatus.RETRYABLE, send(underTest, createResponse(statuses)));
      Assert.assertEquals(createRequest(originalRequest, toIntArray(remaining)),
        underTest.getRetryRequest());
    }

    // Only one Mutation should be outstanding at this point. Create a response that has all OKs,
    // with the exception of the remaining statuses.
    Status[] statuses = new Status[10];
    Arrays.fill(statuses, OK);
    statuses[remaining.get(0)] = DEADLINE_EXCEEDED;
    Assert.assertEquals(createResponse(statuses), underTest.buildResponse());
  }

  private final int[] toIntArray(List<Integer> integers) {
    int indicesToRetry[] = new int[integers.size()];
    for (int i = 0; i < integers.size(); i++) {
      indicesToRetry[i] = integers.get(i);
    }
    return indicesToRetry;
  }

  @Test
  public void testNotRetryable() {
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, createRequest(3));
    Assert.assertEquals(ProcessingStatus.NOT_RETRYABLE,
      send(underTest, createResponse(OK, OK, NOT_FOUND)));
    Assert.assertEquals(createResponse(OK, OK, NOT_FOUND), underTest.buildResponse());
  }

  @Test
  public void testInvalid() {
    // Create 3 muntations, but only 2 OKs.  That should be invalid
    MutateRowsRequestManager underTest =
        new MutateRowsRequestManager(retryOptions, createRequest(3));
    Assert.assertEquals(ProcessingStatus.INVALID, send(underTest, createResponse(OK, OK)));
  }
}
