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
import com.google.rpc.Status;

import io.grpc.Deadline;
import io.grpc.Status.Code;

/**
 * Tests for {@link MutateRowsRequestManager}
 *
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

  private static boolean send(MutateRowsRequestManager underTest, MutateRowsResponse response) {
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
  public void testEmptySuccess() throws Exception {
    MutateRowsRequestManager underTest = createRequestManager(createRequest(0));
    send(underTest, createResponse());
    Assert.assertEquals(createResponse(), underTest.buildResponse());
  }

  @Test
  public void testSingleSuccess() throws Exception {
    MutateRowsRequestManager underTest = createRequestManager(createRequest(1));
    send(underTest, createResponse(OK));
    Assert.assertEquals(createResponse(OK),underTest.buildResponse());
  }

  @Test
  public void testMultiSuccess() throws Exception {
    MutateRowsRequestManager underTest = createRequestManager(createRequest(10));
    send(underTest, createResponse(OK, OK, OK, OK, OK, DEADLINE_EXCEEDED, DEADLINE_EXCEEDED,
      DEADLINE_EXCEEDED, DEADLINE_EXCEEDED));
    send(underTest, createResponse(OK, OK, OK, OK, OK));
    Assert.assertEquals(createResponse(OK, OK, OK, OK, OK, OK, OK, OK, OK, OK),
      underTest.buildResponse());
  }

  @Test
  public void testMultiAttempt() throws Exception {
    MutateRowsRequestManager underTest = createRequestManager(createRequest(10));
    for (int i = 10; i > 1; i--) {
      Status statuses[] = new Status[i];
      statuses[0] = OK;
      for (int j = 1; j < i; j++) {
        statuses[j] = DEADLINE_EXCEEDED;
      }
      Assert.assertFalse(send(underTest, createResponse(statuses)));
      Assert.assertTrue(underTest.isRetryable());
      Assert.assertFalse(underTest.isMessageInvalid());
    }
    Assert.assertEquals(createResponse(OK, OK, OK, OK, OK, OK, OK, OK, OK, DEADLINE_EXCEEDED),
      underTest.buildResponse());
  }

  @Test
  public void testNotRetryable() throws Exception {
    MutateRowsRequestManager underTest = createRequestManager(createRequest(3));
    Assert.assertFalse(send(underTest, createResponse(OK, OK, NOT_FOUND)));
    Assert.assertFalse(underTest.isRetryable());
    Assert.assertFalse(underTest.isMessageInvalid());
    Assert.assertEquals(createResponse(OK, OK, NOT_FOUND), underTest.buildResponse());
  }

  @Test
  public void testInvalid() throws Exception {
    MutateRowsRequestManager underTest = createRequestManager(createRequest(3));
    Assert.assertFalse(send(underTest, createResponse(OK, OK)));
    Assert.assertFalse(underTest.isRetryable());
    Assert.assertTrue(underTest.isMessageInvalid());
  }

  private MutateRowsRequestManager createRequestManager(MutateRowsRequest request) {
    return new MutateRowsRequestManager(retryOptions, request);
  }
}
