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

import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

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
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.config.RetryOptionsUtil;
import com.google.cloud.bigtable.grpc.scanner.ScanRetriesExhaustedException;

import io.grpc.Status;

/**
 * Test for {@link RetryingRpcFunction}
 */
@RunWith(JUnit4.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class RetryingRpcFunctionTest {

  private RetryingRpcFunction underTest;

  @Mock
  private BigtableAsyncRpc readAsync;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private NanoClock nanoClock;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    RetryOptions retryOptions = RetryOptionsUtil.createTestRetryOptions(nanoClock);
    underTest =
        RetryingRpcFunction.create(retryOptions, ReadRowsRequest.getDefaultInstance(),
          readAsync);
  }

  @Test
  public void testBackoff() throws Exception {
    expectedException.expect(ScanRetriesExhaustedException.class);

    final AtomicLong totalSleep = new AtomicLong();
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
    underTest.sleeper = new Sleeper() {
      @Override
      public void sleep(long ms) throws InterruptedException {
        totalSleep.addAndGet(ms * 1000000);
      }
    };
    // This should throw a ScanRetriesExhaustedException after a short while.  The max of 50
    // is a safe number of attempts before assuming that a ScanRetriesExhaustedException will
    // not be thrown.
    for (int i = 0; i < 50; i++) {
      underTest.apply(Status.INTERNAL.asRuntimeException());
    }
  }
}
