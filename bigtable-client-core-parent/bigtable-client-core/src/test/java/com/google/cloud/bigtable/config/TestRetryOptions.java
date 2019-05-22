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
package com.google.cloud.bigtable.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link RetryOptions}. */
@RunWith(JUnit4.class)
public class TestRetryOptions {

  @Test
  public void isRetryableRead_noRetryOnDeadlineExceeded() {
    assertIsRetryableRead(false);
  }

  @Test
  public void isRetryableRead_yesRetryOnDeadlineExceeded() {
    assertIsRetryableRead(true);
  }

  private void assertIsRetryableRead(boolean retryOnDeadlineExceeded) {
    RetryOptions options =
        RetryOptions.builder().setRetryOnDeadlineExceeded(retryOnDeadlineExceeded).build();
    assertTrue(options.isRetryable(Status.UNAVAILABLE.getCode()));
    assertTrue(options.isRetryable(Status.ABORTED.getCode()));
    assertFalse(options.isRetryable(Status.FAILED_PRECONDITION.getCode()));
    assertEquals(retryOnDeadlineExceeded, options.isRetryable(Status.DEADLINE_EXCEEDED.getCode()));
  }

  @Test
  public void testGetRetryableStatusCodesWhenDefaultCodes() {
    RetryOptions options = RetryOptions.getDefaultOptions();
    assertEquals(RetryOptions.DEFAULT_ENABLE_GRPC_RETRIES_SET, options.getRetryableStatusCodes());
  }

  @Test
  public void testGetRetryableStatusCodesWhenDisabledDeadLineExceed() {
    RetryOptions options = RetryOptions.builder().setRetryOnDeadlineExceeded(false).build();
    assertEquals(
        ImmutableSet.of(Status.Code.UNAVAILABLE, Status.Code.ABORTED, Status.Code.UNAUTHENTICATED),
        options.getRetryableStatusCodes());
  }
}
