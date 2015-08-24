package com.google.cloud.bigtable.config;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import io.grpc.Status;

/**
 * Unit tests for {@link RetryOptions}.
 */
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
        new RetryOptions.Builder().setRetryOnDeadlineExceeded(retryOnDeadlineExceeded).build();
    assertTrue(options.isRetryableRead(Status.INTERNAL.getCode()));
    assertTrue(options.isRetryableRead(Status.UNAVAILABLE.getCode()));
    assertTrue(options.isRetryableRead(Status.ABORTED.getCode()));
    assertFalse(options.isRetryableRead(Status.FAILED_PRECONDITION.getCode()));
    assertEquals(
        retryOnDeadlineExceeded, options.isRetryableRead(Status.DEADLINE_EXCEEDED.getCode()));
  }
}

