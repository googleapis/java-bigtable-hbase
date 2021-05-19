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
package com.google.cloud.bigtable.hbase2_x;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link ApiFutureUtils}
 *
 * @author spollapally
 */
public class ApiFutureUtilsTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFailedFuture() throws Exception {
    CompletableFuture<Object> asyncFuture =
        ApiFutureUtils.failedFuture(new IllegalStateException("Test failed feature"));

    assertTrue("future should be complete", asyncFuture.isDone());
    assertTrue("Should complete with exception", asyncFuture.isCompletedExceptionally());

    thrown.expect(ExecutionException.class);
    thrown.expectCause(IsInstanceOf.<Throwable>instanceOf(IllegalStateException.class));
    thrown.expectMessage(containsString("Test failed feature"));
    asyncFuture.get();
  }
}
