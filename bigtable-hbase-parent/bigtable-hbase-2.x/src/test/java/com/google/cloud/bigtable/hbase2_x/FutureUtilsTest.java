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

import static org.junit.Assert.*;
import static org.hamcrest.core.StringContains.containsString;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class FutureUtilsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFailedFuture() throws Exception {
    CompletableFuture<Object> asyncFuture =
        FutureUtils.failedFuture(new IllegalStateException("Test failed feature"));

    assertTrue("future should be complete", asyncFuture.isDone());
    assertTrue("Should complete with exception", asyncFuture.isCompletedExceptionally());

    thrown.expect(ExecutionException.class);
    thrown.expectCause(IsInstanceOf.<Throwable>instanceOf(IllegalStateException.class));
    thrown.expectMessage(containsString("Test failed feature"));
    asyncFuture.get();
  }

  @Test
  public void testToCompletableFuture() throws Exception {
    String result = "Result";

    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));

    ListenableFuture<String> listenableFuture = service.submit(new Callable<String>() {
      public String call() throws Exception {
        Thread.sleep(1000);
        return result;
      }
    });

    CompletableFuture<String> completableFuture = FutureUtils.toCompletableFuture(listenableFuture);
    assertFalse("Should be in progress", completableFuture.isDone());

    Throwable t = null;
    try {
      completableFuture.get(500, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      t = e;
    }
    assertNotNull("TimeoutException is expected", t);
    assertEquals(result, completableFuture.get());
  }

  @Test
  public void testToCompletableFuture_exception() throws Exception {
    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));

    ListenableFuture<String> listenableFuture = service.submit(new Callable<String>() {
      public String call() throws Exception {
        throw new IllegalStateException("Test failed feature");
      }
    });

    CompletableFuture<String> completableFuture = FutureUtils.toCompletableFuture(listenableFuture);
    assertTrue("future should be complete", completableFuture.isDone());
    assertTrue("Should complete with exception", completableFuture.isCompletedExceptionally());

    thrown.expect(ExecutionException.class);
    thrown.expectCause(IsInstanceOf.<Throwable>instanceOf(IllegalStateException.class));
    thrown.expectMessage(containsString("Test failed feature"));
    completableFuture.get();
  }
}
