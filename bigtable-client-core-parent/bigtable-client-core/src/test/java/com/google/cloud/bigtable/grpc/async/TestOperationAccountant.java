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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.api.client.util.NanoClock;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Tests for {@link OperationAccountant}
 *
 */
@RunWith(JUnit4.class)
public class TestOperationAccountant {

  @Mock
  ListenableFuture<?> future;

  @Mock
  NanoClock clock;

  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testOnOperationCompletion() throws InterruptedException {
    OperationAccountant underTest = new OperationAccountant();
    SettableFuture<String> future = SettableFuture.create();
    underTest.registerOperation(future);
    assertTrue(underTest.hasInflightOperations());
    future.set("");
    assertFalse(underTest.hasInflightOperations());
  }

  @Test
  public void testFlush() throws Exception {
    final int registerCount = 1000;
    ExecutorService pool = Executors.newCachedThreadPool();
    try {
      final OperationAccountant underTest = new OperationAccountant();
      final LinkedBlockingQueue<SettableFuture<String>> registeredEvents = new LinkedBlockingQueue<>();
      Future<Boolean> writeFuture = pool.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws InterruptedException {
          for (long i = 0; i < registerCount; i++) {
            SettableFuture<String> completionFuture = SettableFuture.create();
            underTest.registerOperation(completionFuture);
            registeredEvents.offer(completionFuture);
          }
          underTest.awaitCompletion();
          return true;
        }
      });
      Future<?> readFuture = pool.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception{
          for (int i = 0; i < registerCount; i++) {
            SettableFuture<String> future = registeredEvents.poll(1, TimeUnit.SECONDS);
            if (future != null) {
              future.set("");
            }
            if (i % 10 == 0) {
              // Exercise the .offer and the awaitCompletion() in the writeFuture.
              Thread.sleep(4);
            }
          }
          return null;
        }
      });

      readFuture.get(3, TimeUnit.SECONDS);
      assertTrue(writeFuture.get(3, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testNoSuccessWarning() throws Exception {

    final long timeBetweenOperations = TimeUnit.NANOSECONDS.convert(5, TimeUnit.MINUTES);
    when(clock.nanoTime()).thenAnswer(new Answer<Long>() {
      private int count = 0;

      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return count++ * timeBetweenOperations;
      }
    });

    long finishWaitTime = 100;
    final OperationAccountant underTest = new OperationAccountant(clock, finishWaitTime);

    SettableFuture<String> operation = SettableFuture.create();
    underTest.registerOperation(operation);
    final int iterations = 4;

    ExecutorService pool = Executors.newCachedThreadPool();
    try {
      pool.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          underTest.awaitCompletion();
          return null;
        }
      });
      // Sleep a multiple of the finish wait time to force a few iterations
      Thread.sleep(finishWaitTime * (iterations + 1));
      // Trigger completion
      operation.set("");
    } finally {
      pool.shutdown();
      pool.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    // The test is non-deterministic due to actual waiting. Allow a margin of error.
    // TODO Refactor to make it deterministic.
    assertTrue(underTest.getNoSuccessWarningCount() >= iterations);
  }
}