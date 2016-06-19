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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Simple microbenchmark for {@link ResourceLimiter}
 */
public class ResourceLimiterPerf {
  final static long SIZE = 10_000L;
  final static int REGISTER_COUNT = ((int) SIZE) * 100;

  public static void main(String[] args) throws Exception {
    ListeningExecutorService pool =
        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    try {
      for (int i = 0; i < 10; i++) {
        System.out.println("=======");
        test(pool);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * @param pool
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  private static void test(ListeningExecutorService pool)
      throws InterruptedException, ExecutionException, TimeoutException {
    final ResourceLimiter underTest = new ResourceLimiter(SIZE, (int) SIZE);
    final LinkedBlockingQueue<Long> registeredEvents = new LinkedBlockingQueue<>();

    final int readerCount = 20;
    final int writerCount = 1;
    Runnable writePerfRunnable =
        new Runnable() {
          @Override
          public void run() {
            long startReg = System.nanoTime();
            int offerCount = REGISTER_COUNT / writerCount;
            try {
              for (int i = 0; i < offerCount; i++) {
                registeredEvents.offer(underTest.registerOperationWithHeapSize(1));
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            } finally {
              long totalTime = System.nanoTime() - startReg;
              System.out.println(
                  String.format(
                      "Registered %d in %d ms.  %d nanos/reg.  %f offer/sec",
                      offerCount,
                      totalTime / 1000000,
                      totalTime / offerCount,
                      offerCount * 1000000000.0 / totalTime));
            }
          }
        };
    Runnable readPerfRunnable =
        new Runnable() {
          @Override
          public void run() {
            long startComplete = System.nanoTime();
            int regCount = REGISTER_COUNT / readerCount;
            try {
              for (int i = 0; i < regCount; i++) {
                Long registeredId = registeredEvents.poll(1, TimeUnit.SECONDS);
                if (registeredId == null) {
                  i--;
                } else {
                  underTest.markCanBeCompleted(registeredId);
                }
              }
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } finally {
              long totalTime = System.nanoTime() - startComplete;
              System.out.println(
                  String.format(
                      "markCanBeCompleted %d in %d.  %d nanos/complete.  %f unreg/sec",
                      regCount,
                      totalTime / 1000000,
                      totalTime / regCount,
                      regCount * 1000000000.0 / totalTime));
            }
          }
        };

    List<ListenableFuture<?>> writerFutures = new ArrayList<>();
    List<ListenableFuture<?>> readerFutures = new ArrayList<>();

    for (int i = 0; i < writerCount; i++) {
      writerFutures.add(pool.submit(writePerfRunnable));
    }
    Thread.sleep(10);
    for (int i = 0; i < readerCount; i++) {
      readerFutures.add(pool.submit(readPerfRunnable));
    }
    Futures.allAsList(writerFutures).get(300, TimeUnit.MINUTES);
    Futures.allAsList(readerFutures).get(300, TimeUnit.MINUTES);
  }
}
