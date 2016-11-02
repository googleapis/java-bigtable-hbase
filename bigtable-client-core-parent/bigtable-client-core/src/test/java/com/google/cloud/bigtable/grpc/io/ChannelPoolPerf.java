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
package com.google.cloud.bigtable.grpc.io;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.cloud.bigtable.grpc.io.ChannelPool;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

/**
 * Simple microbenchmark for {@link ChannelPool}
 */
public class ChannelPoolPerf {
  private static final int TEST_COUNT = 1_000_000;

  public static void main(String[] args) throws Exception {
    final ManagedChannel channel = new ManagedChannel() {

      @Override
      public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
          MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return null;
      }

      @Override
      public String authority() {
        return null;
      }

      @Override
      public ManagedChannel shutdown() {
        // TODO(sduskis): Auto-generated method stub
        return null;
      }

      @Override
      public boolean isShutdown() {
        // TODO(sduskis): Auto-generated method stub
        return false;
      }

      @Override
      public boolean isTerminated() {
        // TODO(sduskis): Auto-generated method stub
        return false;
      }

      @Override
      public ManagedChannel shutdownNow() {
        // TODO(sduskis): Auto-generated method stub
        return null;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO(sduskis): Auto-generated method stub
        return false;
      }
    };
    int threads = 10;
    int concurrent = 400;
    final ChannelPool.ChannelFactory pool = new ChannelPool.ChannelFactory() {
      @Override
      public ManagedChannel create() throws IOException {
        return channel;
      }
    };
    List<HeaderInterceptor> headers = Collections.<HeaderInterceptor> emptyList();
    final ChannelPool cp = new ChannelPool(headers, pool, 40);
    ExecutorService es = Executors.newFixedThreadPool(threads);
    Callable<Void> runnable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        long start = System.nanoTime();
        for (int i = 0; i < TEST_COUNT; i++) {
          cp.newCall(null, null);
        }
        long diff = System.nanoTime() - start;
        double nanosPerRow = diff / TEST_COUNT;
        System.out.println(String.format("took %d ms.  %.0f nanos/row", diff / 1_000_000, nanosPerRow));
        return null;
      }
    };
    runnable.call();
    for (int j = 0; j < concurrent; j++) {
      es.submit(runnable);
    }
    es.shutdown();
    es.awaitTermination(1000, TimeUnit.SECONDS);
  }
}