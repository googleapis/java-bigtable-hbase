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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import com.google.cloud.bigtable.config.Logger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Utility methods for converting guava {@link ListenableFuture} Future to
 * {@link CompletableFuture}. Useful to convert the ListenableFuture types used by
 * bigtable-client-core component to Java 8 CompletableFuture types used in Hbase 2
 * 
 * @author spollapally
 */
public class FutureUtils {

  private static final ExecutorService DIRECT_EXECUTOR = MoreExecutors.newDirectExecutorService();
  static Logger logger = new Logger(FutureUtils.class);

  public static <T> CompletableFuture<T>
      toCompletableFuture(final ListenableFuture<T> listenableFuture) {
    return toCompletableFuture(listenableFuture, DIRECT_EXECUTOR);
  }

  public static <T> CompletableFuture<T> toCompletableFuture(
      final ListenableFuture<T> listenableFuture, ExecutorService es) {
    return toCompletableFuture(listenableFuture, (r -> r), es);
  }

  public static <BTType, HBType> CompletableFuture<HBType> toCompletableFuture(
      final ListenableFuture<BTType> listenableFuture, final Function<BTType, HBType> converter,
      ExecutorService es) {
    CompletableFuture<HBType> completableFuture = new CompletableFuture<HBType>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        boolean result = listenableFuture.cancel(mayInterruptIfRunning);
        super.cancel(mayInterruptIfRunning);
        return result;
      }
    };

    FutureCallback<BTType> callback = new FutureCallback<BTType>() {
      public void onFailure(Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }

      public void onSuccess(BTType s) {
        try {
          completableFuture.complete(converter.apply(s));
        } catch (RuntimeException e) {
          onFailure(e);
        }
      }
    };
    Futures.addCallback(listenableFuture, callback, es);

    return completableFuture;
  }

  public static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

}
